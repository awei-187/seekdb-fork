/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "file_scan_utils.h"
#include "parquet_file_reader.h"
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cmath>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <system_error>
#include <string_view>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace oceanbase::sql::file_sql
{
namespace
{
namespace fs = std::filesystem;
fs::path utf8_path(std::string_view text)
{
  return fs::path(std::u8string_view(reinterpret_cast<const char8_t *>(text.data()), text.size()));
}
Status error(Error code, const std::string &message) { return {code, message}; }
std::string path_text(const fs::path &path)
{
  const auto utf8 = path.generic_u8string();
  return std::string(reinterpret_cast<const char *>(utf8.data()), utf8.size());
}
bool within(const fs::path &path, const fs::path &root)
{
  auto p = path.begin();
  for (auto r = root.begin(); r != root.end(); ++r, ++p) {
    if (p == path.end()) { return false; }
#ifdef _WIN32
    if (_wcsicmp(p->c_str(), r->c_str()) != 0) { return false; }
#else
    if (*p != *r) { return false; }
#endif
  }
  return true;
}
Status identity(FILE *file, Identity &id)
{
#ifdef _WIN32
  BY_HANDLE_FILE_INFORMATION info{};
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(_fileno(file)));
  if (!GetFileInformationByHandle(handle, &info) || (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
    return error(Error::IO, "cannot identify regular input file");
  }
  id.size = (uint64_t(info.nFileSizeHigh) << 32) | info.nFileSizeLow;
  id.modified = (int64_t(info.ftLastWriteTime.dwHighDateTime) << 32) | info.ftLastWriteTime.dwLowDateTime;
  id.inode = (uint64_t(info.nFileIndexHigh) << 32) | info.nFileIndexLow;
  id.device = info.dwVolumeSerialNumber;
#else
  struct stat info{};
  if (fstat(fileno(file), &info) != 0 || !S_ISREG(info.st_mode)) {
    return error(Error::IO, "cannot identify regular input file");
  }
  id.size = info.st_size;
#ifdef __APPLE__
  id.modified = info.st_mtimespec.tv_sec * int64_t(1000000000) + info.st_mtimespec.tv_nsec;
#else
  id.modified = info.st_mtim.tv_sec * int64_t(1000000000) + info.st_mtim.tv_nsec;
#endif
  id.inode = info.st_ino;
  id.device = info.st_dev;
#endif
  return {};
}
Status open_input(const std::string &path, const std::string &root, FILE *&file)
{
  file = nullptr;
#ifdef _WIN32
  HANDLE handle = CreateFileW(utf8_path(path).c_str(), GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) { return error(Error::IO, "cannot open input file"); }
  wchar_t resolved[32768];
  DWORD count = GetFinalPathNameByHandleW(handle, resolved, 32768, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
  std::wstring name = count > 0 && count < 32768 ? std::wstring(resolved, count) : L"";
  if (name.starts_with(L"\\\\?\\")) { name.erase(0, 4); }
  std::error_code ec;
  fs::path allowed = fs::canonical(utf8_path(root), ec);
  if (ec || name.empty() || !within(fs::path(name), allowed)) {
    CloseHandle(handle);
    return error(Error::DENIED, "opened file is outside secure_file_priv");
  }
  int fd = _open_osfhandle(reinterpret_cast<intptr_t>(handle), _O_RDONLY | _O_BINARY);
  if (fd < 0) { CloseHandle(handle); return error(Error::IO, "cannot open file descriptor"); }
  file = _fdopen(fd, "rb");
  if (nullptr == file) { _close(fd); }
#else
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd < 0) { return error(Error::IO, "cannot open input file"); }
  std::error_code ec;
  const fs::path actual = fs::canonical(utf8_path(path), ec);
  const fs::path allowed = fs::canonical(utf8_path(root), ec);
  struct stat a{}, b{};
  if (ec || !within(actual, allowed) || fstat(fd, &a) != 0 || stat(actual.c_str(), &b) != 0
      || !S_ISREG(a.st_mode) || a.st_dev != b.st_dev || a.st_ino != b.st_ino) {
    ::close(fd);
    return error(Error::DENIED, "opened file is outside secure_file_priv or changed during open");
  }
  file = fdopen(fd, "rb");
  if (nullptr == file) { ::close(fd); }
#endif
  return nullptr == file ? error(Error::IO, "cannot open input stream") : Status{};
}
bool digits(const std::string &s, size_t start, size_t count, int &value)
{
  value = 0;
  if (start + count > s.size()) { return false; }
  for (size_t i = start; i < start + count; ++i) {
    if (s[i] < '0' || s[i] > '9') { return false; }
    value = value * 10 + s[i] - '0';
  }
  return true;
}
bool date(const std::string &s)
{
  int y, m, d;
  if (s.size() < 10 || s[4] != '-' || s[7] != '-' || !digits(s, 0, 4, y)
      || !digits(s, 5, 2, m) || !digits(s, 8, 2, d) || y == 0 || m < 1 || m > 12) { return false; }
  const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int limit = days[m - 1] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
  return d >= 1 && d <= limit;
}
bool datetime(const std::string &s)
{
  int h, m, sec;
  if (s.size() < 19 || !date(s) || (s[10] != 'T' && s[10] != ' ')
      || s[13] != ':' || s[16] != ':' || !digits(s, 11, 2, h)
      || !digits(s, 14, 2, m) || !digits(s, 17, 2, sec) || h > 23 || m > 59 || sec > 59) { return false; }
  if (s.size() == 19) { return true; }
  if (s[19] != '.' || s.size() > 26 || s.size() < 21) { return false; }
  int fraction;
  return digits(s, 20, s.size() - 20, fraction);
}
void utf8_append(std::string &s, uint32_t cp)
{
  if (cp <= 0x7f) { s += char(cp); }
  else if (cp <= 0x7ff) { s += char(0xc0 | (cp >> 6)); s += char(0x80 | (cp & 63)); }
  else if (cp <= 0xffff) { s += char(0xe0 | (cp >> 12)); s += char(0x80 | ((cp >> 6) & 63)); s += char(0x80 | (cp & 63)); }
  else { s += char(0xf0 | (cp >> 18)); s += char(0x80 | ((cp >> 12) & 63)); s += char(0x80 | ((cp >> 6) & 63)); s += char(0x80 | (cp & 63)); }
}
class FlatJson
{
public:
  explicit FlatJson(const std::string &line) : s_(line) {}
  Status parse(std::vector<Cell> &values, std::vector<std::string> &keys)
  {
    whitespace();
    if (!take('{')) { return error(Error::INVALID, "JSONL record must be an object"); }
    whitespace();
    std::set<std::string> seen;
    if (!take('}')) {
      do {
        Cell cell;
        std::string key;
        whitespace();
        if (!string(key) || !seen.insert(key).second) { return error(Error::INVALID, "invalid or duplicate JSON key"); }
        whitespace();
        if (!take(':')) { return error(Error::INVALID, "expected ':' in JSON object"); }
        whitespace();
        if (pos_ < s_.size() && s_[pos_] == '"') {
          if (!string(cell.text)) { return error(Error::INVALID, "invalid JSON string"); }
          cell.quoted = true;
        } else {
          size_t start = pos_;
          while (pos_ < s_.size() && s_[pos_] != ',' && s_[pos_] != '}' && !space(s_[pos_])) { ++pos_; }
          cell.text = s_.substr(start, pos_ - start);
          if (cell.text == "null") { cell.is_null = true; }
          else if (cell.text != "true" && cell.text != "false" && !number(cell.text)) {
            return error(Error::INVALID, "expected scalar JSON value; nested arrays and objects are unsupported");
          }
        }
        keys.push_back(std::move(key));
        values.push_back(std::move(cell));
        whitespace();
        if (take('}')) { break; }
        if (!take(',')) { return error(Error::INVALID, "expected ',' in JSON object"); }
      } while (true);
    }
    whitespace();
    return pos_ == s_.size() ? Status{} : error(Error::INVALID, "trailing data after JSON object");
  }
private:
  static bool space(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }
  void whitespace() { while (pos_ < s_.size() && space(s_[pos_])) { ++pos_; } }
  bool take(char c) { if (pos_ < s_.size() && s_[pos_] == c) { ++pos_; return true; } return false; }
  bool hex(uint32_t &cp)
  {
    cp = 0;
    for (int i = 0; i < 4; ++i) {
      if (pos_ == s_.size()) { return false; }
      char c = s_[pos_++];
      int digit = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
      if (digit < 0) { return false; }
      cp = (cp << 4) | digit;
    }
    return true;
  }
  bool string(std::string &out)
  {
    if (!take('"')) { return false; }
    while (pos_ < s_.size()) {
      unsigned char c = s_[pos_++];
      if (c == '"') { return valid_utf8(out); }
      if (c < 0x20) { return false; }
      if (c != '\\') { out += char(c); continue; }
      if (pos_ == s_.size()) { return false; }
      switch (s_[pos_++]) {
        case '"': out += '"'; break;
        case '\\': out += '\\'; break;
        case '/': out += '/'; break;
        case 'b': out += '\b'; break;
        case 'f': out += '\f'; break;
        case 'n': out += '\n'; break;
        case 'r': out += '\r'; break;
        case 't': out += '\t'; break;
        case 'u': {
          uint32_t cp;
          if (!hex(cp)) { return false; }
          if (cp >= 0xd800 && cp <= 0xdbff) {
            uint32_t low;
            if (!take('\\') || !take('u') || !hex(low) || low < 0xdc00 || low > 0xdfff) { return false; }
            cp = 0x10000 + ((cp - 0xd800) << 10) + low - 0xdc00;
          } else if (cp >= 0xdc00 && cp <= 0xdfff) { return false; }
          utf8_append(out, cp);
          break;
        }
        default: return false;
      }
    }
    return false;
  }
  static bool number(const std::string &s)
  {
    size_t i = !s.empty() && s[0] == '-' ? 1 : 0;
    if (i == s.size()) { return false; }
    if (s[i] == '0') { ++i; }
    else {
      if (s[i] < '1' || s[i] > '9') { return false; }
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; }
    }
    if (i < s.size() && s[i] == '.') {
      size_t start = ++i;
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; }
      if (i == start) { return false; }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
      ++i;
      if (i < s.size() && (s[i] == '+' || s[i] == '-')) { ++i; }
      size_t start = i;
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') { ++i; }
      if (i == start) { return false; }
    }
    return i == s.size();
  }
  const std::string &s_;
  size_t pos_ = 0;
};
bool wildcard(const std::string &pattern, const std::string &name)
{
  size_t p = 0, n = 0, star = std::string::npos, match = 0;
  while (n < name.size()) {
    if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == name[n])) { ++p; ++n; }
    else if (p < pattern.size() && pattern[p] == '*') { star = p++; match = n; }
    else if (star != std::string::npos) { p = star + 1; n = ++match; }
    else { return false; }
  }
  while (p < pattern.size() && pattern[p] == '*') { ++p; }
  return p == pattern.size();
}
} // namespace

bool valid_utf8(const std::string &s)
{
  for (size_t i = 0; i < s.size();) {
    uint32_t cp = static_cast<unsigned char>(s[i++]);
    if (cp <= 0x7f) { continue; }
    int extra;
    uint32_t minimum;
    if (cp >= 0xc2 && cp <= 0xdf) { extra = 1; cp &= 31; minimum = 0x80; }
    else if (cp >= 0xe0 && cp <= 0xef) { extra = 2; cp &= 15; minimum = 0x800; }
    else if (cp >= 0xf0 && cp <= 0xf4) { extra = 3; cp &= 7; minimum = 0x10000; }
    else { return false; }
    if (i + extra > s.size()) { return false; }
    while (extra--) {
      unsigned char c = s[i++];
      if ((c & 0xc0) != 0x80) { return false; }
      cp = (cp << 6) | (c & 63);
    }
    if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) { return false; }
  }
  return true;
}

Type infer_type(const Cell &cell)
{
  if (cell.is_null) { return Type::NULL_VALUE; }
  const std::string &s = cell.text;
  if (s == "true" || s == "false" || s == "TRUE" || s == "FALSE") { return Type::BOOLEAN; }
  int64_t integer;
  auto i = std::from_chars(s.data(), s.data() + s.size(), integer);
  if (!s.empty() && i.ec == std::errc{} && i.ptr == s.data() + s.size()) { return Type::BIGINT; }
  double real;
  auto d = std::from_chars(s.data(), s.data() + s.size(), real);
  if (!s.empty() && d.ec == std::errc{} && d.ptr == s.data() + s.size() && std::isfinite(real)) { return Type::DOUBLE; }
  if (s.size() == 10 && date(s)) { return Type::DATE; }
  if (datetime(s)) { return Type::DATETIME; }
  return Type::VARCHAR;
}

Type widen(Type a, Type b)
{
  if (a == b || b == Type::NULL_VALUE) { return a; }
  if (a == Type::NULL_VALUE) { return b; }
  if ((a == Type::BIGINT && b == Type::DOUBLE) || (a == Type::DOUBLE && b == Type::BIGINT)) { return Type::DOUBLE; }
  if ((a == Type::DATE && b == Type::DATETIME) || (a == Type::DATETIME && b == Type::DATE)) { return Type::DATETIME; }
  return Type::VARCHAR;
}

Status authorize_path(const std::string &path, const std::string &secure_root,
    std::string &canonical_path, bool output)
{
  if (path.empty() || path.find('\0') != std::string::npos || secure_root.empty()
      || secure_root.find('\0') != std::string::npos || secure_root == "NULL") {
    return error(Error::DENIED, "file SQL requires a nonempty secure_file_priv directory and a valid path");
  }
  std::error_code ec;
  fs::path root = fs::canonical(utf8_path(secure_root), ec);
  if (ec || !fs::is_directory(root, ec)) { return error(Error::DENIED, "invalid secure_file_priv directory"); }
  fs::path requested = utf8_path(path);
  fs::path actual;
  if (output) {
    if (fs::exists(requested, ec) || fs::is_symlink(requested, ec)) { return error(Error::IO, "output file already exists"); }
    ec.clear();
    actual = fs::canonical(requested.has_parent_path() ? requested.parent_path() : fs::path("."), ec) / requested.filename();
  } else { actual = fs::canonical(requested, ec); }
  if (ec) { return error(Error::IO, "file path cannot be resolved"); }
  if (!within(actual, root)) { return error(Error::DENIED, "file path escapes secure_file_priv"); }
  if (!output && !fs::is_regular_file(actual, ec)) { return error(Error::INVALID, "input must be a regular file"); }
  canonical_path = path_text(actual);
  return {};
}

Status discover(const std::string &pattern, const std::string &root, std::vector<std::string> &files)
{
  files.clear();
  if (pattern.find('\0') != std::string::npos) { return error(Error::DENIED, "invalid file pattern"); }
  fs::path p = utf8_path(pattern);
  fs::path directory = p.has_parent_path() ? p.parent_path() : fs::path(".");
  if (path_text(directory).find_first_of("*?") != std::string::npos) { return error(Error::UNSUPPORTED, "wildcards are supported in the filename only"); }
  std::error_code ec;
  fs::path parent = fs::canonical(directory, ec);
  fs::path allowed = fs::canonical(utf8_path(root), ec);
  if (root.empty() || ec || !within(parent, allowed)) { return error(Error::DENIED, "glob directory escapes secure_file_priv"); }
  fs::directory_iterator entries(parent, ec);
  if (ec) { return error(Error::IO, "cannot list glob directory"); }
  for (const auto &entry : entries) {
    if (files.size() == 100000) { return error(Error::LIMIT, "glob matched too many files"); }
    if (entry.is_regular_file(ec) && wildcard(path_text(p.filename()), path_text(entry.path().filename()))) {
      std::string actual;
      Status status = authorize_path(path_text(entry.path()), root, actual);
      if (!status) { return status; }
      files.push_back(std::move(actual));
    }
    if (ec) { return error(Error::IO, "glob directory changed"); }
  }
  std::sort(files.begin(), files.end());
  return {};
}

Reader::Reader() = default;
Reader::~Reader() { close(); }
void Reader::close() { if (input_) { fclose(input_); input_ = nullptr; } parquet_.reset(); }
Status Reader::open(const Schema &schema, const std::string &root, const std::vector<size_t> *projection)
{
  close();
  schema_ = schema;
  secure_root_ = root;
  record_number_ = 0;
  discovered_index_ = 0;
  discovered_.clear();
  projected_ = projection != nullptr;
  projection_ = projection ? *projection : std::vector<size_t>{};
  if (schema.options.format == Format::GLOB) { return discover(schema.path, root, discovered_); }
  std::string actual;
  Status s = authorize_path(schema.path, root, actual);
  if (!s) { return s; }
  s = open_input(actual, root, input_);
  if (!s) { return s; }
  s = check_identity();
  if (!s) { close(); return s; }
  if (schema.options.format == Format::PARQUET) {
    parquet_ = std::make_unique<ParquetReader>();
    return parquet_->open(input_, schema.columns, schema.options.max_record_bytes, projected_ ? &projection_ : nullptr);
  }
  unsigned char bom[3];
  size_t count = fread(bom, 1, 3, input_);
  if (count != 3 || bom[0] != 0xef || bom[1] != 0xbb || bom[2] != 0xbf) { rewind(input_); }
  if (schema.options.format == Format::CSV && schema.options.header) {
    std::vector<Cell> ignored;
    return csv_record(ignored);
  }
  return {};
}
Status Reader::rescan()
{
  Schema copy = schema_;
  std::vector<size_t> projection = projection_;
  return open(copy, secure_root_, projected_ ? &projection : nullptr);
}
Status Reader::check_identity()
{
  Identity current;
  Status s = identity(input_, current);
  if (!s) { return s; }
  if (!(current == schema_.identity)) { return error(Error::CHANGED, "file changed since schema inference"); }
  FILE *path_file = nullptr;
  s = open_input(schema_.path, secure_root_, path_file);
  if (!s) { return error(Error::CHANGED, "file path changed during scan"); }
  Identity named;
  s = identity(path_file, named);
  fclose(path_file);
  return !s || !(current == named) ? error(Error::CHANGED, "file replaced during scan") : Status{};
}
Status Reader::next(std::vector<Cell> &row)
{
  row.clear();
  if (schema_.options.format == Format::GLOB) {
    if (discovered_index_ == discovered_.size()) { return {Error::END, {}}; }
    row.push_back({discovered_[discovered_index_++]});
    return {};
  }
  if (!input_) { return error(Error::IO, "file scanner is not open"); }
  Status s = check_identity();
  if (!s) { return s; }
  if (parquet_) { s = parquet_->next(row); }
  else { s = next_raw(row); }
  if (!s) { return s; }
  if (row.size() != schema_.columns.size()) { return error(Error::INVALID, "record column count does not match schema"); }
  s = check_identity();
  return s;
}
Status Reader::next_raw(std::vector<Cell> &row, std::vector<std::string> *keys)
{
  Status s = schema_.options.format == Format::CSV ? csv_record(row) : json_record(row, keys);
  if (!s && s.code != Error::END) { s.message = schema_.path + ": record " + std::to_string(record_number_) + ": " + s.message; }
  return s;
}

Status Reader::csv_record(std::vector<Cell> &row)
{
  row.clear();
  const Options &o = schema_.options;
  bool quoted = false, after_quote = false, any = false;
  Cell cell;
  uint64_t bytes = 0;
  auto field = [&]() {
    cell.is_null = !cell.quoted && cell.text == o.null_text;
    row.push_back(std::move(cell));
    cell = {};
  };
  ++record_number_;
  for (;;) {
    int c = fgetc(input_);
    if (c == EOF) {
      if (ferror(input_)) { return error(Error::IO, "input read failed"); }
      if (quoted) { return error(Error::INVALID, "unterminated quoted CSV field"); }
      if (!any && row.empty()) { return {Error::END, {}}; }
      field(); break;
    }
    any = true;
    if (++bytes > o.max_record_bytes) { return error(Error::LIMIT, "record exceeds maximum size"); }
    if (quoted) {
      if (c == o.quote) {
        if (o.escape == o.quote) {
          int next = fgetc(input_);
          if (next == o.quote) { cell.text += char(c); ++bytes; }
          else { if (next != EOF) { ungetc(next, input_); } quoted = false; after_quote = true; }
        } else { quoted = false; after_quote = true; }
      } else if (c == o.escape && o.escape != o.quote) {
        int next = fgetc(input_);
        if (next == EOF || (next != o.quote && next != o.escape)) { return error(Error::INVALID, "invalid CSV escape"); }
        cell.text += char(next); ++bytes;
      } else if (c == '\r') {
        int next = fgetc(input_);
        if (next == '\n') { cell.text += '\n'; ++bytes; }
        else { cell.text += '\r'; if (next != EOF) { ungetc(next, input_); } }
      } else { cell.text += char(c); }
    } else if (c == o.delimiter) {
      field(); after_quote = false;
      if (row.size() >= o.max_columns) { return error(Error::LIMIT, "too many CSV columns"); }
    } else if (c == '\n' || c == '\r') {
      if (c == '\r') { int next = fgetc(input_); if (next != '\n' && next != EOF) { ungetc(next, input_); } }
      field(); break;
    } else if (after_quote) { return error(Error::INVALID, "unexpected character after closing CSV quote"); }
    else if (c == o.quote) {
      if (!cell.text.empty()) { return error(Error::INVALID, "quote inside unquoted CSV field"); }
      quoted = true; cell.quoted = true;
    } else { cell.text += char(c); }
  }
  for (const auto &value : row) { if (!valid_utf8(value.text)) { return error(Error::INVALID, "invalid UTF-8 CSV field"); } }
  return {};
}

Status Reader::json_record(std::vector<Cell> &row, std::vector<std::string> *keys)
{
  std::string line;
  for (;;) {
    int c = fgetc(input_);
    if (c == EOF) {
      if (ferror(input_)) { return error(Error::IO, "input read failed"); }
      if (line.empty()) { return {Error::END, {}}; }
      break;
    }
    if (c == '\n') { break; }
    line += char(c);
    if (line.size() > schema_.options.max_record_bytes) { return error(Error::LIMIT, "JSONL record exceeds maximum size"); }
  }
  ++record_number_;
  std::vector<std::string> names;
  std::vector<Cell> values;
  Status s = FlatJson(line).parse(values, names);
  if (!s) { return s; }
  if (names.size() > schema_.options.max_columns) { return error(Error::LIMIT, "too many JSONL columns"); }
  if (keys) { *keys = std::move(names); row = std::move(values); }
  else {
    row.assign(schema_.columns.size(), Cell{{}, true});
    for (size_t i = 0; i < names.size(); ++i) {
      auto column = std::find_if(schema_.columns.begin(), schema_.columns.end(), [&](const Column &c) { return c.name == names[i]; });
      if (column == schema_.columns.end()) { return error(Error::CHANGED, "JSONL schema changed"); }
      row[column - schema_.columns.begin()] = std::move(values[i]);
    }
  }
  return {};
}

Status infer_schema(const std::string &path, const Options &options, const std::string &root, Schema &schema)
{
  schema = {};
  if (options.sample_size != -1) { return error(Error::UNSUPPORTED, "sample_size only supports -1"); }
  if (options.format == Format::GLOB) {
    std::vector<std::string> files;
    Status s = discover(path, root, files);
    if (!s) { return s; }
    schema.path = path;
    schema.options = options;
    schema.columns.push_back({"file", Type::VARCHAR});
    return {};
  }
  if (options.delimiter == options.quote || options.delimiter == '\n' || options.delimiter == '\r'
      || options.delimiter == '\0' || options.quote == '\0' || options.escape == '\0') {
    return error(Error::INVALID, "invalid CSV separator, quote or escape");
  }
  Status s = authorize_path(path, root, schema.path);
  if (!s) { return s; }
  schema.options = options;
  Reader reader;
  reader.schema_ = schema;
  reader.secure_root_ = root;
  s = open_input(schema.path, root, reader.input_);
  if (!s) { return s; }
  s = identity(reader.input_, schema.identity);
  if (!s) { return s; }
  reader.schema_.identity = schema.identity;
  if (options.format == Format::PARQUET) {
    s = ParquetReader::schema(reader.input_, schema.columns, options.max_record_bytes);
    return s ? reader.check_identity() : s;
  }
  unsigned char bom[3];
  size_t count = fread(bom, 1, 3, reader.input_);
  if (count != 3 || bom[0] != 0xef || bom[1] != 0xbb || bom[2] != 0xbf) { rewind(reader.input_); }
  std::vector<Cell> row;
  std::vector<std::string> keys;
  bool first = true;
  std::set<std::string> names;
  auto identifier_key = [](std::string name) {
    for (char &c : name) { if (c >= 'A' && c <= 'Z') { c += 'a' - 'A'; } }
    return name;
  };
  for (;;) {
    s = reader.next_raw(row, options.format == Format::JSONL ? &keys : nullptr);
    if (s.code == Error::END) { break; }
    if (!s) { return s; }
    if (first && options.format == Format::CSV) {
      for (size_t i = 0; i < row.size(); ++i) {
        std::string base = options.header ? row[i].text : "column" + std::to_string(i);
        if (base.empty()) { base = "column" + std::to_string(i); }
        std::string name = base;
        for (size_t suffix = 2; !names.insert(identifier_key(name)).second; ++suffix) { name = base + "__" + std::to_string(suffix); }
        schema.columns.push_back({name, Type::NULL_VALUE});
      }
      first = false;
      if (options.header) { continue; }
    }
    if (options.format == Format::CSV) {
      if (row.size() != schema.columns.size()) { return error(Error::INVALID, schema.path + ": record " + std::to_string(reader.record_number_) + ": wrong number of CSV fields"); }
      for (size_t i = 0; i < row.size(); ++i) {
        schema.columns[i].type = widen(schema.columns[i].type, infer_type(row[i]));
        schema.columns[i].nullable |= row[i].is_null;
      }
    } else {
      for (size_t i = 0; i < keys.size(); ++i) {
        auto col = std::find_if(schema.columns.begin(), schema.columns.end(), [&](const Column &c) { return c.name == keys[i]; });
        // Quoted JSON values are strings, even if their spelling resembles a number or boolean.
        Type t = row[i].quoted ? Type::VARCHAR : infer_type(row[i]);
        if (row[i].quoted && row[i].text.size() == 10 && date(row[i].text)) { t = Type::DATE; }
        else if (row[i].quoted && datetime(row[i].text)) { t = Type::DATETIME; }
        if (col == schema.columns.end()) {
          if (!names.insert(identifier_key(keys[i])).second) {
            return error(Error::INVALID, "JSONL keys collide as SQL column identifiers");
          }
          schema.columns.push_back({keys[i], t, reader.record_number_ > 1 || row[i].is_null});
        }
        else { col->type = widen(col->type, t); col->nullable |= row[i].is_null; }
      }
      for (auto &column : schema.columns) {
        if (std::find(keys.begin(), keys.end(), column.name) == keys.end()) { column.nullable = true; }
      }
      if (schema.columns.size() > options.max_columns) { return error(Error::LIMIT, "too many inferred JSONL columns"); }
    }
  }
  if (schema.columns.empty()) { return error(Error::INVALID, "input has no columns"); }
  return reader.check_identity();
}

bool is_file_plan(const std::string &encoded) { return encoded.starts_with("FILESQL1"); }
Status encode_schema(const Schema &schema, std::string &encoded)
{
  encoded = "FILESQL1";
  auto number = [&](uint64_t value) { for (unsigned i = 0; i < 8; ++i) { encoded += char(value >> (i * 8)); } };
  auto text = [&](const std::string &value) { number(value.size()); encoded += value; };
  number(static_cast<unsigned>(schema.options.format));
  number(schema.options.header);
  number(uint8_t(schema.options.delimiter)); number(uint8_t(schema.options.quote)); number(uint8_t(schema.options.escape));
  text(schema.options.null_text);
  number(schema.options.max_record_bytes); number(schema.options.max_columns);
  number(schema.identity.size); number(uint64_t(schema.identity.modified)); number(schema.identity.device); number(schema.identity.inode);
  text(schema.path);
  number(schema.columns.size());
  for (const auto &column : schema.columns) { text(column.name); number(static_cast<unsigned>(column.type)); number(column.nullable); }
  return encoded.size() <= 16 * 1024 * 1024 ? Status{} : error(Error::LIMIT, "file schema exceeds plan size limit");
}
Status decode_schema(const std::string &encoded, Schema &schema)
{
  schema = {};
  if (!is_file_plan(encoded) || encoded.size() > 16 * 1024 * 1024) { return error(Error::INVALID, "invalid file scan plan"); }
  size_t pos = 8;
  bool valid = true;
  auto number = [&]() -> uint64_t {
    if (!valid || encoded.size() - pos < 8) { valid = false; return 0; }
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) { value |= uint64_t(uint8_t(encoded[pos++])) << (8 * i); }
    return value;
  };
  auto text = [&]() -> std::string {
    uint64_t size = number();
    if (!valid || size > encoded.size() - pos) { valid = false; return {}; }
    std::string result(encoded, pos, size); pos += size; return result;
  };
  uint64_t format = number(); schema.options.format = static_cast<Format>(format);
  uint64_t header = number(); schema.options.header = header != 0;
  schema.options.delimiter = char(number()); schema.options.quote = char(number()); schema.options.escape = char(number());
  schema.options.null_text = text();
  schema.options.max_record_bytes = number(); schema.options.max_columns = number();
  schema.identity.size = number(); schema.identity.modified = int64_t(number());
  schema.identity.device = number(); schema.identity.inode = number();
  schema.path = text();
  uint64_t count = number();
  if (format > static_cast<unsigned>(Format::GLOB) || header > 1 || count == 0 || count > 4096
      || schema.options.max_columns > 4096 || schema.options.max_record_bytes > 16 * 1024 * 1024) { valid = false; }
  for (uint64_t i = 0; valid && i < count; ++i) {
    std::string name = text();
    uint64_t type = number();
    uint64_t nullable = number();
    if (type > static_cast<unsigned>(Type::DATETIME) || nullable > 1) { valid = false; }
    schema.columns.push_back({name, static_cast<Type>(type), nullable != 0});
  }
  return valid && pos == encoded.size() ? Status{} : error(Error::INVALID, "invalid file scan plan");
}

CsvWriter::~CsvWriter()
{
  if (output_) { fclose(output_); }
#ifdef _WIN32
  if (!temporary_.empty()) { std::error_code ec; fs::remove(utf8_path(temporary_), ec); }
  if (directory_descriptor_ != -1) { CloseHandle(reinterpret_cast<HANDLE>(directory_descriptor_)); }
#else
  if (directory_descriptor_ >= 0) {
    if (!temporary_.empty()) { ::unlinkat(int(directory_descriptor_), utf8_path(temporary_).filename().c_str(), 0); }
    ::close(int(directory_descriptor_));
  }
#endif
}
Status CsvWriter::open(const std::string &path, const std::string &root, const Options &options)
{
  if (output_ || !temporary_.empty() || directory_descriptor_ != -1) { return error(Error::INVALID, "CSV writer already open"); }
  if (options.delimiter == options.quote || options.delimiter == '\n' || options.delimiter == '\r'
      || options.delimiter == '\0' || options.quote == '\0' || options.escape == '\0'
      || options.null_text.find_first_of(std::string{options.delimiter, options.quote, '\r', '\n', '\0'}) != std::string::npos) {
    return error(Error::INVALID, "invalid CSV output options");
  }
  Status s = authorize_path(path, root, target_, true);
  if (!s) { return s; }
  secure_root_ = root;
  const fs::path parent = utf8_path(target_).parent_path();
  static std::atomic<uint64_t> sequence{0};
#ifdef _WIN32
  const uint64_t pid = GetCurrentProcessId();
  HANDLE directory = CreateFileW(parent.c_str(), FILE_READ_ATTRIBUTES,
      FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING,
      FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
  if (directory == INVALID_HANDLE_VALUE) { return error(Error::IO, "cannot pin output directory"); }
  directory_descriptor_ = reinterpret_cast<intptr_t>(directory);
#else
  const uint64_t pid = getpid();
  directory_descriptor_ = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (directory_descriptor_ < 0) { return error(Error::IO, "cannot pin output directory"); }
#endif
  temporary_ = target_ + ".tmp." + std::to_string(pid) + "." + std::to_string(++sequence);
  // Reserve the temporary path exclusively before using a stream.
#ifdef _WIN32
  HANDLE handle = CreateFileW(utf8_path(temporary_).c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) { temporary_.clear(); return error(Error::IO, "cannot create temporary CSV output"); }
  wchar_t actual_buffer[32768], directory_buffer[32768];
  DWORD actual_count = GetFinalPathNameByHandleW(handle, actual_buffer, 32768, FILE_NAME_NORMALIZED);
  DWORD directory_count = GetFinalPathNameByHandleW(directory, directory_buffer, 32768, FILE_NAME_NORMALIZED);
  if (!actual_count || actual_count >= 32768 || !directory_count || directory_count >= 32768
      || fs::path(std::wstring(actual_buffer, actual_count)).parent_path()
          != fs::path(std::wstring(directory_buffer, directory_count))) {
    CloseHandle(handle);
    return error(Error::CHANGED, "output directory changed during open");
  }
  int fd = _open_osfhandle(reinterpret_cast<intptr_t>(handle), _O_WRONLY | _O_BINARY);
  if (fd < 0) { CloseHandle(handle); return error(Error::IO, "cannot open output descriptor"); }
  output_ = _fdopen(fd, "wb");
  if (!output_) { _close(fd); }
#else
  int fd = ::openat(int(directory_descriptor_), utf8_path(temporary_).filename().c_str(),
      O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) { temporary_.clear(); return error(Error::IO, "cannot create temporary CSV output"); }
  output_ = fdopen(fd, "wb");
  if (!output_) { ::close(fd); }
#endif
  options_ = options;
  return output_ ? Status{} : error(Error::IO, "cannot open CSV output");
}
Status CsvWriter::write(const std::vector<Cell> &row)
{
  for (size_t i = 0; i < row.size(); ++i) {
    if (!output_) { return error(Error::IO, "CSV writer is not open"); }
    if (i) { fputc(options_.delimiter, output_); }
    const Cell &cell = row[i];
    if (cell.is_null) { fwrite(options_.null_text.data(), 1, options_.null_text.size(), output_); continue; }
    bool quote = cell.text == options_.null_text || cell.text.find(options_.delimiter) != std::string::npos
        || cell.text.find(options_.quote) != std::string::npos || cell.text.find_first_of("\r\n") != std::string::npos;
    if (quote) { fputc(options_.quote, output_); }
    for (char c : cell.text) {
      if (quote && (c == options_.quote || (options_.escape != options_.quote && c == options_.escape))) { fputc(options_.escape, output_); }
      fputc(c, output_);
    }
    if (quote) { fputc(options_.quote, output_); }
  }
  if (!output_) { return error(Error::IO, "CSV writer is not open"); }
  fputc('\n', output_);
  return !ferror(output_) ? Status{} : error(Error::IO, "cannot write CSV output");
}
Status CsvWriter::finish()
{
  std::string current;
  Status authorized = authorize_path(target_, secure_root_, current, true);
  if (!authorized || current != target_) { return error(Error::CHANGED, "output directory changed"); }
#ifndef _WIN32
  struct stat pinned{}, named{};
  const fs::path parent = utf8_path(target_).parent_path();
  if (fstat(int(directory_descriptor_), &pinned) != 0 || stat(parent.c_str(), &named) != 0
      || pinned.st_dev != named.st_dev || pinned.st_ino != named.st_ino) {
    return error(Error::CHANGED, "output directory changed");
  }
#endif
  if (!output_ || fflush(output_) != 0) { return error(Error::IO, "cannot flush CSV output"); }
  int result = fclose(output_);
  output_ = nullptr;
  if (result != 0) { return error(Error::IO, "cannot close CSV output"); }
  // link() and MoveFile() both fail if a competing writer created the target.
#ifdef _WIN32
  if (!MoveFileW(utf8_path(temporary_).c_str(), utf8_path(target_).c_str())) {
    return error(Error::IO, "cannot publish CSV output (Windows error " + std::to_string(GetLastError()) + ")");
  }
#else
  if (::linkat(int(directory_descriptor_), utf8_path(temporary_).filename().c_str(),
      int(directory_descriptor_), utf8_path(target_).filename().c_str(), 0) != 0) {
    return error(Error::IO, "cannot publish CSV output; target may already exist");
  }
  ::unlinkat(int(directory_descriptor_), utf8_path(temporary_).filename().c_str(), 0);
#endif
  temporary_.clear();
  return {};
}
} // namespace oceanbase::sql::file_sql
