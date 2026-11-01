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
#include "parquet_file_reader.h"
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <set>

namespace oceanbase::sql::file_sql
{
namespace
{
Status invalid(const std::string &text) { return {Error::INVALID, "Parquet: " + text}; }
Status unsupported(const std::string &text) { return {Error::UNSUPPORTED, "Parquet: " + text}; }
Status memory_limit() { return {Error::LIMIT, "Parquet decoded data exceeds memory budget"}; }
int seek(FILE *f, uint64_t offset)
{
  if (offset > uint64_t(INT64_MAX)) { return -1; }
#ifdef _WIN32
  return _fseeki64(f, offset, SEEK_SET);
#else
  return fseeko(f, offset, SEEK_SET);
#endif
}
int64_t tell(FILE *f)
{
#ifdef _WIN32
  return _ftelli64(f);
#else
  return ftello(f);
#endif
}
uint32_t le32(const char *p)
{
  return uint32_t(uint8_t(p[0])) | (uint32_t(uint8_t(p[1])) << 8)
      | (uint32_t(uint8_t(p[2])) << 16) | (uint32_t(uint8_t(p[3])) << 24);
}
uint64_t le64(const char *p) { return uint64_t(le32(p)) | (uint64_t(le32(p + 4)) << 32); }
bool varint(const std::string &s, size_t &pos, uint64_t &value)
{
  value = 0;
  for (unsigned shift = 0; shift < 64; shift += 7) {
    if (pos == s.size()) { return false; }
    uint8_t c = s[pos++];
    if (shift == 63 && (c & 0xfe)) { return false; }
    value |= uint64_t(c & 127) << shift;
    if (!(c & 128)) { return true; }
  }
  return false;
}
struct Value
{
  int64_t number = 0;
  std::string text;
  std::map<int, Value> fields;
  std::vector<Value> list;
  const Value *get(int id) const { auto it = fields.find(id); return it == fields.end() ? nullptr : &it->second; }
  int64_t integer(int id, int64_t fallback = 0) const { const Value *v = get(id); return v ? v->number : fallback; }
};
class Compact
{
public:
  explicit Compact(const std::string &data) : data_(data) {}
  bool object(Value &value, unsigned depth = 0)
  {
    if (depth > 32) { return false; }
    int last = 0;
    while (pos_ < data_.size()) {
      unsigned field = uint8_t(data_[pos_++]);
      if (field == 0) { return true; }
      int id = field >> 4;
      if (id) { id += last; }
      else { uint64_t raw; if (!varint(data_, pos_, raw)) { return false; } id = int((raw >> 1) ^ uint64_t(-int64_t(raw & 1))); }
      if (id <= 0 || value.fields.contains(id) || ++nodes_ > 1000000) { return false; }
      last = id;
      if (!read(field & 15, value.fields[id], depth + 1)) { return false; }
    }
    return false;
  }
  size_t consumed() const { return pos_; }
private:
  bool read(unsigned type, Value &out, unsigned depth)
  {
    if (depth > 32) { return false; }
    uint64_t raw;
    switch (type) {
      case 1: out.number = 1; return true;
      case 2: out.number = 0; return true;
      case 3: if (pos_ == data_.size()) { return false; } out.number = int8_t(data_[pos_++]); return true;
      case 4: case 5: case 6:
        if (!varint(data_, pos_, raw)) { return false; }
        out.number = int64_t(raw >> 1) ^ -int64_t(raw & 1); return true;
      case 7: if (data_.size() - pos_ < 8) { return false; } pos_ += 8; return true;
      case 8:
        if (!varint(data_, pos_, raw) || raw > data_.size() - pos_) { return false; }
        out.text.assign(data_, pos_, raw); pos_ += raw; return true;
      case 9: case 10: {
        if (pos_ == data_.size()) { return false; }
        unsigned header = uint8_t(data_[pos_++]);
        uint64_t count = header >> 4;
        if (count == 15 && !varint(data_, pos_, count)) { return false; }
        if (count > 1000000 - nodes_) { return false; }
        nodes_ += count;
        out.list.resize(count);
        for (auto &item : out.list) {
          if (!read(header & 15, item, depth + 1)) { return false; }
        }
        return true;
      }
      case 12: return object(out, depth + 1);
      default: return false;
    }
  }
  const std::string &data_;
  size_t pos_ = 0;
  size_t nodes_ = 0;
};
struct ColumnMeta
{
  std::string name;
  int physical = -1;
  int converted = -1;
  int time_unit = 0;
  bool optional = false;
  Type type = Type::VARCHAR;
};
struct Chunk
{
  int codec = 0;
  uint64_t start = 0;
  uint64_t end = 0;
  uint64_t rows = 0;
};
struct Group
{
  uint64_t rows = 0;
  std::vector<Chunk> chunks;
};
struct Footer
{
  std::vector<ColumnMeta> columns;
  std::vector<Group> groups;
  uint64_t rows = 0;
  uint64_t data_end = 0;
};
Status footer(FILE *f, uint64_t limit, Footer &out)
{
#ifdef _WIN32
  if (_fseeki64(f, 0, SEEK_END) != 0) { return invalid("cannot seek footer"); }
#else
  if (fseeko(f, 0, SEEK_END) != 0) { return invalid("cannot seek footer"); }
#endif
  int64_t size = tell(f);
  char end[8], magic[4];
  if (size < 12 || seek(f, size - 8) != 0 || fread(end, 1, 8, f) != 8
      || std::memcmp(end + 4, "PAR1", 4) != 0 || seek(f, 0) != 0
      || fread(magic, 1, 4, f) != 4 || std::memcmp(magic, "PAR1", 4) != 0) { return invalid("invalid file magic"); }
  const uint32_t bytes = le32(end);
  if (bytes > limit || bytes > uint64_t(size - 12)) { return invalid("invalid or oversized footer"); }
  std::string encoded(bytes, '\0');
  out.data_end = size - bytes - 8;
  if (seek(f, out.data_end) != 0 || fread(encoded.data(), 1, bytes, f) != bytes) { return invalid("truncated footer"); }
  Value metadata;
  Compact compact(encoded);
  if (!compact.object(metadata) || compact.consumed() != encoded.size() || !metadata.get(2)
      || !metadata.get(3) || !metadata.get(4) || metadata.integer(3) < 0) { return invalid("invalid file metadata"); }
  out.rows = metadata.integer(3);
  const auto &schema = metadata.get(2)->list;
  if (schema.size() < 2 || schema.size() > 4097 || schema[0].integer(5) != schema.size() - 1) { return unsupported("only flat scalar schemas are supported"); }
  std::set<std::string> names;
  for (size_t i = 1; i < schema.size(); ++i) {
    const Value &value = schema[i];
    ColumnMeta column;
    if (!value.get(1) || !value.get(4) || value.integer(5) != 0 || value.integer(3) > 1) { return unsupported("nested and repeated columns are unsupported"); }
    column.name = value.get(4)->text;
    column.physical = value.integer(1);
    column.converted = value.integer(6, -1);
    column.optional = value.integer(3) == 1;
    if (!valid_utf8(column.name) || column.name.empty() || !names.insert(column.name).second) { return invalid("invalid or duplicate column name"); }
    switch (column.physical) {
      case 0: column.type = Type::BOOLEAN; break;
      case 1: case 2: column.type = Type::BIGINT; break;
      case 4: case 5: column.type = Type::DOUBLE; break;
      case 6: column.type = Type::VARCHAR; break;
      default: return unsupported("unsupported physical column type");
    }
    if (column.converted == 6 && column.physical == 1) { column.type = Type::DATE; }
    if ((column.converted == 9 || column.converted == 10) && column.physical == 2) {
      column.type = Type::DATETIME;
      column.time_unit = column.converted == 9 ? 1000 : 1000000;
    }
    if (const Value *logical = value.get(10)) {
      if (logical->get(6) && column.physical == 1) { column.type = Type::DATE; }
      if (const Value *timestamp = logical->get(8)) {
        const Value *unit = timestamp->get(2);
        if (column.physical != 2 || !unit) { return invalid("invalid timestamp schema"); }
        column.type = Type::DATETIME;
        column.time_unit = unit->get(1) ? 1000 : unit->get(2) ? 1000000 : unit->get(3) ? 1000000000 : 0;
        if (!column.time_unit) { return unsupported("timestamp unit"); }
      }
    }
    out.columns.push_back(std::move(column));
  }
  uint64_t total = 0;
  for (const Value &rg : metadata.get(4)->list) {
    Group group;
    if (rg.integer(3, -1) < 0 || !rg.get(1) || rg.get(1)->list.size() != out.columns.size()) { return invalid("invalid row group"); }
    group.rows = rg.integer(3);
    if (group.rows > out.rows - total) { return invalid("invalid row group row count"); }
    total += group.rows;
    for (size_t i = 0; i < out.columns.size(); ++i) {
      const Value &cc = rg.get(1)->list[i];
      const Value *md = cc.get(3);
      if (cc.get(1) || !md || md->integer(1, -1) != out.columns[i].physical || !md->get(3)
          || md->get(3)->list.size() != 1 || md->get(3)->list[0].text != out.columns[i].name
          || md->integer(5, -1) != group.rows || md->integer(7, -1) < 0 || md->integer(9, -1) < 4) { return invalid("invalid column chunk"); }
      Chunk chunk;
      chunk.codec = md->integer(4);
      if (chunk.codec != 0 && chunk.codec != 1) { return unsupported("only uncompressed and Snappy compression are supported"); }
      chunk.start = md->integer(11, md->integer(9));
      if (chunk.start < 4 || chunk.start > out.data_end || uint64_t(md->integer(7)) > out.data_end - chunk.start) { return invalid("column chunk extends outside data region"); }
      chunk.end = chunk.start + md->integer(7);
      chunk.rows = group.rows;
      group.chunks.push_back(chunk);
    }
    out.groups.push_back(std::move(group));
  }
  return total == out.rows ? Status{} : invalid("row count does not match row groups");
}
Status hybrid(const std::string &s, size_t &pos, unsigned width, size_t count, std::vector<uint32_t> &values)
{
  if (width > 32 || count > 16000000) { return invalid("invalid RLE width or count"); }
  values.clear();
  values.reserve(count);
  while (values.size() < count) {
    uint64_t header;
    if (!varint(s, pos, header) || header == 0) { return invalid("truncated RLE stream"); }
    if (!(header & 1)) {
      size_t bytes = (width + 7) / 8;
      if (bytes > s.size() - pos) { return invalid("truncated RLE value"); }
      uint32_t value = 0;
      for (size_t i = 0; i < bytes; ++i) { value |= uint32_t(uint8_t(s[pos++])) << (8 * i); }
      if (width < 32 && value >= (uint64_t(1) << width)) { return invalid("RLE value exceeds bit width"); }
      size_t run = std::min<uint64_t>(header >> 1, count - values.size());
      values.insert(values.end(), run, value);
    } else {
      uint64_t groups = header >> 1;
      if (groups > (s.size() - pos) / std::max<unsigned>(1, width)) { return invalid("truncated bit-packed stream"); }
      size_t bytes = groups * width;
      size_t run = std::min<uint64_t>(groups * 8, count - values.size());
      for (size_t i = 0; i < run; ++i) {
        uint32_t value = 0;
        for (unsigned bit = 0; bit < width; ++bit) {
          size_t offset = i * width + bit;
          value |= uint32_t((uint8_t(s[pos + offset / 8]) >> (offset % 8)) & 1) << bit;
        }
        values.push_back(value);
      }
      pos += bytes;
    }
  }
  return {};
}
std::string numeric(double value)
{
  char buffer[64];
  auto result = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::general, std::numeric_limits<double>::max_digits10);
  return result.ec == std::errc{} ? std::string(buffer, result.ptr) : std::string{};
}
Status temporal(int64_t value, const ColumnMeta &meta, std::string &text)
{
  using namespace std::chrono;
  int64_t micros = 0;
  int64_t days_since_epoch = value;
  if (meta.type == Type::DATETIME) {
    const int64_t seconds = value / meta.time_unit;
    const int64_t fraction = value % meta.time_unit;
    days_since_epoch = seconds / 86400;
    int64_t remainder = seconds % 86400;
    if (remainder < 0 || (remainder == 0 && fraction < 0)) { --days_since_epoch; }
    int64_t within = seconds - days_since_epoch * 86400;
    micros = within * 1000000 + fraction * 1000000 / meta.time_unit;
  }
  if (days_since_epoch < -719162 || days_since_epoch > 2932896) { return invalid("date outside supported range"); }
  year_month_day ymd{sys_days{days{days_since_epoch}}};
  char buffer[40];
  int length = std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u", int(ymd.year()), unsigned(ymd.month()), unsigned(ymd.day()));
  text.assign(buffer, length);
  if (meta.type == Type::DATETIME) {
    int64_t seconds = micros / 1000000;
    length = std::snprintf(buffer, sizeof(buffer), " %02lld:%02lld:%02lld.%06lld",
        static_cast<long long>(seconds / 3600), static_cast<long long>((seconds / 60) % 60),
        static_cast<long long>(seconds % 60), static_cast<long long>(micros % 1000000));
    text.append(buffer, length);
  }
  return {};
}
Status plain(const std::string &s, const ColumnMeta &meta, size_t count, uint64_t limit, std::vector<Cell> &values)
{
  values.clear();
  if (count > limit / sizeof(Cell)) { return memory_limit(); }
  uint64_t memory = count * sizeof(Cell);
  values.reserve(count);
  size_t pos = 0;
  for (size_t i = 0; i < count; ++i) {
    Cell cell;
    int64_t integer = 0;
    switch (meta.physical) {
      case 0:
        if (i / 8 >= s.size()) { return invalid("truncated BOOLEAN values"); }
        cell.text = (uint8_t(s[i / 8]) >> (i % 8)) & 1 ? "true" : "false";
        break;
      case 1:
        if (s.size() - pos < 4) { return invalid("truncated INT32 values"); }
        integer = int32_t(le32(s.data() + pos)); pos += 4; cell.text = std::to_string(integer); break;
      case 2:
        if (s.size() - pos < 8) { return invalid("truncated INT64 values"); }
        integer = int64_t(le64(s.data() + pos)); pos += 8; cell.text = std::to_string(integer); break;
      case 4: {
        if (s.size() - pos < 4) { return invalid("truncated FLOAT values"); }
        uint32_t bits = le32(s.data() + pos); pos += 4;
        float value; std::memcpy(&value, &bits, 4);
        if (!std::isfinite(value)) { return invalid("nonfinite FLOAT value"); }
        cell.text = numeric(value); break;
      }
      case 5: {
        if (s.size() - pos < 8) { return invalid("truncated DOUBLE values"); }
        uint64_t bits = le64(s.data() + pos); pos += 8;
        double value; std::memcpy(&value, &bits, 8);
        if (!std::isfinite(value)) { return invalid("nonfinite DOUBLE value"); }
        cell.text = numeric(value); break;
      }
      case 6: {
        if (s.size() - pos < 4) { return invalid("truncated BYTE_ARRAY length"); }
        uint32_t length = le32(s.data() + pos); pos += 4;
        if (length > s.size() - pos) { return invalid("truncated BYTE_ARRAY data"); }
        cell.text.assign(s, pos, length); pos += length;
        if (!valid_utf8(cell.text)) { return invalid("BYTE_ARRAY is not UTF-8"); }
        break;
      }
      default: return unsupported("PLAIN physical type");
    }
    if (meta.type == Type::DATE || meta.type == Type::DATETIME) {
      Status status = temporal(integer, meta, cell.text);
      if (!status) { return status; }
    }
    if (cell.text.size() > limit - memory) { return memory_limit(); }
    memory += cell.text.size();
    values.push_back(std::move(cell));
  }
  return {};
}
struct Cursor
{
  Chunk chunk;
  uint64_t offset = 0;
  std::vector<Cell> dictionary;
  std::vector<Cell> page;
  size_t row = 0;
  uint64_t values_read = 0;
};
Status page(FILE *file, Cursor &cursor, const ColumnMeta &meta, uint64_t limit)
{
  while (cursor.offset < cursor.chunk.end) {
    const uint64_t available = cursor.chunk.end - cursor.offset;
    // Page headers are small. Grow only if the entire prefix is a header;
    // no compressed page is read until sizes and the file boundary are checked.
    Value header;
    size_t header_size = 0;
    for (size_t prefix = 256; prefix <= 65536; prefix *= 2) {
      std::string buffer(std::min<uint64_t>(prefix, available), '\0');
      if (seek(file, cursor.offset) != 0 || fread(buffer.data(), 1, buffer.size(), file) != buffer.size()) { return invalid("truncated page header"); }
      header = {};
      Compact compact(buffer);
      if (compact.object(header)) { header_size = compact.consumed(); break; }
      if (buffer.size() < prefix) { break; }
    }
    if (!header_size || header.integer(2, -1) < 0 || header.integer(3, -1) < 0) { return invalid("invalid page header"); }
    uint64_t raw_size = header.integer(2), compressed = header.integer(3);
    if (raw_size > limit || compressed > limit || header_size > available || compressed > available - header_size) { return invalid("oversized or truncated page"); }
    std::string payload(compressed, '\0'), decoded;
    if (seek(file, cursor.offset + header_size) != 0 || fread(payload.data(), 1, payload.size(), file) != payload.size()) { return invalid("truncated page data"); }
    cursor.offset += header_size + compressed;
    const int type = header.integer(1, -1);
    bool v2 = type == 3;
    const Value *data_header = header.get(v2 ? 8 : 5);
    size_t definition_bytes = 0;
    if (v2) {
      if (!data_header || data_header->integer(5, -1) < 0 || data_header->integer(6, -1) != 0) { return invalid("invalid data page V2 levels"); }
      definition_bytes = data_header->integer(5);
      if (definition_bytes > payload.size() || definition_bytes > raw_size) { return invalid("invalid V2 definition level length"); }
    }
    Status status;
    if (cursor.chunk.codec == 1 && (!v2 || data_header->integer(7, 1))) {
      std::string suffix = payload.substr(definition_bytes);
      status = decode_snappy(suffix, raw_size - definition_bytes, limit, decoded);
      if (!status) { return status; }
      decoded.insert(0, payload, 0, definition_bytes);
    } else {
      if (payload.size() != raw_size) { return invalid("uncompressed page size mismatch"); }
      decoded = std::move(payload);
    }
    if (type == 2) {
      const Value *dict = header.get(7);
      if (!dict || dict->integer(1, -1) < 0 || dict->integer(2, -1) != 0 || !cursor.dictionary.empty()) { return invalid("invalid dictionary page"); }
      if (uint64_t(dict->integer(1)) > limit / sizeof(Cell)) { return invalid("dictionary exceeds memory limit"); }
      status = plain(decoded, meta, dict->integer(1), limit, cursor.dictionary);
      if (!status) { return status; }
      continue;
    }
    if (type != 0 && type != 3) { return unsupported("page type"); }
    if (!data_header || data_header->integer(1, -1) < 0) { return invalid("missing data page header"); }
    const uint64_t count = data_header->integer(1);
    if (count > cursor.chunk.rows - cursor.values_read || count > limit / sizeof(Cell)) { return invalid("invalid data page value count"); }
    size_t pos = 0;
    std::vector<uint32_t> levels;
    if (meta.optional) {
      if (!v2) {
        if (decoded.size() < 4 || data_header->integer(3, -1) != 3) { return unsupported("definition level encoding"); }
        definition_bytes = le32(decoded.data()); pos = 4;
      }
      if (definition_bytes > decoded.size() - pos) { return invalid("truncated definition levels"); }
      std::string encoded = decoded.substr(pos, definition_bytes);
      size_t level_pos = 0;
      status = hybrid(encoded, level_pos, 1, count, levels);
      if (!status || level_pos != encoded.size()) { return invalid("invalid definition levels"); }
      pos += definition_bytes;
    } else { levels.assign(count, 1); }
    size_t nonnull = std::count(levels.begin(), levels.end(), 1);
    std::vector<Cell> values;
    const int encoding = data_header->integer(v2 ? 4 : 2, -1);
    if (encoding == 0) { status = plain(decoded.substr(pos), meta, nonnull, limit, values); }
    else if (encoding == 3 && meta.physical == 0) {
      if (decoded.size() - pos < 4) { return invalid("truncated BOOLEAN RLE length"); }
      const uint32_t length = le32(decoded.data() + pos); pos += 4;
      if (length != decoded.size() - pos) { return invalid("invalid BOOLEAN RLE length"); }
      std::vector<uint32_t> booleans;
      status = hybrid(decoded, pos, 1, nonnull, booleans);
      if (!status) { return status; }
      if (nonnull > limit / (sizeof(Cell) + 5)) { return memory_limit(); }
      values.reserve(nonnull);
      for (uint32_t value : booleans) { values.push_back({value ? "true" : "false"}); }
    }
    else if (encoding == 2 || encoding == 8) {
      if (pos == decoded.size()) {
        if (nonnull) { return invalid("missing dictionary indexes"); }
      } else {
        unsigned width = uint8_t(decoded[pos++]);
        std::vector<uint32_t> indexes;
        status = hybrid(decoded, pos, width, nonnull, indexes);
        if (!status) { return status; }
        if (nonnull > limit / sizeof(Cell)) { return memory_limit(); }
        uint64_t memory = nonnull * sizeof(Cell);
        values.reserve(nonnull);
        for (uint32_t index : indexes) {
          if (index >= cursor.dictionary.size()) { return invalid("dictionary index out of range"); }
          if (cursor.dictionary[index].text.size() > limit - memory) { return memory_limit(); }
          memory += cursor.dictionary[index].text.size();
          values.push_back(cursor.dictionary[index]);
        }
      }
    } else { return unsupported("data encoding " + std::to_string(encoding)); }
    if (!status) { return status; }
    cursor.page.clear();
    cursor.page.reserve(count);
    size_t value = 0;
    for (uint32_t level : levels) {
      cursor.page.push_back(level ? std::move(values[value++]) : Cell{{}, true});
    }
    cursor.row = 0;
    cursor.values_read += count;
    if (!cursor.page.empty()) { return {}; }
  }
  return {Error::END, {}};
}
} // namespace

Status decode_snappy(const std::string &input, size_t expected, size_t limit, std::string &output)
{
  output.clear();
  size_t pos = 0;
  uint64_t size;
  if (!varint(input, pos, size) || size != expected || size > limit) { return invalid("invalid Snappy size"); }
  output.reserve(size);
  while (pos < input.size() && output.size() < size) {
    unsigned tag = uint8_t(input[pos++]);
    size_t length = tag >> 2;
    size_t offset = 0;
    switch (tag & 3) {
      case 0:
        if (length < 60) { ++length; }
        else {
          unsigned bytes = length - 59;
          if (bytes > 4 || bytes > input.size() - pos) { return invalid("truncated Snappy literal length"); }
          length = 0;
          for (unsigned i = 0; i < bytes; ++i) { length |= size_t(uint8_t(input[pos++])) << (8 * i); }
          ++length;
        }
        if (length > input.size() - pos || length > size - output.size()) { return invalid("invalid Snappy literal"); }
        output.append(input, pos, length); pos += length; continue;
      case 1:
        if (pos == input.size()) { return invalid("truncated Snappy copy"); }
        length = 4 + ((tag >> 2) & 7); offset = ((tag & 0xe0) << 3) | uint8_t(input[pos++]); break;
      case 2:
        if (input.size() - pos < 2) { return invalid("truncated Snappy copy"); }
        length += 1; offset = uint8_t(input[pos]) | (size_t(uint8_t(input[pos + 1])) << 8); pos += 2; break;
      case 3:
        if (input.size() - pos < 4) { return invalid("truncated Snappy copy"); }
        length += 1; offset = le32(input.data() + pos); pos += 4; break;
    }
    if (!offset || offset > output.size() || length > size - output.size()) { return invalid("invalid Snappy copy offset"); }
    for (size_t i = 0; i < length; ++i) { output += output[output.size() - offset]; }
  }
  return output.size() == size && pos == input.size() ? Status{} : invalid("Snappy output size mismatch");
}

struct ParquetReader::Impl
{
  FILE *file = nullptr;
  uint64_t limit = 0;
  Footer footer;
  size_t group = 0;
  uint64_t row = 0;
  std::vector<Cursor> cursors;
  std::vector<bool> selected;
};
ParquetReader::ParquetReader() : impl_(std::make_unique<Impl>()) {}
ParquetReader::~ParquetReader() = default;
Status ParquetReader::schema(FILE *file, std::vector<Column> &columns, uint64_t limit)
{
  Footer metadata;
  Status status = footer(file, limit, metadata);
  if (status) {
    columns.clear();
    for (const auto &column : metadata.columns) { columns.push_back({column.name, column.type, column.optional}); }
  }
  return status;
}
Status ParquetReader::open(FILE *file, const std::vector<Column> &columns, uint64_t limit,
    const std::vector<size_t> *projection)
{
  *impl_ = {};
  impl_->file = file;
  impl_->limit = limit;
  Status s = footer(file, limit, impl_->footer);
  if (!s) { return s; }
  if (columns.size() != impl_->footer.columns.size()) { return invalid("schema changed"); }
  impl_->selected.assign(columns.size(), projection == nullptr);
  if (projection) {
    for (size_t index : *projection) {
      if (index >= columns.size()) { return invalid("projection column outside schema"); }
      impl_->selected[index] = true;
    }
  }
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].name != impl_->footer.columns[i].name || columns[i].type != impl_->footer.columns[i].type) { return invalid("schema changed"); }
  }
  const size_t selected = std::count(impl_->selected.begin(), impl_->selected.end(), true);
  impl_->limit = std::min<uint64_t>(limit, (128ULL * 1024 * 1024) / (8 * std::max<size_t>(1, selected)));
  return {};
}
Status ParquetReader::next(std::vector<Cell> &row)
{
  Impl &state = *impl_;
  while (state.group < state.footer.groups.size() && state.row == state.footer.groups[state.group].rows) {
    ++state.group; state.row = 0; state.cursors.clear();
  }
  if (state.group == state.footer.groups.size()) { return {Error::END, {}}; }
  const auto &group = state.footer.groups[state.group];
  if (state.cursors.empty()) {
    for (const auto &chunk : group.chunks) {
      Cursor cursor; cursor.chunk = chunk; cursor.offset = chunk.start; state.cursors.push_back(std::move(cursor));
    }
  }
  row.assign(state.cursors.size(), Cell{{}, true});
  for (size_t i = 0; i < state.cursors.size(); ++i) {
    if (!state.selected[i]) { continue; }
    Cursor &cursor = state.cursors[i];
    if (cursor.row == cursor.page.size()) {
      std::vector<Cell>().swap(cursor.page);
      Status s = page(state.file, cursor, state.footer.columns[i], state.limit);
      if (!s) { return s.code == Error::END ? invalid("column ended before its row group") : s; }
    }
    row[i] = cursor.page[cursor.row++];
  }
  ++state.row;
  return {};
}
} // namespace oceanbase::sql::file_sql
