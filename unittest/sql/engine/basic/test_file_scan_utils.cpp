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
#include "sql/engine/basic/file_scan_utils.h"
#include "sql/engine/basic/parquet_file_reader.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#ifdef FILE_SQL_STANDALONE_TEST
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#define TEST(suite, name) void suite##_##name()
#define ASSERT_TRUE(value) assert(static_cast<bool>(value))
#define ASSERT_FALSE(value) assert(!static_cast<bool>(value))
#define ASSERT_EQ(lhs, rhs) assert((lhs) == (rhs))
#else
#include <gtest/gtest.h>
#endif

using namespace oceanbase::sql::file_sql;
namespace fs = std::filesystem;
namespace
{
struct Fixture
{
  fs::path path = fs::temp_directory_path() / ("file-scan-regression-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  Fixture() { fs::create_directory(path); }
  ~Fixture() { std::error_code ec; fs::remove_all(path, ec); }
  std::string write(const std::string &name, const std::string &text)
  {
    std::ofstream output(path / name, std::ios::binary);
    output << text;
    output.close();
    return (path / name).generic_string();
  }
  std::string root() const { return path.generic_string(); }
};
}

TEST(FileScan, inference_boundaries)
{
  ASSERT_EQ(Type::BIGINT, infer_type({"9223372036854775807"}));
  ASSERT_EQ(Type::BIGINT, infer_type({"-9223372036854775808"}));
  ASSERT_EQ(Type::DOUBLE, infer_type({"9223372036854775808"}));
  ASSERT_EQ(Type::VARCHAR, infer_type({"NaN"}));
  ASSERT_EQ(Type::VARCHAR, infer_type({"1e9999"}));
  ASSERT_EQ(Type::DATE, infer_type({"2024-02-29"}));
  ASSERT_EQ(Type::VARCHAR, infer_type({"2023-02-29"}));
  ASSERT_EQ(Type::VARCHAR, infer_type({"2024-01-01 24:00:00"}));
  ASSERT_EQ(Type::DATETIME, infer_type({"2024-01-01 23:59:59.123456"}));
  ASSERT_EQ(Type::DOUBLE, widen(Type::BIGINT, Type::DOUBLE));
  ASSERT_EQ(Type::VARCHAR, widen(Type::BOOLEAN, Type::BIGINT));
  ASSERT_EQ(Type::DATE, widen(Type::NULL_VALUE, Type::DATE));
  ASSERT_FALSE(valid_utf8(std::string("\xc0\x80", 2)));
  ASSERT_FALSE(valid_utf8(std::string("\xed\xa0\x80", 3)));
}

TEST(FileScan, csv_multiline_rescan_and_nulls)
{
  Fixture f;
  std::string path = f.write("input.csv", "\xef\xbb\xbfid,name,name,empty,quoted\r\n1,\"hello\r\nworld\",\"a\"\"b\",,\"\"\r\n2,last,x,,\"\"\r\n");
  Options options;
  Schema schema;
  ASSERT_TRUE(infer_schema(path, options, f.root(), schema));
  ASSERT_EQ(size_t(5), schema.columns.size());
  ASSERT_EQ(std::string("name__2"), schema.columns[2].name);
  ASSERT_EQ(Type::BIGINT, schema.columns[0].type);
  ASSERT_EQ(Type::NULL_VALUE, schema.columns[3].type);
  Reader reader;
  ASSERT_TRUE(reader.open(schema, f.root()));
  std::vector<Cell> row;
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(std::string("hello\nworld"), row[1].text);
  ASSERT_EQ(std::string("a\"b"), row[2].text);
  ASSERT_TRUE(row[3].is_null);
  ASSERT_FALSE(row[4].is_null);
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(std::string("2"), row[0].text);
  ASSERT_EQ(Error::END, reader.next(row).code);
  ASSERT_TRUE(reader.rescan());
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(std::string("1"), row[0].text);
  Schema case_schema;
  ASSERT_TRUE(infer_schema(f.write("case.csv", "id,ID,id__2\n1,2,3\n"), options, f.root(), case_schema));
  ASSERT_EQ(std::string("ID__2"), case_schema.columns[1].name);
  ASSERT_EQ(std::string("id__2__2"), case_schema.columns[2].name);
}

TEST(FileScan, json_union_and_validation)
{
  Fixture f;
  Options options; options.format = Format::JSONL;
  Schema schema;
  std::string path = f.write("input.jsonl", "{\"id\":1,\"name\":\"a\\uD83D\\uDE00\",\"n\":null}\n{\"id\":2.5,\"other\":true}\n");
  ASSERT_TRUE(infer_schema(path, options, f.root(), schema));
  ASSERT_EQ(size_t(4), schema.columns.size());
  ASSERT_EQ(Type::DOUBLE, schema.columns[0].type);
  Reader reader;
  ASSERT_TRUE(reader.open(schema, f.root()));
  std::vector<Cell> row;
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(std::string("a\xf0\x9f\x98\x80"), row[1].text);
  ASSERT_TRUE(row[3].is_null);
  ASSERT_TRUE(reader.next(row));
  ASSERT_TRUE(row[1].is_null);
  ASSERT_EQ(std::string("true"), row[3].text);
  for (const std::string &bad : {"{\"a\":{}}", "{\"a\":[]}", "{\"a\":1,\"a\":2}", "{\"a\":01}", "{\"a\":\"\\uD800\"}"}) {
    std::string p = f.write("bad.jsonl", bad);
    ASSERT_FALSE(infer_schema(p, options, f.root(), schema));
  }
  ASSERT_FALSE(infer_schema(f.write("case.jsonl", "{\"id\":1}\n{\"ID\":2}\n"), options, f.root(), schema));
}

TEST(FileScan, path_change_and_limits)
{
  Fixture f;
  Schema schema;
  Options options;
  auto path = f.write("input.csv", "id\n1\n");
  ASSERT_TRUE(infer_schema(path, options, f.root(), schema));
  Reader reader;
  ASSERT_TRUE(reader.open(schema, f.root()));
  f.write("input.csv", "id\n12345\n");
  std::vector<Cell> row;
  ASSERT_EQ(Error::CHANGED, reader.next(row).code);
  options.sample_size = 1;
  ASSERT_EQ(Error::UNSUPPORTED, infer_schema(path, options, f.root(), schema).code);
  options.sample_size = -1;
  options.max_record_bytes = 3;
  ASSERT_EQ(Error::LIMIT, infer_schema(path, options, f.root(), schema).code);
  std::string canonical;
  ASSERT_EQ(Error::DENIED, authorize_path(path, "", canonical).code);
  Fixture other;
  auto outside = other.write("outside.csv", "id\n9\n");
  ASSERT_EQ(Error::DENIED, authorize_path(outside, f.root(), canonical).code);
  std::error_code ec;
  fs::create_symlink(outside, f.path / "escape.csv", ec);
  if (!ec) { ASSERT_EQ(Error::DENIED, authorize_path((f.path / "escape.csv").generic_string(), f.root(), canonical).code); }
}

TEST(FileScan, csv_export_round_trip_and_exclusive_publish)
{
  Fixture f;
  Options options; options.null_text = "NULL";
  std::string target = (f.path / "output.csv").generic_string();
  CsvWriter writer;
  ASSERT_TRUE(writer.open(target, f.root(), options));
  ASSERT_TRUE(writer.write({{"id"}, {"value"}}));
  ASSERT_TRUE(writer.write({{"1"}, {"x,\"y\"\nz"}}));
  ASSERT_TRUE(writer.write({{"2"}, {"NULL"}}));
  ASSERT_TRUE(writer.write({{"3"}, {{}, true}}));
  Status finished = writer.finish();
  if (!finished) { std::cerr << finished.message << '\n'; }
  ASSERT_TRUE(finished);
  Schema schema;
  ASSERT_TRUE(infer_schema(target, options, f.root(), schema));
  Reader reader;
  ASSERT_TRUE(reader.open(schema, f.root()));
  std::vector<Cell> row;
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(std::string("x,\"y\"\nz"), row[1].text);
  ASSERT_TRUE(reader.next(row));
  ASSERT_FALSE(row[1].is_null);
  ASSERT_TRUE(reader.next(row));
  ASSERT_TRUE(row[1].is_null);
  CsvWriter collision;
  ASSERT_FALSE(collision.open(target, f.root(), options));
  std::string failure = (f.path / "failure.csv").generic_string();
  { CsvWriter abandoned; ASSERT_TRUE(abandoned.open(failure, f.root(), options)); ASSERT_TRUE(abandoned.write({{"x"}})); }
  ASSERT_FALSE(fs::exists(failure));
  for (const auto &entry : fs::directory_iterator(f.path)) { ASSERT_FALSE(entry.path().filename().string().find(".tmp.") != std::string::npos); }
}

TEST(FileScan, snappy_overlap_and_corruption)
{
  std::string output;
  ASSERT_TRUE(decode_snappy(std::string("\x09\x08" "abc" "\x16\x03\x00", 8), 9, 100, output));
  ASSERT_EQ(std::string("abcabcabc"), output);
  ASSERT_FALSE(decode_snappy(std::string("\x04\x01\x00", 3), 4, 100, output));
  ASSERT_FALSE(decode_snappy(std::string("\x09\x08" "abc", 5), 9, 100, output));
  ASSERT_FALSE(decode_snappy(std::string("\x09", 1), 9, 8, output));
}

TEST(FileScan, schema_plan_round_trip_and_glob)
{
  Fixture f;
  auto first = f.write("a.csv", "id,value,empty\n1,one,\n2,two,\n");
  f.write("b.csv", "id\n3\n");
  f.write("other.txt", "ignore");
  Schema original, decoded;
  Options options;
  ASSERT_TRUE(infer_schema(first, options, f.root(), original));
  ASSERT_FALSE(original.columns[0].nullable);
  ASSERT_TRUE(original.columns[2].nullable);
  ASSERT_EQ(Type::NULL_VALUE, original.columns[2].type);
  std::string plan;
  ASSERT_TRUE(encode_schema(original, plan));
  ASSERT_TRUE(is_file_plan(plan));
  ASSERT_TRUE(decode_schema(plan, decoded));
  ASSERT_EQ(original.path, decoded.path);
  ASSERT_EQ(original.identity, decoded.identity);
  ASSERT_EQ(original.columns.size(), decoded.columns.size());
  ASSERT_EQ(original.columns[2].nullable, decoded.columns[2].nullable);
  for (size_t size = 0; size < plan.size(); ++size) {
    Schema invalid;
    ASSERT_FALSE(decode_schema(plan.substr(0, size), invalid));
  }
  ASSERT_FALSE(decode_schema(plan + "x", decoded));
  std::vector<std::string> paths;
  ASSERT_TRUE(discover((f.path / "*.csv").generic_string(), f.root(), paths));
  ASSERT_EQ(size_t(2), paths.size());
  ASSERT_EQ(first, paths[0]);
  options.format = Format::GLOB;
  ASSERT_TRUE(infer_schema((f.path / "*.csv").generic_string(), options, f.root(), original));
  Reader reader;
  ASSERT_TRUE(reader.open(original, f.root()));
  std::vector<Cell> row;
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(first, row[0].text);
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(paths[1], row[0].text);
  ASSERT_EQ(Error::END, reader.next(row).code);
  ASSERT_TRUE(reader.rescan());
  ASSERT_TRUE(reader.next(row));
  ASSERT_EQ(first, row[0].text);
}

#ifdef FILE_SQL_STANDALONE_TEST
int main(int argc, char **argv)
{
#ifdef _WIN32
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
  FileScan_inference_boundaries();
  FileScan_csv_multiline_rescan_and_nulls();
  FileScan_json_union_and_validation();
  FileScan_path_change_and_limits();
  FileScan_csv_export_round_trip_and_exclusive_publish();
  FileScan_snappy_overlap_and_corruption();
  FileScan_schema_plan_round_trip_and_glob();
  std::cout << "7 file scan regression tests passed\n";
  if (argc == 2) {
    size_t cases = 0;
    for (const auto &entry : fs::directory_iterator(argv[1])) {
      if (entry.path().extension() != ".parquet") { continue; }
      Schema schema;
      Options options; options.format = Format::PARQUET;
      Status status = infer_schema(entry.path().generic_string(), options, argv[1], schema);
      if (!status) { std::cerr << entry.path() << ": " << status.message << '\n'; return 1; }
      Reader reader;
      status = reader.open(schema, argv[1]);
      if (!status) { std::cerr << status.message << '\n'; return 1; }
      size_t count = 0;
      std::vector<Cell> row;
      for (;;) {
        status = reader.next(row);
        if (status.code == Error::END) { break; }
        if (!status) { std::cerr << entry.path() << ": row " << count << ": " << status.message << '\n'; return 1; }
        bool official = entry.path().filename() == "official.parquet";
        ASSERT_EQ(std::to_string(count + (official ? 0 : 1)), row[0].text);
        if (!official) {
          const char *flags[] = {"true", "false", nullptr, "true", "false", "true", nullptr};
          const char *texts[] = {"a", "b", nullptr, "a", "你好", "x\ny", ""};
          ASSERT_EQ(flags[count] == nullptr, row[1].is_null);
          if (flags[count]) { ASSERT_EQ(std::string(flags[count]), row[1].text); }
          ASSERT_EQ(texts[count] == nullptr, row[3].is_null);
          if (texts[count]) { ASSERT_EQ(std::string(texts[count]), row[3].text); }
          if (count == 3) { ASSERT_EQ(std::string("1969-12-31 23:59:59.999999"), row[5].text); }
        }
        ++count;
      }
      ASSERT_EQ(size_t(7), count);
      for (const std::vector<size_t> &projection : {std::vector<size_t>{}, std::vector<size_t>{0}}) {
        Reader projected;
        ASSERT_TRUE(projected.open(schema, argv[1], &projection));
        size_t projected_count = 0;
        while ((status = projected.next(row)).code != Error::END) {
          ASSERT_TRUE(status);
          ASSERT_EQ(schema.columns.size(), row.size());
          for (size_t i = 0; i < row.size(); ++i) {
            if (projection.empty() || i != 0) { ASSERT_TRUE(row[i].is_null); }
          }
          if (!projection.empty()) {
            ASSERT_EQ(std::to_string(projected_count + (entry.path().filename() == "official.parquet" ? 0 : 1)), row[0].text);
          }
          ++projected_count;
        }
        ASSERT_EQ(size_t(7), projected_count);
        ASSERT_TRUE(projected.rescan());
        ASSERT_TRUE(projected.next(row));
      }
      ++cases;
    }
    ASSERT_EQ(size_t(9), cases);
    const fs::path expansion = fs::path(argv[1]) / "limits/dictionary-expansion.parquet";
    if (fs::exists(expansion)) {
      Schema schema;
      Options options; options.format = Format::PARQUET;
      ASSERT_TRUE(infer_schema(expansion.generic_string(), options, argv[1], schema));
      Reader reader;
      std::vector<Cell> row;
      ASSERT_TRUE(reader.open(schema, argv[1]));
      ASSERT_EQ(Error::LIMIT, reader.next(row).code);
      Reader projected;
      std::vector<size_t> projection{0};
      ASSERT_TRUE(projected.open(schema, argv[1], &projection));
      for (size_t i = 0; i < 32; ++i) {
        ASSERT_TRUE(projected.next(row));
        ASSERT_EQ(std::to_string(i), row[0].text);
        ASSERT_TRUE(row[1].is_null);
      }
      ASSERT_EQ(Error::END, projected.next(row).code);
    }
    std::cout << cases << " independent Parquet reference files passed\n";
  }
}
#endif
