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
#pragma once

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace oceanbase::sql::file_sql
{
enum class Type { NULL_VALUE, BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, DATETIME };
enum class Format { CSV, JSONL, PARQUET, GLOB };
enum class Error { OK, END, INVALID, IO, DENIED, CHANGED, LIMIT, UNSUPPORTED };

struct Status
{
  Error code = Error::OK;
  std::string message;
  explicit operator bool() const { return code == Error::OK; }
};

struct Options
{
  Format format = Format::CSV;
  bool header = true;
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  std::string null_text;
  int64_t sample_size = -1;
  uint64_t max_record_bytes = 16 * 1024 * 1024;
  uint64_t max_columns = 4096;
};

struct Cell
{
  std::string text;
  bool is_null = false;
  bool quoted = false;
};

struct Column
{
  std::string name;
  Type type = Type::NULL_VALUE;
  bool nullable = false;
};

// This identity is carried in the SQL plan and checked on open and every read.
struct Identity
{
  uint64_t size = 0;
  int64_t modified = 0;
  uint64_t device = 0;
  uint64_t inode = 0;
  bool operator==(const Identity &) const = default;
};

struct Schema
{
  std::string path;
  Options options;
  Identity identity;
  std::vector<Column> columns;
};

Type infer_type(const Cell &cell);
Type widen(Type lhs, Type rhs);
bool valid_utf8(const std::string &text);
Status authorize_path(const std::string &path, const std::string &secure_root,
    std::string &canonical_path, bool output = false);
Status discover(const std::string &pattern, const std::string &secure_root,
    std::vector<std::string> &files);
Status infer_schema(const std::string &path, const Options &options,
    const std::string &secure_root, Schema &schema);
Status encode_schema(const Schema &schema, std::string &encoded);
Status decode_schema(const std::string &encoded, Schema &schema);
bool is_file_plan(const std::string &encoded);

class ParquetReader;
class Reader
{
public:
  Reader();
  ~Reader();
  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;
  Status open(const Schema &schema, const std::string &secure_root,
      const std::vector<size_t> *projection = nullptr);
  Status next(std::vector<Cell> &row);
  Status rescan();
  void close();
  uint64_t record_number() const { return record_number_; }
private:
  friend Status infer_schema(const std::string &, const Options &, const std::string &, Schema &);
  Status next_raw(std::vector<Cell> &row, std::vector<std::string> *keys = nullptr);
  Status csv_record(std::vector<Cell> &row);
  Status json_record(std::vector<Cell> &row, std::vector<std::string> *keys);
  Status check_identity();
  Schema schema_;
  FILE *input_ = nullptr;
  std::string secure_root_;
  std::unique_ptr<ParquetReader> parquet_;
  uint64_t record_number_ = 0;
  std::vector<std::string> discovered_;
  size_t discovered_index_ = 0;
  bool projected_ = false;
  std::vector<size_t> projection_;
};

// Writes to a private sibling file. Publishing must be explicit and exclusive;
// destruction removes unfinished output, including a failed SQL query.
class CsvWriter
{
public:
  ~CsvWriter();
  Status open(const std::string &path, const std::string &secure_root, const Options &options);
  Status write(const std::vector<Cell> &row);
  Status finish();
private:
  std::string target_;
  std::string temporary_;
  std::string secure_root_;
  intptr_t directory_descriptor_ = -1;
  FILE *output_ = nullptr;
  Options options_;
};
} // namespace oceanbase::sql::file_sql
