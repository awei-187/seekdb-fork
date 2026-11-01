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
#include "file_scan_utils.h"

namespace oceanbase::sql::file_sql
{
// A bounded reader for flat Parquet columns: Thrift Compact metadata,
// PLAIN/dictionary pages, RLE definition levels, and uncompressed/Snappy blocks.
// Unsupported encodings fail explicitly before exposing a row.
class ParquetReader
{
public:
  ParquetReader();
  ~ParquetReader();
  static Status schema(FILE *file, std::vector<Column> &columns, uint64_t limit);
  Status open(FILE *file, const std::vector<Column> &columns, uint64_t limit,
      const std::vector<size_t> *projection = nullptr);
  Status next(std::vector<Cell> &row);
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
Status decode_snappy(const std::string &input, size_t expected, size_t limit, std::string &output);
} // namespace oceanbase::sql::file_sql
