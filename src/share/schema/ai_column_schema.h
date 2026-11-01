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
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase::share::schema
{
// Only native DDL reconciliation enables enqueueing on an internal session.
// Result publication uses ordinary internal DML with this scope disabled.
class AIColumnEnqueueScope
{
public:
  AIColumnEnqueueScope() : previous_(enabled_) { enabled_ = true; }
  ~AIColumnEnqueueScope() { enabled_ = previous_; }
  static bool active() { return enabled_; }
private:
  bool previous_;
  static thread_local bool enabled_;
};
struct AIColumnDefinition
{
  common::ObString expression_;
  common::ObString normalized_expr_;
  common::ObString model_name_;
  common::ObString prompt_expr_;
  common::ObString result_hint_;
  common::ObSEArray<common::ObString, 8> dependency_names_;
  common::ObSEArray<uint64_t, 8> dependency_ids_;
  bool embedding_ = false;
  int64_t dimension_ = 0;
  int64_t max_retries_ = 2;
  uint64_t state_column_id_ = 0;
  uint64_t request_column_id_ = 0;
  uint64_t result_spec_column_id_ = 0;
  int encode(common::ObIAllocator &allocator, common::ObString &encoded) const;
  int decode(common::ObIAllocator &allocator, const common::ObString &encoded);
};
} // namespace oceanbase::share::schema
