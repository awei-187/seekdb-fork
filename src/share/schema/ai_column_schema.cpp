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
#include "ai_column_schema.h"
#include "common/json_type/ob_json_tree.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_common.h"

namespace oceanbase::share::schema
{
using namespace common;
thread_local bool AIColumnEnqueueScope::enabled_ = false;
int AIColumnDefinition::encode(ObIAllocator &allocator, ObString &encoded) const
{
  int ret = OB_SUCCESS;
  ObJsonObject object(&allocator);
  auto string = [&](const char *name, const ObString &value) {
    auto *node = OB_NEWx(ObJsonString, &allocator, value);
    return node ? object.add(name, node) : OB_ALLOCATE_MEMORY_FAILED;
  };
  auto integer = [&](const char *name, int64_t value) {
    auto *node = OB_NEWx(ObJsonInt, &allocator, value);
    return node ? object.add(name, node) : OB_ALLOCATE_MEMORY_FAILED;
  };
  if (OB_FAIL(integer("version", 1))) {
  } else if (OB_FAIL(string("expression", expression_))) {
  } else if (OB_FAIL(string("normalized_expr", normalized_expr_))) {
  } else if (OB_FAIL(string("model", model_name_))) {
  } else if (OB_FAIL(string("prompt", prompt_expr_))) {
  } else if (OB_FAIL(string("hint", result_hint_))) {
  } else if (OB_FAIL(integer("embedding", embedding_))) {
  } else if (OB_FAIL(integer("dimension", dimension_))) {
  } else if (OB_FAIL(integer("max_retries", max_retries_))) {
  } else if (OB_FAIL(integer("state_column_id", state_column_id_))) {
  } else if (OB_FAIL(integer("request_column_id", request_column_id_))) {
  } else if (OB_FAIL(integer("result_spec_column_id", result_spec_column_id_))) {
  }
  ObJsonArray names(&allocator), ids(&allocator);
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_names_.count(); ++i) {
    auto *name = OB_NEWx(ObJsonString, &allocator, dependency_names_.at(i));
    if (!name) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { ret = names.append(name); }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_ids_.count(); ++i) {
    auto *id = OB_NEWx(ObJsonInt, &allocator, dependency_ids_.at(i));
    if (!id) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { ret = ids.append(id); }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(object.add("dependency_names", &names))) {
    } else if (OB_FAIL(object.add("dependency_ids", &ids))) {
    } else {
      ObJsonBuffer buffer(&allocator);
      if (OB_FAIL(object.print(buffer, 0))) {
      } else { encoded = buffer.string(); }
    }
  }
  return ret;
}

int AIColumnDefinition::decode(ObIAllocator &allocator, const ObString &encoded)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *parsed = nullptr;
  dependency_names_.reuse();
  dependency_ids_.reuse();
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, encoded, ObJsonInType::JSON_TREE,
      ObJsonInType::JSON_TREE, parsed))) {
  } else if (!parsed || parsed->json_type() != ObJsonNodeType::J_OBJECT) { ret = OB_INVALID_ARGUMENT; }
  else {
    auto *object = static_cast<ObJsonObject *>(parsed);
    auto string = [&](const char *name, ObString &value) {
      ObJsonNode *node = object->get_value(name);
      if (!node || node->json_type() != ObJsonNodeType::J_STRING) { return OB_INVALID_ARGUMENT; }
      value = static_cast<ObJsonString *>(node)->get_str();
      return OB_SUCCESS;
    };
    auto integer = [&](const char *name, int64_t &value) {
      ObJsonNode *node = object->get_value(name);
      if (!node || node->json_type() != ObJsonNodeType::J_INT) { return OB_INVALID_ARGUMENT; }
      value = node->get_int();
      return OB_SUCCESS;
    };
    int64_t version = 0, embedding = 0, state = 0, request = 0, spec = 0;
    if (OB_FAIL(integer("version", version))) {
    } else if (version != 1) { ret = OB_NOT_SUPPORTED; }
    else if (OB_FAIL(string("expression", expression_))) {
    } else if (OB_FAIL(string("normalized_expr", normalized_expr_))) {
    } else if (OB_FAIL(string("model", model_name_))) {
    } else if (OB_FAIL(string("prompt", prompt_expr_))) {
    } else if (OB_FAIL(string("hint", result_hint_))) {
    } else if (OB_FAIL(integer("embedding", embedding))) {
    } else if (OB_FAIL(integer("dimension", dimension_))) {
    } else if (OB_FAIL(integer("max_retries", max_retries_))) {
    } else if (OB_FAIL(integer("state_column_id", state))) {
    } else if (OB_FAIL(integer("request_column_id", request))) {
    } else if (OB_FAIL(integer("result_spec_column_id", spec))) {
    } else if (state < 0 || request < 0 || spec < 0 || embedding < 0 || embedding > 1
        || dimension_ < 0 || max_retries_ < 0 || max_retries_ > 1000 || model_name_.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      embedding_ = embedding;
      state_column_id_ = state;
      request_column_id_ = request;
      result_spec_column_id_ = spec;
      ObJsonNode *names = object->get_value("dependency_names");
      ObJsonNode *ids = object->get_value("dependency_ids");
      if (!names || !ids || names->json_type() != ObJsonNodeType::J_ARRAY || ids->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        auto *name_array = static_cast<ObJsonArray *>(names);
        auto *id_array = static_cast<ObJsonArray *>(ids);
        for (int64_t i = 0; OB_SUCC(ret) && i < names->element_count(); ++i) {
          ObJsonNode *value = name_array->get_value(i);
          if (!value || value->json_type() != ObJsonNodeType::J_STRING) { ret = OB_INVALID_ARGUMENT; }
          else { ret = dependency_names_.push_back(static_cast<ObJsonString *>(value)->get_str()); }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < ids->element_count(); ++i) {
          ObJsonNode *value = id_array->get_value(i);
          if (!value || value->json_type() != ObJsonNodeType::J_INT || value->get_int() < 0) { ret = OB_INVALID_ARGUMENT; }
          else { ret = dependency_ids_.push_back(value->get_int()); }
        }
      }
    }
  }
  return ret;
}
} // namespace oceanbase::share::schema
