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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/schema/ai_column_schema.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_ai_service.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase::sql
{
using namespace common;
using namespace share::schema;
namespace
{
int ai_dependencies(const ParseNode *node, AIColumnDefinition &definition, int depth = 0)
{
  int ret = OB_SUCCESS;
  if (!node) {
  } else if (depth > 128) {
    ret = OB_SIZE_OVERFLOW;
  } else if (node->type_ == T_COLUMN_REF) {
    if (node->num_child_ != 3 || !node->children_[2]
        || node->children_[0] || node->children_[1]) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ObString name(node->children_[2]->str_len_, node->children_[2]->str_value_);
      bool found = false;
      for (int64_t i = 0; i < definition.dependency_names_.count(); ++i) {
        found |= definition.dependency_names_.at(i).case_compare(name) == 0;
      }
      if (!found) { ret = definition.dependency_names_.push_back(name); }
    }
  } else if (node->type_ == T_SELECT || node->type_ == T_FUN_UDF || node->type_ == T_QUESTIONMARK) {
    ret = OB_NOT_SUPPORTED;
  } else {
    if (node->type_ == T_FUN_SYS && node->num_child_ > 0 && node->children_[0]) {
      ObString name(node->children_[0]->str_len_, node->children_[0]->str_value_);
      if (!name.case_compare("AI_COMPLETE") || !name.case_compare("AI_EMBED")
          || !name.case_compare("AI_RERANK")) {
        ret = OB_NOT_SUPPORTED;
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ret = ai_dependencies(node->children_[i], definition, depth + 1);
    }
  }
  return ret;
}
// Hex literals avoid both NO_BACKSLASH_ESCAPES and quote injection.
int ai_sql_literal(ObSqlString &sql, const ObString &value)
{
  int ret = sql.append("CONVERT(X'");
  static constexpr char hex[] = "0123456789ABCDEF";
  for (int32_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const unsigned char c = value.ptr()[i];
    char pair[] = {hex[c >> 4], hex[c & 15]};
    ret = sql.append(pair, 2);
  }
  if (OB_SUCC(ret)) { ret = sql.append("' USING utf8mb4)"); }
  return ret;
}
} // namespace

int ObDDLResolver::resolve_ai_column_definition(ObColumnSchemaV2 &column, const ParseNode &node)
{
  int ret = OB_SUCCESS;
  AIColumnDefinition definition;
  const ParseNode *call = node.children_[4];
  const ParseNode *options = node.children_[5];
  if (!call || call->type_ != T_FUN_SYS || call->num_child_ != 2
      || !call->children_[0] || !call->children_[1]) {
    ret = OB_NOT_SUPPORTED;
  } else {
    const ParseNode *args = call->children_[1];
    ObString name(call->children_[0]->str_len_, call->children_[0]->str_value_);
    definition.embedding_ = !name.case_compare("AI_EMBED");
    if ((!definition.embedding_ && name.case_compare("AI_COMPLETE"))
        || (args->num_child_ != 2 && args->num_child_ != 3)
        || !args->children_[0] || args->children_[0]->type_ != T_VARCHAR
        || !args->children_[1]) {
      ret = OB_NOT_SUPPORTED;
    } else {
      definition.model_name_.assign_ptr(args->children_[0]->str_value_, args->children_[0]->str_len_);
      definition.prompt_expr_.assign_ptr(args->children_[1]->str_value_, args->children_[1]->str_len_);
      definition.expression_.assign_ptr(call->str_value_, call->str_len_);
      if (OB_FAIL(ai_dependencies(args->children_[1], definition))) {
      } else if (definition.embedding_) {
        if (!column.is_collection() || column.get_extended_type_info().count() != 1) {
          ret = OB_NOT_SUPPORTED;
        } else {
          ObString type = column.get_extended_type_info().at(0);
          int64_t dimension = 0;
          bool seen = false;
          for (int32_t i = 0; i < type.length(); ++i) {
            if (type.ptr()[i] >= '0' && type.ptr()[i] <= '9') {
              seen = true;
              if (dimension > 1000000) { ret = OB_INVALID_ARGUMENT; break; }
              dimension = dimension * 10 + type.ptr()[i] - '0';
            }
          }
          definition.dimension_ = dimension;
          if (!seen || dimension <= 0 || type.length() < 6 || strncasecmp(type.ptr(), "VECTOR", 6)) {
            ret = OB_NOT_SUPPORTED;
          } else if (args->num_child_ == 3 && (!args->children_[2]
              || args->children_[2]->type_ != T_INT || args->children_[2]->value_ != dimension)) {
            ret = OB_ERR_INVALID_VECTOR_DIM;
            LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, dimension,
                args->children_[2] ? args->children_[2]->value_ : 0);
          }
        }
      } else if (column.is_json()) {
        definition.result_hint_ = ObString::make_string("Return exactly one valid JSON value");
      } else if (ob_is_integer_type(column.get_data_type())) {
        definition.result_hint_ = ObString::make_string("Return exactly one base-10 integer");
      } else if (ob_is_numeric_type(column.get_data_type())) {
        definition.result_hint_ = ObString::make_string("Return exactly one finite number");
      } else if (!column.is_string_type() && !ob_is_text_tc(column.get_data_type())) {
        ret = OB_NOT_SUPPORTED;
      }
      // Completion's optional configuration remains part of the durable SQL
      // expression. Invalid configurations are rejected rather than discarded.
      if (OB_SUCC(ret) && !definition.embedding_ && args->num_child_ != 2) {
        ret = OB_NOT_SUPPORTED;
      }
    }
  }
  bool seen_sync = false, seen_retries = false;
  for (int64_t i = 0; OB_SUCC(ret) && options && i < options->num_child_; ++i) {
    const ParseNode *option = options->children_[i];
    if (!option || option->type_ != T_OP_EQ || option->num_child_ != 2
        || !option->children_[0] || option->children_[0]->type_ != T_COLUMN_REF
        || !option->children_[1]) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const ParseNode *key = option->children_[0]->children_[2];
      const ParseNode *value = option->children_[1];
      ObString name(key->str_len_, key->str_value_);
      if (!name.case_compare("sync_mode") && !seen_sync) {
        seen_sync = true;
        if (value->type_ != T_VARCHAR
            || ObString(value->str_len_, value->str_value_).case_compare("async")) {
          ret = OB_NOT_SUPPORTED;
        }
      } else if (!name.case_compare("max_retries") && !seen_retries) {
        seen_retries = true;
        if (value->type_ != T_INT || value->value_ < 0 || value->value_ > 1000) {
          ret = OB_INVALID_ARGUMENT;
        } else { definition.max_retries_ = value->value_; }
      } else { ret = OB_INVALID_ARGUMENT; }
    }
  }
  if (OB_SUCC(ret)) {
    omt::ObAiServiceGuard guard;
    const share::ObAiModelEndpointInfo *endpoint = nullptr;
    ObString encoded;
    ObSqlString normalized;
    if (OB_FAIL(guard.get_ai_endpoint_by_ai_model_name(definition.model_name_, endpoint))) {
    } else if (OB_FAIL(normalized.append(definition.expression_))) {
    } else if (!definition.result_hint_.empty()
        && OB_FAIL(normalized.append_fmt(" /* %.*s */", definition.result_hint_.length(), definition.result_hint_.ptr()))) {
    } else {
      definition.normalized_expr_ = normalized.string();
      if (OB_FAIL(definition.encode(*allocator_, encoded))) {
      } else if (OB_FAIL(column.set_ai_definition(encoded))) {
      } else {
        ObObj null;
        null.set_null();
        column.set_nullable(true);
        if (OB_FAIL(column.set_cur_default_value(null, false))) {
        } else { ret = column.set_orig_default_value(null); }
      }
    }
  }
  if (ret == OB_NOT_SUPPORTED) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI COLUMN expression, type, or synchronous mode");
  }
  return ret;
}

int ObDDLResolver::finalize_ai_columns(ObTableSchema &table, uint64_t &max_column_id)
{
  int ret = OB_SUCCESS;
  const int64_t count = table.get_column_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObColumnSchemaV2 *column = table.get_column_schema_by_idx(i);
    if (!column || !column->is_ai_column()) { continue; }
    if (column->is_rowkey_column()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI column as a primary key");
      break;
    }
    AIColumnDefinition definition;
    if (OB_FAIL(definition.decode(*allocator_, column->get_ai_definition()))) {
    } else if (definition.state_column_id_ != 0) { continue; }
    for (int64_t d = 0; OB_SUCC(ret) && d < definition.dependency_names_.count(); ++d) {
      const ObColumnSchemaV2 *dependency = table.get_column_schema(definition.dependency_names_.at(d));
      if (!dependency) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, definition.dependency_names_.at(d).length(),
            definition.dependency_names_.at(d).ptr(), "AI COLUMN expression");
      } else if (dependency->is_ai_column() || dependency->is_hidden()) {
        ret = OB_NOT_SUPPORTED;
      } else { ret = definition.dependency_ids_.push_back(dependency->get_column_id()); }
    }
    ObColumnSchemaV2 state, request, spec;
    ObColumnSchemaV2 *hidden[] = {&state, &request, &spec};
    for (int n = 0; OB_SUCC(ret) && n < 3; ++n) {
      char name[128];
      snprintf(name, sizeof(name), "__ai_%s_%lu", n == 0 ? "state" : n == 1 ? "request" : "spec",
          static_cast<unsigned long>(column->get_column_id()));
      hidden[n]->set_table_id(table.get_table_id());
      const ObColumnSchemaV2 *existing = table.get_column_schema(ObString::make_string(name));
      if (existing && !existing->is_hidden()) { ret = OB_ERR_COLUMN_DUPLICATE; break; }
      hidden[n]->set_column_id(existing ? existing->get_column_id() : ++max_column_id);
      hidden[n]->set_is_hidden(true);
      hidden[n]->set_nullable(n != 0);
      hidden[n]->set_charset_type(CHARSET_UTF8MB4);
      hidden[n]->set_collation_type(CS_TYPE_UTF8MB4_BIN);
      ret = hidden[n]->set_column_name(name);
    }
    if (OB_SUCC(ret)) {
      state.set_data_type(ObTinyIntType);
      state.set_charset_type(CHARSET_BINARY);
      state.set_collation_type(CS_TYPE_BINARY);
      ObObj zero;
      zero.set_int(0);
      if (OB_FAIL(state.set_cur_default_value(zero, false))) {
      } else if (OB_FAIL(state.set_orig_default_value(zero))) {
      }
      request.set_data_type(ObVarcharType);
      request.set_data_length(64);
      spec.set_data_type(ObLongTextType);
      spec.set_data_length(ObAccuracy::MAX_ACCURACY[ObLongTextType].get_length());
      definition.state_column_id_ = state.get_column_id();
      definition.request_column_id_ = request.get_column_id();
      definition.result_spec_column_id_ = spec.get_column_id();
      ObSqlString expression;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(expression.append("JSON_OBJECT('model',"))) {
      } else if (OB_FAIL(ai_sql_literal(expression, definition.model_name_))) {
      } else if (OB_FAIL(expression.append_fmt(",'prompt',(%.*s),'hint',",
          definition.prompt_expr_.length(), definition.prompt_expr_.ptr()))) {
      } else if (OB_FAIL(ai_sql_literal(expression, definition.result_hint_))) {
      } else if (OB_FAIL(expression.append(",'definition',"))) {
      } else if (OB_FAIL(ai_sql_literal(expression, definition.expression_))) {
      } else if (OB_FAIL(expression.append_fmt(",'embedding',%d,'dimension',%ld)",
          definition.embedding_ ? 1 : 0, static_cast<long>(definition.dimension_)))) {
      } else {
        ObObj default_expr;
        default_expr.set_varchar(expression.string());
        default_expr.set_collation_type(ObCharset::get_system_collation());
        spec.add_column_flag(STORED_GENERATED_COLUMN_FLAG);
        ObSEArray<ObString, 4> exprs;
        ObSEArray<ObColumnSchemaV2 *, 4> resolved;
        if (OB_FAIL(spec.set_cur_default_value(default_expr, false))) {
        } else if (OB_FAIL(check_default_value(spec.get_cur_default_value(), session_info_->get_tz_info_wrap(),
            *allocator_, table, resolved, spec, exprs, session_info_->get_sql_mode(), session_info_,
            schema_checker_, params_.srs_provider_, params_.lob_read_service_))) {
        } else if (OB_FAIL(spec.set_orig_default_value(spec.get_cur_default_value()))) {
        } else {
          ObString encoded;
          if (OB_FAIL(definition.encode(*allocator_, encoded))) {
          } else if (OB_FAIL(column->set_ai_definition(encoded))) {
          } else {
            for (auto *added : hidden) {
              if (OB_FAIL(ret)) { break; }
              ObColumnSchemaV2 *existing = table.get_column_schema(added->get_column_id());
              ret = existing ? existing->assign(*added) : table.add_column(*added);
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) { table.set_max_used_column_id(max_column_id); }
  return ret;
}
} // namespace oceanbase::sql
