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
#include "sql/resolver/ob_resolver.h"
#include "sql/parser/ob_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "share/schema/ai_column_schema.h"

namespace oceanbase::sql
{
using namespace common;
using namespace share::schema;
namespace
{
int retry_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int32_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name.ptr()[i] == '`' && OB_FAIL(sql.append("`"))) { break; }
    ret = sql.append(name.ptr() + i, 1);
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}
int retry_parse(ObResolverParams &params, const ObSqlString &sql, ParseNode *&node)
{
  int ret = OB_SUCCESS;
  ParseResult parsed;
  ObParser parser(*params.allocator_, params.session_info_->get_sql_mode());
  if (OB_FAIL(parser.parse(sql.string(), parsed))) {
  } else if (!parsed.result_tree_ || parsed.result_tree_->num_child_ != 1 || !parsed.result_tree_->children_[0]) {
    ret = OB_ERR_UNEXPECTED;
  } else { node = parsed.result_tree_->children_[0]; }
  return ret;
}
int retry_status(ObResolverParams &params, ParseNode *&node, const ObString &state, int depth = 0)
{
  int ret = OB_SUCCESS;
  if (!node) { return ret; }
  if (depth > 128) { return OB_SIZE_OVERFLOW; }
  if (node->type_ == T_COLUMN_REF && node->num_child_ == 3 && !node->children_[0] && !node->children_[1]
      && node->children_[2] && !node->children_[2]->is_input_quoted_
      && !ObString(node->children_[2]->str_len_, node->children_[2]->str_value_).case_compare("status")) {
    ObSqlString sql;
    ParseNode *select = nullptr;
    if (OB_FAIL(sql.append("SELECT CASE "))) {
    } else if (OB_FAIL(retry_identifier(sql, state))) {
    } else if (OB_FAIL(sql.append(" WHEN 0 THEN 'PENDING' WHEN 1 THEN 'READY' WHEN 2 THEN 'FAILED' END"))) {
    } else if (OB_FAIL(retry_parse(params, sql, select))) {
    } else {
      ParseNode *projects = select->children_[PARSE_SELECT_SELECT];
      if (!projects || projects->num_child_ != 1 || !projects->children_[0]
          || projects->children_[0]->num_child_ != 1) { ret = OB_ERR_UNEXPECTED; }
      else { node = projects->children_[0]->children_[0]; }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ret = retry_status(params, node->children_[i], state, depth + 1);
    }
  }
  return ret;
}
}
int ObResolver::resolve_ai_column_retry(IsPrepared prepared, const ParseNode &query, ObStmt *&stmt)
{
  int ret = OB_SUCCESS;
  const ParseNode *relation = query.children_[0], *target = query.children_[1];
  if (!relation || relation->type_ != T_RELATION_FACTOR || relation->num_child_ < 2
      || !relation->children_[1] || !target || !params_.query_ctx_) { return OB_ERR_UNEXPECTED; }
  ObString database = params_.session_info_->get_database_name();
  if (relation->children_[0]) {
    database.assign_ptr(relation->children_[0]->str_value_, relation->children_[0]->str_len_);
  }
  ObString name(relation->children_[1]->str_len_, relation->children_[1]->str_value_);
  ObString column_name(target->str_len_, target->str_value_);
  const ObTableSchema *table = nullptr;
  const ObColumnSchemaV2 *column = nullptr, *state = nullptr, *request = nullptr;
  AIColumnDefinition definition;
  if (OB_FAIL(params_.schema_checker_->get_table_schema(database, name, false, table))) {
  } else if (!table || !(column = table->get_column_schema(column_name)) || !column->is_ai_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
  } else if (OB_FAIL(definition.decode(*params_.allocator_, column->get_ai_definition()))) {
  } else if (!(state = table->get_column_schema(definition.state_column_id_))
      || !(request = table->get_column_schema(definition.request_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObSqlString sql;
    ParseNode *update = nullptr;
    if (OB_FAIL(sql.append("UPDATE "))) {
    } else if (OB_FAIL(retry_identifier(sql, database))) {
    } else if (OB_FAIL(sql.append("."))) {
    } else if (OB_FAIL(retry_identifier(sql, name))) {
    } else if (OB_FAIL(sql.append(" SET "))) {
    } else if (OB_FAIL(retry_identifier(sql, column->get_column_name_str()))) {
    } else if (OB_FAIL(sql.append("=NULL,"))) {
    } else if (OB_FAIL(retry_identifier(sql, state->get_column_name_str()))) {
    } else if (OB_FAIL(sql.append("=0,"))) {
    } else if (OB_FAIL(retry_identifier(sql, request->get_column_name_str()))) {
    } else if (OB_FAIL(sql.append("=UUID()"))) {
    } else if (OB_FAIL(retry_parse(params_, sql, update))) {
    } else {
      ParseNode *where = query.children_[2];
      if (where && OB_FAIL(retry_status(params_, where, state->get_column_name_str()))) {
      } else {
        update->children_[3] = where;
        params_.query_ctx_->ai_retry_table_id_ = table->get_table_id();
        params_.query_ctx_->ai_retry_column_id_ = column->get_column_id();
        ObResolver resolver(params_);
        ret = resolver.resolve(prepared, *update, stmt);
      }
    }
  }
  return ret;
}
} // namespace oceanbase::sql
