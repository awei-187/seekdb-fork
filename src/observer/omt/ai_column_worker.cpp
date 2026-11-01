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

#define USING_LOG_PREFIX SERVER_OMT
#include "observer/omt/ob_ai_service.h"
#include "observer/ai_service/ob_ai_service_executor.h"
#include "share/schema/ai_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_server_struct.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include <charconv>
#include <cmath>
#include <limits>

namespace oceanbase::omt
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace
{
struct ColumnTask
{
  ObString request, predicate, spec, encoded_definition, owner, result;
  uint64_t table_id = 0, column_id = 0;
  int64_t version = 0, retries = 0, lease = 0, state = 0;
};
int worker_literal(ObSqlString &sql, const ObString &value)
{
  int ret = sql.append("CONVERT(X'");
  static constexpr char hex[] = "0123456789ABCDEF";
  for (int32_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const unsigned char c = value.ptr()[i];
    const char pair[] = {hex[c >> 4], hex[c & 15]};
    ret = sql.append(pair, 2);
  }
  if (OB_SUCC(ret)) { ret = sql.append("' USING utf8mb4)"); }
  return ret;
}
int worker_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int32_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name.ptr()[i] == '`' && OB_FAIL(sql.append("`"))) { break; }
    ret = sql.append(name.ptr() + i, 1);
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}
int worker_copy(sqlclient::ObMySQLResult &row, const char *name, ObIAllocator &allocator, ObString &value)
{
  ObString temporary;
  int ret = row.get_varchar(name, temporary);
  if (OB_SUCC(ret)) { ret = ob_write_string(allocator, temporary, value); }
  return ret;
}
int read_task(sqlclient::ObMySQLResult &row, ObIAllocator &allocator, ColumnTask &task)
{
  int ret = OB_SUCCESS;
  int64_t table_id = 0, column_id = 0;
  if (OB_FAIL(worker_copy(row, "request_id", allocator, task.request))) {
  } else if (OB_FAIL(worker_copy(row, "rowkey_predicate", allocator, task.predicate))) {
  } else if (OB_FAIL(worker_copy(row, "result_spec", allocator, task.spec))) {
  } else if (OB_FAIL(row.get_int("table_id", table_id))) {
  } else if (OB_FAIL(row.get_int("column_id", column_id))) {
  } else if (table_id <= 0 || column_id <= 0) { ret = OB_INVALID_DATA;
  } else if (OB_FAIL(row.get_int("definition_version", task.version))) {
  } else if (OB_FAIL(row.get_int("retry_count", task.retries))) {
  } else if (OB_FAIL(row.get_int("lease_version", task.lease))) {
  } else { ret = row.get_int("state", task.state); }
  if (OB_SUCC(ret)) { task.table_id = table_id; task.column_id = column_id; }
  return ret;
}
int request_condition(ObSqlString &sql, const ColumnTask &task, bool owner)
{
  int ret = sql.append("request_id=");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(worker_literal(sql, task.request))) {
  } else if (OB_FAIL(sql.append_fmt(" AND column_id=%lu", task.column_id))) {
  } else if (owner) {
    if (OB_FAIL(sql.append_fmt(" AND state=1 AND lease_version=%ld AND lease_owner=", task.lease))) {
    } else { ret = worker_literal(sql, task.owner); }
  }
  return ret;
}
int claim_task(ObIAllocator &allocator, ColumnTask &task)
{
  int ret = OB_SUCCESS;
  alignas(16) ObMySQLProxy::MySQLResult response;
  const char *query = "SELECT * FROM oceanbase.__all_ai_column_task "
      "WHERE ((state IN (0,3) AND eligible_ts<=NOW(6)) OR (state=1 AND lease_expire_ts<=NOW(6))) "
      "ORDER BY eligible_ts,request_id,column_id LIMIT 1";
  if (OB_FAIL(GCTX.sql_proxy_->read(response, query))) {
  } else if (!response.get_result()) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(response.get_result()->next())) {
  } else { ret = read_task(*response.get_result(), allocator, task); }
  response.reset();
  if (OB_SUCC(ret)) {
    char owner[37] = {};
    ObSqlString claim;
    int64_t affected = 0;
    if (OB_FAIL(sql::ObExprUuid::gen_server_uuid(owner, 36))) {
    } else if (OB_FAIL(ob_write_string(allocator, ObString(36, owner), task.owner))) {
    } else if (OB_FAIL(claim.append("UPDATE oceanbase.__all_ai_column_task SET state=1,lease_owner="))) {
    } else if (OB_FAIL(worker_literal(claim, task.owner))) {
    } else if (OB_FAIL(claim.append(",lease_version=lease_version+1,lease_expire_ts=DATE_ADD(NOW(6),INTERVAL 30 SECOND) WHERE "))) {
    } else if (OB_FAIL(request_condition(claim, task, false))) {
    } else if (OB_FAIL(claim.append_fmt(" AND lease_version=%ld AND ((state IN (0,3) AND eligible_ts<=NOW(6)) OR "
        "(state=1 AND lease_expire_ts<=NOW(6)))", task.lease))) {
    } else if (OB_FAIL(GCTX.sql_proxy_->write(claim.ptr(), affected))) {
    } else if (affected != 1) { ret = OB_EAGAIN; }
    else { ++task.lease; }
  }
  return ret;
}
int validate_result(ObIAllocator &allocator, const ObColumnSchemaV2 &column,
    const AIColumnDefinition &definition, const ObString &result)
{
  int ret = OB_SUCCESS;
  if (definition.embedding_ || column.is_json()) {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, result,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, value))) {
    } else if (definition.embedding_) {
      if (!value || value->json_type() != ObJsonNodeType::J_ARRAY
          || value->element_count() != definition.dimension_) { ret = OB_INVALID_DATA; }
      else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < value->element_count(); ++i) {
          ObIJsonBase *element = nullptr;
          if (OB_FAIL(value->get_array_element(i, element))) {
          } else if (!element || (element->json_type() != ObJsonNodeType::J_INT
              && element->json_type() != ObJsonNodeType::J_UINT && element->json_type() != ObJsonNodeType::J_DOUBLE
              && element->json_type() != ObJsonNodeType::J_DECIMAL)
              || (element->json_type() == ObJsonNodeType::J_DOUBLE && !std::isfinite(element->get_double()))) {
            ret = OB_INVALID_DATA;
          }
        }
      }
    }
  } else if (column.get_data_type() >= ObUTinyIntType && column.get_data_type() <= ObUInt64Type) {
    uint64_t integer = 0;
    auto parsed = std::from_chars(result.ptr(), result.ptr() + result.length(), integer);
    if (parsed.ec != std::errc() || parsed.ptr != result.ptr() + result.length()) { ret = OB_INVALID_DATA; }
    else if ((column.get_data_type() == ObUTinyIntType && integer > UINT8_MAX)
        || (column.get_data_type() == ObUSmallIntType && integer > UINT16_MAX)
        || (column.get_data_type() == ObUMediumIntType && integer > 16777215)
        || (column.get_data_type() == ObUInt32Type && integer > UINT32_MAX)) { ret = OB_DATA_OUT_OF_RANGE; }
  } else if (ob_is_integer_type(column.get_data_type())) {
    int64_t integer = 0;
    auto parsed = std::from_chars(result.ptr(), result.ptr() + result.length(), integer);
    if (parsed.ec != std::errc() || parsed.ptr != result.ptr() + result.length()) { ret = OB_INVALID_DATA; }
    else if ((column.get_data_type() == ObTinyIntType && (integer < -128 || integer > 127))
        || (column.get_data_type() == ObSmallIntType && (integer < -32768 || integer > 32767))
        || (column.get_data_type() == ObMediumIntType && (integer < -8388608 || integer > 8388607))
        || (column.get_data_type() == ObInt32Type && (integer < INT32_MIN || integer > INT32_MAX))) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  } else if (ob_is_numeric_type(column.get_data_type())) {
    double value = 0;
    auto parsed = std::from_chars(result.ptr(), result.ptr() + result.length(), value);
    if (parsed.ec != std::errc() || parsed.ptr != result.ptr() + result.length() || !std::isfinite(value)) {
      ret = OB_INVALID_DATA;
    }
  } else if (!ob_is_text_tc(column.get_data_type())
      && ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, result.ptr(), result.length()) > column.get_data_length()) {
    ret = OB_ERR_DATA_TOO_LONG;
  }
  return ret;
}
int invoke_task(ObArenaAllocator &allocator, ColumnTask &task, AIColumnDefinition &definition)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table = nullptr;
  const ObColumnSchemaV2 *column = nullptr;
  ObIJsonBase *spec_base = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) {
  } else if (OB_FAIL(schema_guard.get_table_schema(task.table_id, table))) {
  } else if (!table || !(column = table->get_column_schema(task.column_id)) || !column->is_ai_column()
      || column->get_schema_version() != task.version) { ret = OB_SCHEMA_EAGAIN; }
  else if (OB_FAIL(definition.decode(allocator, column->get_ai_definition()))) {
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, task.spec,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, spec_base))) {
  } else if (!spec_base || spec_base->json_type() != ObJsonNodeType::J_OBJECT) { ret = OB_INVALID_DATA; }
  else {
    auto *spec = static_cast<ObJsonObject *>(spec_base);
    ObJsonNode *prompt = spec->get_value("prompt");
    ObString content;
    if (!prompt) { ret = OB_INVALID_DATA; }
    else if (prompt->json_type() == ObJsonNodeType::J_STRING) {
      content = static_cast<ObJsonString *>(prompt)->get_str();
    } else if (prompt->json_type() == ObJsonNodeType::J_OBJECT) {
      ret = ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator,
          static_cast<ObJsonObject *>(prompt), content);
    } else { ret = OB_INVALID_DATA; }
    ObAIFuncExprInfo *info = nullptr;
    ObAiModelEndpointInfo endpoint;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, definition.model_name_, info))) {
    } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(allocator, definition.model_name_, endpoint))) {
    } else {
      ObAIFuncModel model(allocator, *info, endpoint);
      model.set_request_policy(0, 10);
      ObJsonObject config(&allocator);
      if (definition.embedding_) {
        ObJsonInt dimension(definition.dimension_);
        ObArray<ObString> inputs, outputs;
        if (OB_FAIL(config.add("dimensions", &dimension))) {
        } else if (OB_FAIL(inputs.push_back(content))) {
        } else if (OB_FAIL(model.call_dense_embedding_vector_v2(inputs, &config, outputs))) {
        } else if (outputs.count() != 1) { ret = OB_INVALID_DATA; }
        else { task.result = outputs.at(0); }
      } else { ret = model.call_completion(content, &config, task.result, definition.result_hint_); }
      if (OB_SUCC(ret)) { ret = validate_result(allocator, *column, definition, task.result); }
    }
  }
  return ret;
}
int stage_task(const ColumnTask &task, const AIColumnDefinition &definition, int invocation_error)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected = 0;
  const bool retry = invocation_error != OB_SUCCESS && task.retries < definition.max_retries_
      && invocation_error != OB_SCHEMA_EAGAIN;
  const int state = invocation_error == OB_SUCCESS ? 2 : retry ? 3 : 4;
  if (OB_FAIL(sql.append_fmt("UPDATE oceanbase.__all_ai_column_task SET state=%d,retry_count=%ld,"
      "eligible_ts=DATE_ADD(NOW(6),INTERVAL %ld SECOND),lease_expire_ts=NULL,result_value=",
      state, task.retries + (retry ? 1 : 0), retry ? std::min<int64_t>(30, 1LL << std::min<int64_t>(5, task.retries)) : 0))) {
  } else if (invocation_error == OB_SUCCESS) { ret = worker_literal(sql, task.result); }
  else { ret = sql.append("NULL"); }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt(",last_error='%d' WHERE ", invocation_error))) {
  } else if (OB_FAIL(request_condition(sql, task, true))) {
  } else if (OB_FAIL(sql.append(" AND lease_expire_ts>NOW(6)"))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected))) {
  } else if (affected != 1) { ret = OB_EAGAIN; }
  return ret;
}
int publish_group(ObArenaAllocator &allocator, const ObString &request)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction transaction;
  ObSqlString query;
  ObSEArray<ColumnTask, 8> tasks;
  if (OB_FAIL(query.append("SELECT * FROM oceanbase.__all_ai_column_task WHERE request_id="))) {
  } else if (OB_FAIL(worker_literal(query, request))) {
  } else if (OB_FAIL(query.append(" ORDER BY column_id FOR UPDATE"))) {
  } else if (OB_FAIL(transaction.start(GCTX.sql_proxy_))) {
  } else {
    alignas(16) ObMySQLProxy::MySQLResult response;
    if (OB_FAIL(transaction.read(response, query.ptr()))) {
    } else if (!response.get_result()) { ret = OB_ERR_UNEXPECTED; }
    else {
      auto &rows = *response.get_result();
      while (OB_SUCC(ret) && OB_SUCCESS == (ret = rows.next())) {
        ColumnTask task;
        if (OB_FAIL(read_task(rows, allocator, task))) {
        } else if (task.state != 2 && task.state != 4) { ret = OB_EAGAIN; }
        else if (task.state == 2 && OB_FAIL(worker_copy(rows, "result_value", allocator, task.result))) {
        } else { ret = tasks.push_back(task); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
    response.reset();
    if (OB_SUCC(ret) && !tasks.empty()) {
      ObSchemaGetterGuard guard;
      const ObTableSchema *table = nullptr;
      const ObDatabaseSchema *database = nullptr;
      bool obsolete = false;
      ObSqlString update;
      if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
      } else if (OB_FAIL(guard.get_table_schema(tasks.at(0).table_id, table))) {
      } else if (!table) { obsolete = true; }
      else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
      } else if (!database) { obsolete = true; }
      else if (OB_FAIL(update.append("UPDATE "))) {
      } else if (OB_FAIL(worker_identifier(update, database->get_database_name_str()))) {
      } else if (OB_FAIL(update.append("."))) {
      } else if (OB_FAIL(worker_identifier(update, table->get_table_name_str()))) {
      } else { ret = update.append(" SET "); }
      ObSEArray<AIColumnDefinition, 8> definitions;
      for (int64_t i = 0; OB_SUCC(ret) && !obsolete && i < tasks.count(); ++i) {
        const ColumnTask &task = tasks.at(i);
        const ObColumnSchemaV2 *column = table->get_column_schema(task.column_id);
        AIColumnDefinition definition;
        if (!column || !column->is_ai_column() || column->get_schema_version() != task.version
            || task.table_id != table->get_table_id() || task.predicate != tasks.at(0).predicate) {
          obsolete = true;
        } else if (OB_FAIL(definition.decode(allocator, column->get_ai_definition()))) {
        } else if (OB_FAIL(definitions.push_back(definition))) {
        } else if (i && OB_FAIL(update.append(","))) {
        } else if (OB_FAIL(worker_identifier(update, column->get_column_name_str()))) {
        } else if (OB_FAIL(update.append("="))) {
        } else if (task.state == 2) { ret = worker_literal(update, task.result); }
        else { ret = update.append("NULL"); }
        if (OB_SUCC(ret) && !obsolete) {
          const ObColumnSchemaV2 *state = table->get_column_schema(definition.state_column_id_);
          if (!state) { ret = OB_ERR_UNEXPECTED; }
          else if (OB_FAIL(update.append(","))) {
          } else if (OB_FAIL(worker_identifier(update, state->get_column_name_str()))) {
          } else { ret = update.append_fmt("=%d", task.state == 2 ? 1 : 2); }
        }
      }
      if (OB_SUCC(ret) && !obsolete) {
        if (OB_FAIL(update.append(" WHERE ("))) {
        } else if (OB_FAIL(update.append(tasks.at(0).predicate))) {
        } else { ret = update.append(")"); }
        for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
          const auto *column = table->get_column_schema(definitions.at(i).request_column_id_);
          ObSqlString request_token;
          if (!column) { ret = OB_ERR_UNEXPECTED; }
          else if (OB_FAIL(update.append(" AND "))) {
          } else if (OB_FAIL(worker_identifier(update, column->get_column_name_str()))) {
          } else if (OB_FAIL(update.append("="))) {
          } else if (OB_FAIL(request_token.append_fmt("%ld:%.*s", tasks.at(i).version,
              request.length(), request.ptr()))) {
          } else { ret = worker_literal(update, request_token.string()); }
        }
        int64_t affected = 0;
        if (OB_SUCC(ret)) { ret = transaction.write(update.ptr(), affected); }
      }
      if (OB_SUCC(ret)) {
        ObSqlString remove;
        int64_t affected = 0;
        if (OB_FAIL(remove.append("DELETE FROM oceanbase.__all_ai_column_task WHERE request_id="))) {
        } else if (OB_FAIL(worker_literal(remove, request))) {
        } else { ret = transaction.write(remove.ptr(), affected); }
      }
    }
  }
  if (transaction.is_started()) {
    const int end_ret = transaction.end(OB_SUCC(ret));
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}
int recover_publication(ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  alignas(16) ObMySQLProxy::MySQLResult result;
  ObString request;
  if (OB_FAIL(GCTX.sql_proxy_->read(result,
      "SELECT t.request_id FROM oceanbase.__all_ai_column_task t WHERE t.state IN (2,4) "
      "AND NOT EXISTS (SELECT 1 FROM oceanbase.__all_ai_column_task p WHERE p.request_id=t.request_id AND p.state NOT IN (2,4)) LIMIT 1"))) {
  } else if (!result.get_result()) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(result.get_result()->next())) {
  } else { ret = worker_copy(*result.get_result(), "request_id", allocator, request); }
  result.reset();
  if (OB_SUCC(ret)) { ret = publish_group(allocator, request); }
  return ret;
}

int reconcile_definitions(ObArenaAllocator &allocator)
{
  // DDL may add an AI column to existing rows, or change its prompt. Such
  // rows are enqueued through exactly the same transactional DML path as a
  // user retry; no remote request is made by the DDL transaction.
  static std::atomic<int64_t> next_check{0};
  const int64_t now = ObTimeUtility::current_time();
  int64_t previous = next_check.load();
  if (now < previous || !next_check.compare_exchange_strong(previous, now + 1000000)) {
    return OB_ITER_END;
  }
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<uint64_t, uint64_t>, 16> columns;
  {
    alignas(16) ObMySQLProxy::MySQLResult result;
    if (OB_FAIL(GCTX.sql_proxy_->read(result,
        "SELECT table_id,column_id FROM oceanbase.__all_ai_column ORDER BY table_id,column_id LIMIT 4096"))) {
    } else if (!result.get_result()) { ret = OB_ERR_UNEXPECTED; }
    else {
      while (OB_SUCCESS == (ret = result.get_result()->next())) {
        int64_t table = 0, column = 0;
        if (OB_FAIL(result.get_result()->get_int("table_id", table))) {
        } else if (OB_FAIL(result.get_result()->get_int("column_id", column))) {
        } else if (table <= 0 || column <= 0) { ret = OB_INVALID_DATA;
        } else { ret = columns.push_back({table, column}); }
        if (OB_FAIL(ret)) { break; }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  ObSchemaGetterGuard guard;
  if (OB_SUCC(ret)) { ret = GCTX.schema_service_->get_runtime_schema_guard(guard); }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObTableSchema *table = nullptr;
    const ObDatabaseSchema *database = nullptr;
    const ObColumnSchemaV2 *column = nullptr, *state = nullptr, *request = nullptr, *spec = nullptr;
    AIColumnDefinition definition;
    ObSqlString update;
    if (OB_FAIL(guard.get_table_schema(columns.at(i).first, table))) {
    } else if (!table || !(column = table->get_column_schema(columns.at(i).second)) || !column->is_ai_column()) {
      continue;
    } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
    } else if (!database) { continue; }
    else if (OB_FAIL(definition.decode(allocator, column->get_ai_definition()))) {
    } else if (!(state = table->get_column_schema(definition.state_column_id_))
        || !(request = table->get_column_schema(definition.request_column_id_))
        || !(spec = table->get_column_schema(definition.result_spec_column_id_))) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(update.append("UPDATE /*+ PARALLEL(1) */ "))) {
    } else if (OB_FAIL(worker_identifier(update, database->get_database_name_str()))) {
    } else if (OB_FAIL(update.append("."))) {
    } else if (OB_FAIL(worker_identifier(update, table->get_table_name_str()))) {
    } else if (OB_FAIL(update.append(" SET "))) {
    } else if (OB_FAIL(worker_identifier(update, column->get_column_name_str()))) {
    } else if (OB_FAIL(update.append("=NULL,"))) {
    } else if (OB_FAIL(worker_identifier(update, state->get_column_name_str()))) {
    } else if (OB_FAIL(update.append("=0,"))) {
    } else if (OB_FAIL(worker_identifier(update, request->get_column_name_str()))) {
    } else if (OB_FAIL(update.append("=UUID(),"))) {
    } else if (OB_FAIL(worker_identifier(update, spec->get_column_name_str()))) {
    } else if (OB_FAIL(update.append("=DEFAULT WHERE "))) {
    } else if (OB_FAIL(worker_identifier(update, request->get_column_name_str()))) {
    } else if (OB_FAIL(update.append(" IS NULL OR "))) {
    } else if (OB_FAIL(worker_identifier(update, request->get_column_name_str()))) {
    } else if (OB_FAIL(update.append_fmt(" NOT LIKE '%ld:%%' LIMIT 64", column->get_schema_version()))) {
    } else {
      AIColumnEnqueueScope scope;
      int64_t affected = 0;
      if (OB_FAIL(GCTX.sql_proxy_->write(update.ptr(), affected))) {
      } else if (affected > 0) {
        if (affected == 64) { next_check.store(now); }
        return OB_SUCCESS;
      }
    }
  }
  return OB_SUCC(ret) ? OB_ITER_END : ret;
}
} // namespace

int ai_column_worker_once()
{
  if (!GCTX.sql_proxy_ || !GCTX.schema_service_ || !GCTX.schema_service_->is_runtime_schema_refreshed()
      || !share::server_runtime()->is_write_enabled()) { return OB_EAGAIN; }
  ObArenaAllocator allocator("AIColumnWorker");
  ColumnTask task;
  int ret = claim_task(allocator, task);
  if (ret == OB_ITER_END) {
    ret = recover_publication(allocator);
    if (ret == OB_ITER_END) { ret = reconcile_definitions(allocator); }
    return ret;
  }
  if (OB_SUCC(ret)) {
    AIColumnDefinition definition;
    const int invocation_error = invoke_task(allocator, task, definition);
    if (OB_SUCCESS == (ret = stage_task(task, definition, invocation_error))) {
      ret = publish_group(allocator, task.request);
    }
  }
  return ret;
}
} // namespace oceanbase::omt
