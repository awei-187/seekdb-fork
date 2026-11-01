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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ob_dml_service.h"
#include "share/schema/ai_column_schema.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase::sql
{
using namespace common;
using namespace share::schema;
namespace
{
int dml_ai_literal(ObSqlString &sql, const ObString &value)
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
int64_t ai_row_index(const ObDMLBaseCtDef &ctdef, uint64_t id)
{
  for (int64_t i = 0; i < ctdef.column_ids_.count(); ++i) {
    if (ctdef.column_ids_.at(i) == id) { return i; }
  }
  return -1;
}
int ai_row_datum(const ObDMLBaseCtDef &ctdef, ObEvalCtx &ctx, uint64_t id,
    bool old, ObDatum *&datum, ObExpr *&expr)
{
  const auto &row = old ? ctdef.old_row_ : ctdef.new_row_;
  int64_t index = ai_row_index(ctdef, id);
  int ret = OB_SUCCESS;
  if (index < 0 || index >= row.count() || !(expr = row.at(index))) { ret = OB_ERR_UNEXPECTED; }
  else { ret = expr->eval(ctx, datum); }
  return ret;
}
int ai_changed(const ObDMLBaseCtDef &ctdef, ObTableModifyOp &op, uint64_t id, bool &changed)
{
  int ret = OB_SUCCESS;
  ObDatum *before = nullptr, *after = nullptr;
  ObExpr *old_expr = nullptr, *new_expr = nullptr;
  if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), id, true, before, old_expr))) {
  } else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), id, false, after, new_expr))) {
  } else if (before->is_null() || after->is_null()) {
    changed |= before->is_null() != after->is_null();
  } else if (ob_is_text_tc(old_expr->obj_meta_.get_type())) {
    ObEvalCtx::TempAllocGuard guard(op.get_eval_ctx());
    ObString left, right;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(op.get_exec_ctx(), guard.get_allocator(),
        *before, old_expr->datum_meta_, old_expr->obj_meta_.has_lob_header(), left))) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(op.get_exec_ctx(), guard.get_allocator(),
        *after, new_expr->datum_meta_, new_expr->obj_meta_.has_lob_header(), right))) {
    } else { changed |= left != right; }
  } else { changed |= !ObDatum::binary_equal(*before, *after); }
  return ret;
}
int ai_row_predicate(const ObDMLBaseCtDef &ctdef, ObTableModifyOp &op,
    const ObTableSchema &table, bool old, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table.get_rowkey_column_num(); ++i) {
    uint64_t id = OB_INVALID_ID;
    if (OB_FAIL(table.get_rowkey_info().get_column_id(i, id))) { break; }
    const ObColumnSchemaV2 *column = table.get_column_schema(id);
    ObDatum *datum = nullptr;
    ObExpr *expr = nullptr;
    if (!column) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), id, old, datum, expr))) {
    } else if (datum->is_null()) { ret = OB_ERR_UNEXPECTED; }
    else {
      if (i && OB_FAIL(sql.append(" AND "))) { break; }
      if (OB_FAIL(sql.append("`"))) { break; }
      const ObString &name = column->get_column_name_str();
      for (int32_t n = 0; OB_SUCC(ret) && n < name.length(); ++n) {
        if (name.ptr()[n] == '`' && OB_FAIL(sql.append("`"))) { break; }
        ret = sql.append(name.ptr() + n, 1);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append("` = "))) {
      } else if (ob_is_string_type(expr->obj_meta_.get_type())) {
        ret = dml_ai_literal(sql, datum->get_string());
      } else {
        ObObj object;
        char buffer[4096];
        int64_t pos = 0;
        if (OB_FAIL(datum->to_obj(object, expr->obj_meta_))) {
        } else if (OB_FAIL(object.print_sql_literal(buffer, sizeof(buffer), pos))) {
        } else { ret = sql.append(buffer, pos); }
      }
    }
  }
  if (OB_SUCC(ret) && sql.empty()) { ret = OB_NOT_SUPPORTED; }
  return ret;
}
int ai_nested_write(ObTableModifyOp &op, const ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObExecContext::AIColumnNestedSQLGuard nested_guard(op.get_exec_ctx());
  if (OB_FAIL(op.open_inner_conn())) {
  } else if (OB_FAIL(op.begin_nested_session(true))) {
  } else {
    ret = op.execute_write(sql.ptr());
    const int end_ret = op.end_nested_session();
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}
} // namespace

int ObDMLService::process_ai_column_row(const ObDMLBaseCtDef &ctdef, ObTableModifyOp &op, int event)
{
  int ret = OB_SUCCESS;
  auto &ctx = op.get_exec_ctx();
  ObSQLSessionInfo *session = ctx.get_my_session();
  const ObTableSchema *table = nullptr;
  if (!ctdef.is_primary_index_ || !session || (!session->is_user_session() && !AIColumnEnqueueScope::active())
      || is_inner_table(ctdef.das_base_ctdef_.index_tid_)) { return ret; }
  if (!ctx.get_sql_ctx() || !ctx.get_sql_ctx()->schema_guard_) { return OB_ERR_UNEXPECTED; }
  if (OB_FAIL(ctx.get_sql_ctx()->schema_guard_->get_table_schema(ctdef.das_base_ctdef_.index_tid_, table))) {
    return ret;
  } else if (!table) { return OB_ERR_UNEXPECTED; }
  bool has_ai = false;
  for (int64_t c = 0; c < table->get_column_count(); ++c) {
    has_ai |= table->get_column_schema_by_idx(c)->is_ai_column();
  }
  if (!has_ai) { return ret; }
  ObEvalCtx::TempAllocGuard guard(op.get_eval_ctx());
  ObIAllocator &allocator = guard.get_allocator();
  ObSqlString old_key, new_key;
  if (event != 1 && OB_FAIL(ai_row_predicate(ctdef, op, *table, true, old_key))) { return ret; }
  if (event != 3 && OB_FAIL(ai_row_predicate(ctdef, op, *table, false, new_key))) { return ret; }
  if (event == 3 || (event == 2 && old_key.string() != new_key.string())) {
    ObSqlString cleanup;
    if (OB_FAIL(cleanup.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND rowkey_predicate=",
        table->get_table_id()))) {
    } else if (OB_FAIL(dml_ai_literal(cleanup, old_key.string()))) {
    } else { ret = ai_nested_write(op, cleanup); }
    if (event == 3 || OB_FAIL(ret)) { return ret; }
  }
  char request[37] = {};
  bool generated = false;
  for (int64_t c = 0; OB_SUCC(ret) && c < table->get_column_count(); ++c) {
    const ObColumnSchemaV2 *column = table->get_column_schema_by_idx(c);
    if (!column || !column->is_ai_column()) { continue; }
    AIColumnDefinition definition;
    bool changed = event == 1 || old_key.string() != new_key.string();
    if (OB_FAIL(definition.decode(allocator, column->get_ai_definition()))) { break; }
    if (event == 2 && !changed) {
      ret = ai_changed(ctdef, op, definition.request_column_id_, changed);
    }
    for (int64_t d = 0; OB_SUCC(ret) && !changed && event == 2 && d < definition.dependency_ids_.count(); ++d) {
      ret = ai_changed(ctdef, op, definition.dependency_ids_.at(d), changed);
    }
    if (OB_FAIL(ret) || !changed) { continue; }
    ObDatum *result = nullptr, *state = nullptr, *request_datum = nullptr, *spec = nullptr;
    ObExpr *result_expr = nullptr, *state_expr = nullptr, *request_expr = nullptr, *spec_expr = nullptr;
    ObString spec_text;
    ObSqlString request_token;
    if (!generated && OB_FAIL(ObExprUuid::gen_server_uuid(request, 36))) {
    } else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), column->get_column_id(), false, result, result_expr))) {
    } else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), definition.state_column_id_, false, state, state_expr))) {
    } else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), definition.request_column_id_, false, request_datum, request_expr))) {
    } else if (OB_FAIL(ai_row_datum(ctdef, op.get_eval_ctx(), definition.result_spec_column_id_, false, spec, spec_expr))) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx, allocator, *spec,
        spec_expr->datum_meta_, spec_expr->obj_meta_.has_lob_header(), spec_text))) {
    } else if (OB_FAIL(request_token.append_fmt("%ld:%.*s", column->get_schema_version(), 36, request))) {
    } else {
      generated = true;
      // These slots are assignments in the native plan, so native secondary
      // and vector index maintenance observes the invalidated values.
      result->set_null();
      // Wrapper evaluation can share the old row's numeric payload. Detach the
      // payload before writing so storage's old-row check and rollback see it.
      char *state_buffer = state_expr->get_str_res_mem(op.get_eval_ctx(), sizeof(int64_t));
      ObString token_value = request_token.string();
      if (nullptr == state_buffer) { ret = OB_ALLOCATE_MEMORY_FAILED; }
      else { state->set_string(state_buffer, sizeof(int64_t)); state->set_int(0); }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(*request_expr, op.get_eval_ctx(), *request_datum,
          token_value))) {
      } else {
        result_expr->get_eval_info(op.get_eval_ctx()).evaluated_ = true;
        state_expr->get_eval_info(op.get_eval_ctx()).evaluated_ = true;
        request_expr->get_eval_info(op.get_eval_ctx()).evaluated_ = true;
        ObSqlString cleanup;
        if (OB_FAIL(cleanup.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND column_id=%lu AND rowkey_predicate=",
            table->get_table_id(), column->get_column_id()))) {
        } else if (OB_FAIL(dml_ai_literal(cleanup, new_key.string()))) {
        } else if (OB_FAIL(ai_nested_write(op, cleanup))) {
        }
        ObSqlString task;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(task.append("INSERT INTO oceanbase.__all_ai_column_task "
            "(request_id,column_id,table_id,definition_version,row_key,rowkey_predicate,result_spec,state,retry_count,eligible_ts,lease_version) VALUES("))) {
        } else if (OB_FAIL(dml_ai_literal(task, ObString(36, request)))) {
        } else if (OB_FAIL(task.append_fmt(",%lu,%lu,%ld,", column->get_column_id(), table->get_table_id(), column->get_schema_version()))) {
        } else if (OB_FAIL(dml_ai_literal(task, new_key.string()))) {
        } else if (OB_FAIL(task.append(","))) {
        } else if (OB_FAIL(dml_ai_literal(task, new_key.string()))) {
        } else if (OB_FAIL(task.append(","))) {
        } else if (OB_FAIL(dml_ai_literal(task, spec_text))) {
        } else if (OB_FAIL(task.append(",0,0,NOW(6),0)"))) {
        } else { ret = ai_nested_write(op, task); }
      }
    }
  }
  return ret;
}
} // namespace oceanbase::sql
