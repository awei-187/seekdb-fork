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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_inner_sql_connection.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "share/schema/ai_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

static int ai_read_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int32_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name.ptr()[i] == '`' && OB_FAIL(sql.append("`"))) { break; }
    ret = sql.append(name.ptr() + i, 1);
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}

static int check_ai_column_reads(ObExecContext &ctx, const ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  bool strict = false;
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (!session || !session->is_user_session() || plan.ai_column_reads_.empty()) { return ret; }
  if (OB_FAIL(session->get_sys_variable(oceanbase::share::SYS_VAR_AI_COLUMN_STRICT_READ, strict))) {
    return ret;
  } else if (!strict) { return ret; }
  auto *schema_guard = ctx.get_sql_ctx()->schema_guard_;
  if (!schema_guard || plan.ai_column_reads_.count() % 2) { return OB_ERR_UNEXPECTED; }
  ObArenaAllocator allocator("AIColumnRead");
  oceanbase::common::sqlclient::ObISQLConnectionGuard connection_guard;
  ObIInnerSQLConnection *connection = nullptr;
  if (OB_FAIL(oceanbase::query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection_guard))) {
  } else if (!(connection = as_inner_sql_connection(connection_guard.get_ptr()))) {
    ret = OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan.ai_column_reads_.count(); i += 2) {
    const uint64_t table_id = plan.ai_column_reads_.at(i), column_id = plan.ai_column_reads_.at(i + 1);
    const oceanbase::share::schema::ObTableSchema *table = nullptr;
    const oceanbase::share::schema::ObDatabaseSchema *database = nullptr;
    const oceanbase::share::schema::ObColumnSchemaV2 *column = nullptr, *state = nullptr;
    oceanbase::share::schema::AIColumnDefinition definition;
    ObSqlString sql;
    if (OB_FAIL(schema_guard->get_table_schema(table_id, table))) {
    } else if (!table || !(column = table->get_column_schema(column_id)) || !column->is_ai_column()) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(definition.decode(allocator, column->get_ai_definition()))) {
    } else if (!(state = table->get_column_schema(definition.state_column_id_))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(schema_guard->get_database_schema(table->get_database_id(), database))) {
    } else if (!database) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(sql.append("SELECT 1 FROM "))) {
    } else if (OB_FAIL(ai_read_identifier(sql, database->get_database_name_str()))) {
    } else if (OB_FAIL(sql.append("."))) {
    } else if (OB_FAIL(ai_read_identifier(sql, table->get_table_name_str()))) {
    } else if (OB_FAIL(sql.append(" WHERE "))) {
    } else if (OB_FAIL(ai_read_identifier(sql, state->get_column_name_str()))) {
    } else if (OB_FAIL(sql.append(" IS NULL OR "))) {
    } else if (OB_FAIL(ai_read_identifier(sql, state->get_column_name_str()))) {
    } else if (OB_FAIL(sql.append("<>1 LIMIT 1"))) {
    } else {
      ObSQLSessionInfo::StmtSavedValue saved_session;
      ObIInnerSQLConnection::SavedValue saved_connection;
      ObExecContext::AIColumnNestedSQLGuard nested_guard(ctx);
      if (OB_FAIL(connection->begin_nested_session(saved_session, saved_connection, true))) {
      } else {
        bool incomplete = false;
        {
          alignas(16) ObISQLClient::ReadResult result;
          if (OB_FAIL(connection->execute_read(sql.ptr(), result))) {
          } else if (!result.get_result()) { ret = OB_ERR_UNEXPECTED; }
          else {
            ret = result.get_result()->next();
            if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
            else if (OB_SUCC(ret)) { incomplete = true; }
          }
        }
        const int end_ret = connection->end_nested_session(saved_session, saved_connection);
        if (OB_SUCC(ret)) { ret = end_ret; }
        if (OB_SUCC(ret) && incomplete) {
          ret = OB_AI_COLUMN_NOT_READY;
          LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, table_id, column_id);
        }
      }
    }
  }
  return ret;
}

int ObExecutor::init(ObPhysicalPlan *plan)
{
  int ret = OB_SUCCESS;
  if (true == inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    phy_plan_ = plan;
    inited_ = true;
  }
  return ret;
}

void ObExecutor::reset()
{
  inited_ = false;
  phy_plan_ = NULL;
  execution_id_ = OB_INVALID_ID;
}

int ObExecutor::execute_plan(ObExecContext &ctx)
{
  NG_TRACE(exec_plan_begin);
  int ret = OB_SUCCESS;
  ObSqlExecutorCtx &task_exec_ctx = ctx.get_sql_exec_ctx();
  ObExecuteResult &exec_result = task_exec_ctx.get_execute_result();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  int64_t batched_stmt_cnt = ctx.get_sql_ctx()->multi_stmt_item_.get_batched_stmt_cnt();
  ctx.set_use_temp_expr_ctx_cache(true);
  // If the batch execution is rewritten by insert multi values, there is no need to repack multiple times
  if (ctx.get_sql_ctx()->is_do_insert_batch_opt()) {
    batched_stmt_cnt = 0;
  }
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(phy_plan_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(session_info->set_cur_phy_plan(phy_plan_))) {
  } else if (OB_FAIL(check_ai_column_reads(ctx, *phy_plan_))) {
  } else if (OB_FAIL(phy_plan_->get_expr_frame_info()
                                 .pre_alloc_exec_memory(ctx))) {
  } else if (batched_stmt_cnt > 0
      && OB_FAIL(plan_ctx->create_implicit_cursor_infos(batched_stmt_cnt))) {
  } else {
    ObPhyPlanType execute_type = phy_plan_->get_plan_type();
    // Some parallel plans do not require the PX scheduler.
    if (execute_type != OB_PHY_PLAN_LOCAL && phy_plan_->is_require_local_execution()) {
      execute_type = OB_PHY_PLAN_LOCAL;
      LOG_TRACE("change the plan execution type",
          "fact", execute_type, K(phy_plan_->get_plan_type()));
    }

    switch (execute_type) {
      case OB_PHY_PLAN_LOCAL: {
        ObOperator *op = NULL;
        if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
        } else if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          exec_result.set_static_engine_root(op);
        }
        break;
      }
      case OB_PHY_PLAN_DISTRIBUTED:
        // PX special path
        // PX mode, scheduling work is handled by the ObPxCoord operator
        ret = execute_static_cg_px_plan(ctx);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  NG_TRACE(exec_plan_end);
  return ret;
}

int ObExecutor::execute_static_cg_px_plan(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObOperator *op = NULL;
  if (OB_FAIL(phy_plan_->get_root_op_spec()->create_op_input(ctx))) {
  } else if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ctx.get_sql_executor_ctx()
        ->get_execute_result()
        .set_static_engine_root(op);
  }
  return ret;
}

int ObExecutor::close(ObExecContext &ctx)
{
  // close function should be designed to be callable at any time, therefore regardless of the value of inited_
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  if (OB_LIKELY(NULL != session_info)) {
    // Reset cur_phy_plan_ in session to NULL
    session_info->reset_cur_phy_plan_to_null();
  }
  return ret;
}
