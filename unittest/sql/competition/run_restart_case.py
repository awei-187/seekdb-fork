# Copyright (c) 2025 OceanBase.
# Licensed under the Apache License, Version 2.0.
"""Prepare a durable leased task; recover it after an externally managed restart.

This script never starts, stops, or restarts the database server itself.
"""
import argparse
import json
from pathlib import Path
import re
import time
import uuid

from run_regression import Runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "recover"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2881)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--fixture-root", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    args = parser.parse_args()
    args.report_dir.mkdir(parents=True, exist_ok=True)
    manifest = args.report_dir / "restart-case.json"
    runner = Runner(args)

    def query(sql):
        with runner.connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall() if cursor.description else ()

    try:
        if args.phase == "prepare":
            if manifest.exists():
                raise RuntimeError("Recover the existing restart case first: " + str(manifest))
            database = "competition_restart_" + uuid.uuid4().hex[:12]
            model = database + "_model"
            query("CREATE DATABASE " + database)
            query("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('%s','{\"type\":\"completion\",\"model_name\":\"mock\"}')" % model)
            endpoint = json.dumps({"ai_model_name": model, "provider": "openai", "access_key": "mock-key",
                                   "url": "http://127.0.0.1:19130/v1/chat/completions"})
            query("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('%s_ep','%s')" % (model, endpoint))
            query("CREATE TABLE %s.docs(id BIGINT PRIMARY KEY, content VARCHAR(100), result VARCHAR(100) AI COLUMN (AI_COMPLETE('%s',content)))" % (database, model))
            table_id = int(query("SELECT table_id FROM oceanbase.__all_ai_column WHERE column_name='result' AND table_id IN (SELECT t.table_id FROM oceanbase.__all_table t JOIN oceanbase.__all_database d ON d.database_id=t.database_id WHERE d.database_name='%s')" % database)[0][0])
            query("START TRANSACTION")
            query("INSERT INTO %s.docs(id,content) VALUES(1,'durable')" % database)
            # Publish the business row and a leased task together, without a
            # live HTTP call racing the externally managed restart.
            query("UPDATE oceanbase.__all_ai_column_task SET state=1,lease_owner='crashed-test-owner',lease_version=7,lease_expire_ts=DATE_ADD(NOW(6),INTERVAL 1 HOUR) WHERE table_id=%d" % table_id)
            query("COMMIT")
            manifest.write_text(json.dumps({"database": database, "table_id": table_id}), encoding="utf-8")
            print("PASS durable leased task prepared; restart the isolated server before recovery", flush=True)
        else:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            database, table_id = data["database"], int(data["table_id"])
            if not re.fullmatch(r"competition_restart_[0-9a-f]{12}", database) or table_id <= 0:
                raise ValueError("Invalid restart-case manifest")
            model = database + "_model"
            assert query("SELECT state,lease_version FROM oceanbase.__all_ai_column_task WHERE table_id=%d" % table_id) == (("1", "7"),)
            assert query("SELECT result FROM %s.docs" % database) == ((None,),)
            runner.start_mock(8000, function=True)
            query("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('%s_ep','%s')" %
                  (model, json.dumps({"url": "http://127.0.0.1:%d/v1/chat/completions" % runner.port_map[8000]})))
            query("UPDATE oceanbase.__all_ai_column_task SET lease_expire_ts=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE table_id=%d" % table_id)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if query("SELECT result FROM %s.docs" % database) == (("mock:durable",),):
                    break
                time.sleep(0.1)
            else:
                raise AssertionError("Persisted lease was not recovered")
            assert query("SELECT COUNT(*) FROM oceanbase.__all_ai_column_task WHERE table_id=%d" % table_id) == (("0",),)
            query("DROP DATABASE " + database)
            query("CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('%s_ep')" % model)
            query("CALL DBMS_AI_SERVICE.DROP_AI_MODEL('%s')" % model)
            manifest.unlink()
            print("PASS persisted task, expired lease recovery, publication, and task cleanup", flush=True)
    finally:
        runner.close()


if __name__ == "__main__":
    main()
