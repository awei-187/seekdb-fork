# Copyright (c) 2025 OceanBase.
# Licensed under the Apache License, Version 2.0.
"""Additional protocol, batching, DDL, and strict-read regression checks."""
import argparse
import json
from pathlib import Path
import time
from urllib.request import urlopen
import uuid

import pymysql

from run_regression import Runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2881)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--fixture-root", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    args = parser.parse_args()
    args.report_dir.mkdir(parents=True, exist_ok=True)
    runner = Runner(args)
    database = "competition_edge_" + uuid.uuid4().hex[:12]
    models = []

    def query(sql):
        with runner.connection.cursor() as cursor:
            cursor.execute(runner.actual_sql(sql))
            return cursor.fetchall() if cursor.description else ()

    def scalar(sql):
        return query(sql)[0][0]

    def expect_error(sql, code=None):
        try:
            query(sql)
        except pymysql.Error as error:
            if code is not None:
                assert error.args[0] == code, error
            return
        raise AssertionError("Expected an error: " + sql)

    def model(kind, path, column=False):
        name = database + "_" + str(len(models))
        models.append(name)
        query("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('%s','%s')" %
              (name, json.dumps({"type": kind, "model_name": "mock"})))
        port = runner.port_map[19121 if column else 8000]
        endpoint = json.dumps({"ai_model_name": name, "provider": "siliconflow" if kind == "rerank" else "openai", "access_key": "mock-key",
                               "url": "http://127.0.0.1:%d/%s" % (port, path), "request_model_name": "mock"})
        query("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('%s_ep','%s')" % (name, endpoint))
        return name

    def wait_for(sql):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if str(scalar(sql)) == "1":
                return
            time.sleep(0.1)
        raise AssertionError("Condition did not become true: " + sql)

    try:
        runner.start_mock(8000, function=True)
        runner.start_mock(19121)
        query("CREATE DATABASE " + database)
        query("USE " + database)
        embed = model("dense_embedding", "v1/embeddings")
        complete = model("completion", "v1/chat/completions")
        rerank = model("rerank", "v1/rerank")
        bad_embed = model("dense_embedding", "malformed/embeddings")
        bad_complete = model("completion", "malformed/completions")

        assert scalar("SELECT AI_COMPLETE('%s','hello')" % complete) == "mock:hello"
        expect_error("SELECT AI_EMBED('%s','hello',2)" % bad_embed)
        expect_error("SELECT AI_COMPLETE('%s','hello')" % bad_complete)
        expect_error("SELECT AI_EMBED('%s','hello',0)" % embed)
        assert str(scalar("SELECT AI_EMBED('%s',NULL,2) IS NULL" % embed)) == "1"
        query("CREATE TABLE batch_rows(id INT PRIMARY KEY, text_value LONGTEXT)")
        query("INSERT INTO batch_rows VALUES " + ",".join("(%d,'row%d')" % (i, i) for i in range(130)))
        rows = query("SELECT id, AI_EMBED('%s',text_value,2) FROM batch_rows ORDER BY id" % embed)
        assert len(rows) == 130
        for row in rows:
            vector = json.loads(row[1])
            assert vector == [int(row[0]) + 1, 0], row
        with urlopen("http://127.0.0.1:%d/events" % runner.port_map[8000], timeout=5) as response:
            events = json.load(response)
        batches = [event["body"]["input"] for event in events
                   if event["path"] == "/v1/embeddings" and isinstance(event["body"].get("input"), list)]
        assert sum(len(batch) for batch in batches) == 130
        assert any(len(batch) > 1 for batch in batches), "Embedding evaluation fell back to one HTTP call per row"
        assert max(map(len, batches)) <= 64

        documents = [{"id": i, "text": "apple" if i in (3, 25, 44) else "banana"} for i in range(45)]
        result = json.loads(scalar("SELECT AI_RERANK('%s','apple','%s','text')" % (rerank, json.dumps(documents))))
        assert [row["id"] for row in result[:3]] == [3, 25, 44]
        assert sorted(row["id"] for row in result) == list(range(45))
        print("PASS AI response validation, embedding batching/order, and rerank global indices", flush=True)

        column_model = model("completion", "v1/chat/completions", column=True)
        query("CREATE TABLE docs(id BIGINT PRIMARY KEY, content VARCHAR(200))")
        query("INSERT INTO docs VALUES(1,'existing'),(2,'existing-two')")
        query("ALTER TABLE docs ADD COLUMN result JSON AI COLUMN (AI_COMPLETE('%s',content))" % column_model)
        wait_for("SELECT COUNT(*)=2 FROM docs WHERE result IS NOT NULL")
        assert "AI COLUMN" in query("SHOW CREATE TABLE docs")[0][1]
        query("ALTER TABLE docs MODIFY COLUMN result JSON AI COLUMN (AI_COMPLETE('%s',CONCAT('modified:',content)))" % column_model)
        wait_for("SELECT COUNT(*)=2 FROM docs WHERE JSON_UNQUOTE(JSON_EXTRACT(result,'$.input')) LIKE 'modified:%%'")

        query("START TRANSACTION")
        query("UPDATE docs SET content='slow-old' WHERE id=1")
        query("SET SESSION ai_column_strict_read=ON")
        expect_error("SELECT result FROM docs WHERE 0", 11120)
        expect_error("UPDATE docs SET content='x' WHERE result IS NOT NULL AND 0", 11120)
        expect_error("DELETE FROM docs WHERE result IS NOT NULL AND 0", 11120)
        assert str(scalar("SELECT COUNT(*) FROM docs WHERE id=2")) == "1"
        query("SET SESSION ai_column_strict_read=OFF")
        query("ROLLBACK")
        wait_for("SELECT COUNT(*)=2 FROM docs WHERE result IS NOT NULL")
        print("PASS AI column ALTER backfill/MODIFY and strict reads before filtering", flush=True)
    finally:
        try:
            query("ROLLBACK")
            query("DROP DATABASE IF EXISTS " + database)
            for name in reversed(models):
                query("CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('%s_ep')" % name)
                query("CALL DBMS_AI_SERVICE.DROP_AI_MODEL('%s')" % name)
        finally:
            runner.close()


if __name__ == "__main__":
    main()
