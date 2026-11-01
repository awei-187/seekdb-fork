# Copyright (c) 2025 OceanBase.
# Licensed under the Apache License, Version 2.0.
"""Run the competition's checked-in mysqltest cases on Windows or POSIX.

This adapter implements only the directives used by these seven cases. It
rejects unsupported commands and compares server text against the checked-in
results. Fixtures and mock processes belong to this invocation only.
Requires PyMySQL. The server must allow file SQL in --fixture-root through
secure_file_priv. No real model or remote HTTP endpoint is used.
"""

import argparse
import base64
import contextlib
import difflib
import importlib.util
import io
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid

import pymysql

ROOT = Path(__file__).resolve().parents[3]
SUITES = ROOT / "tools/deploy/mysql_test/test_suite"
ERROR_STATE = "HY000"
original_raise = pymysql.err.raise_mysql_exception


def capture_sqlstate(data):
    global ERROR_STATE
    ERROR_STATE = data[4:9].decode("ascii") if data[3:4] == b"#" else "HY000"
    return original_raise(data)


pymysql.err.raise_mysql_exception = capture_sqlstate


class Runner:
    def __init__(self, args):
        self.args = args
        self.connection = pymysql.connect(host=args.host, port=args.port, user=args.user,
                                          password=args.password, charset="utf8mb4",
                                          autocommit=True, conv={}, read_timeout=90)
        self.fixture = args.fixture_root.resolve() / ("competition-" + uuid.uuid4().hex)
        self.fixture.mkdir(parents=True)
        self.processes = {}
        self.port_map = {}
        self.output = []
        self.query_log = self.result_log = True
        self.warnings = True
        self.expected_errors = None
        self.replacements = []
        self.variables = {"OBSERVER_DIR": self.fixture.parent.as_posix(),
                          "FILE_SQL_DATA_DIR": self.fixture.as_posix()}
        mock_path = SUITES / "ai_column/include/ai_column_mock.py"
        module = importlib.util.spec_from_file_location("column_mock", mock_path)
        self.mock = importlib.util.module_from_spec(module)
        module.loader.exec_module(self.mock)

    def close(self):
        for process, stream in self.processes.values():
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
            stream.close()
        self.connection.close()
        # The generated UUID subdirectory is the only recursive removal.
        if self.fixture.parent == self.args.fixture_root.resolve():
            shutil.rmtree(self.fixture)

    def substitute(self, value):
        return re.sub(r"\$([A-Za-z_][A-Za-z0-9_]*)", lambda m: str(self.variables[m[1]]), value)

    def actual_sql(self, sql):
        sql = sql.replace("http://0.0.0.0:8000", f"http://127.0.0.1:{self.port_map[8000]}")
        for old, new in self.port_map.items():
            if old != 8000:
                sql = sql.replace(f"127.0.0.1:{old}/", f"127.0.0.1:{new}/")
        return sql

    def emit(self, text):
        for pattern, replacement in self.replacements:
            text = re.sub(pattern, replacement, text)
        self.output.append(text)

    def query(self, sql, visible=True):
        sql = self.substitute(sql).strip()
        expected = self.expected_errors if visible else None
        if visible:
            self.expected_errors = None
        if visible and self.query_log:
            self.emit("\n".join(line.strip() for line in sql.splitlines()) + ";")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(self.actual_sql(sql))
                rows = cursor.fetchall() if cursor.description else ()
                if expected is not None and 0 not in expected:
                    raise AssertionError(f"Expected SQL error {expected}, but query succeeded: {sql}")
                if visible and self.result_log and cursor.description:
                    self.emit("\t".join(column[0] for column in cursor.description))
                    for row in rows:
                        self.emit("\t".join("NULL" if value is None else str(value) for value in row))
                while cursor.nextset():
                    if cursor.description:
                        cursor.fetchall()
                if visible and self.result_log and self.warnings and cursor.warning_count:
                    cursor.execute("SHOW WARNINGS")
                    self.emit("Warnings:")
                    for row in cursor.fetchall():
                        self.emit("\t".join(str(value) for value in row))
                return rows[0][0] if rows else None
        except pymysql.Error as error:
            if expected is None or error.args[0] not in expected:
                raise AssertionError(f"Unexpected SQL error {error.args}: {sql}") from error
            if visible and self.result_log and 0 not in expected:
                self.emit(f"ERROR {ERROR_STATE}: {error.args[1]}")
            return None
        finally:
            if visible:
                self.replacements = []

    @staticmethod
    def free_port(preferred):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", preferred))
            except OSError:
                probe.bind(("127.0.0.1", 0))
            return probe.getsockname()[1]

    def start_mock(self, original, function=False):
        self.stop_mock(original)
        port = self.port_map.setdefault(original, self.free_port(original if original != 8000 else 19130))
        if function:
            script = Path(__file__).with_name("ai_function_mock.py")
            command = [sys.executable, str(script), "--port", str(port)]
            output = self.args.report_dir / "function-mock.log"
        else:
            script = SUITES / "ai_column/include/ai_column_mock.py"
            state = Path(self.mock.state_dir(port))
            state.mkdir(parents=True, exist_ok=True)
            Path(self.mock.state_path(port, "events.jsonl")).write_text("", encoding="utf-8")
            command = [sys.executable, str(script), "serve", str(port)]
            output = self.args.report_dir / f"column-mock-{port}.log"
        stream = output.open("w", encoding="utf-8")
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stream, stderr=stream,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        self.processes[original] = (process, stream)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Mock process failed; inspect {output}")
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    return
            time.sleep(0.05)
        raise TimeoutError(f"Mock did not start: {output}")

    def stop_mock(self, original):
        item = self.processes.pop(original, None)
        if item:
            process, stream = item
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
            stream.close()

    def command(self, name, value):
        if name in {"disable_warnings", "enable_warnings"}:
            self.warnings = name.startswith("enable")
            return
        if name.endswith("query_log"):
            self.query_log = name.startswith("enable")
        elif name.endswith("result_log"):
            self.result_log = name.startswith("enable")
        elif name == "echo":
            self.emit(self.substitute(value))
        elif name == "real_sleep":
            time.sleep(float(value))
        elif name == "error":
            self.expected_errors = {int(code) for code in value.split(",")}
        elif name == "replace_regex":
            self.replacements = [(a.replace(r"\/", "/"), b.replace(r"\/", "/"))
                                 for a, b in re.findall(r"/((?:\\.|[^/])*)/((?:\\.|[^/])*)/", value)]
            if not self.replacements:
                raise ValueError("Cannot parse replace_regex: " + value)
        elif name == "source":
            if value.endswith("copy_file_sql_data.inc"):
                for path in (SUITES / "file_sql/data").iterdir():
                    if path.name == "simple.parquet.b64":
                        (self.fixture / "simple.parquet").write_bytes(base64.b64decode(path.read_text()))
                    else:
                        shutil.copyfile(path, self.fixture / path.name)
                self.query_log = True
            elif value.endswith("remove_file_sql_data.inc"):
                self.query_log = True
            else:
                raise ValueError("Unsupported source: " + value)
        elif name == "exec":
            match = re.fullmatch(r"python3 .*ai_column_mock.py (start|stop|reset|assert) (\d+)(?: ([\w-]+))?", value)
            if not match:
                raise ValueError("Unsupported command: " + value)
            mode, original, assertion = match.groups()
            original = int(original)
            if mode == "start":
                self.start_mock(original)
            elif mode == "stop":
                self.stop_mock(original)
            elif mode == "reset":
                Path(self.mock.state_path(self.port_map[original], "events.jsonl")).write_text("", encoding="utf-8")
            else:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.mock.assert_events(self.port_map[original], assertion)
                self.emit(output.getvalue().rstrip())
        else:
            raise ValueError("Unsupported directive: " + name)

    def condition(self, text):
        text = self.substitute(text).strip()
        if re.fullmatch(r"-?\d+", text):
            return int(text) != 0
        match = re.fullmatch(r"(-?\d+)\s*(==|!=)\s*(-?\d+)", text)
        if not match:
            raise ValueError("Unsupported control condition: " + text)
        a, operator, b = match.groups()
        return (int(a) == int(b)) if operator == "==" else (int(a) != int(b))

    @staticmethod
    def quoted_end(text, start, delimiter):
        quote = None
        index = start
        while index < len(text):
            c = text[index]
            if quote:
                if c == "\\":
                    index += 2
                    continue
                if c == quote:
                    if index + 1 < len(text) and text[index + 1] == quote:
                        index += 2
                        continue
                    quote = None
            elif c in "'\"`":
                quote = c
            elif c == delimiter:
                return index
            index += 1
        raise ValueError("Unterminated statement")

    def execute(self, text):
        index = 0
        while index < len(text):
            if text[index].isspace():
                index += 1
                continue
            if text.startswith("#", index) or text.startswith("--", index):
                end = text.find("\n", index)
                end = len(text) if end < 0 else end
                if text.startswith("--", index):
                    name, _, value = text[index + 2:end].partition(" ")
                    self.command(name.strip(), value.strip())
                index = end
                continue
            control = re.match(r"(if|while)\s*\(([^)]*)\)\s*\{", text[index:])
            if control:
                mode, condition = control.groups()
                begin = index + control.end()
                depth, end = 1, begin
                while depth and end < len(text):
                    if text[end] == "{":
                        depth += 1
                    elif text[end] == "}":
                        depth -= 1
                    end += 1
                if depth:
                    raise ValueError("Unterminated control block")
                body = text[begin:end - 1]
                iterations = 0
                while self.condition(condition):
                    self.execute(body)
                    iterations += 1
                    if mode == "if":
                        break
                    if iterations > 1000:
                        raise RuntimeError("Control loop did not terminate")
                index = end
                continue
            end = self.quoted_end(text, index, ";")
            statement = text[index:end].strip()
            index = end + 1
            if statement.startswith("let "):
                match = re.fullmatch(r"let\s+\$(\w+)\s*=\s*(.*)", statement, re.S)
                key, value = match.groups()
                if key == "FILE_SQL_DATA_DIR":
                    value = self.fixture.as_posix()
                elif value.startswith("`") and value.endswith("`"):
                    value = self.query(value[1:-1], visible=False)
                    if value is None:
                        value = 0
                else:
                    value = self.substitute(value)
                self.variables[key] = value
            elif statement.startswith("dec $"):
                key = statement[5:].strip()
                self.variables[key] = int(self.variables[key]) - 1
            elif statement == "connection default":
                pass
            else:
                self.query(statement[5:] if statement.startswith("eval ") else statement)

    def run(self, case):
        self.output = []
        self.query_log = self.result_log = True
        self.warnings = True
        self.expected_errors = None
        self.replacements = []
        try:
            self.execute(case.read_text(encoding="utf-8"))
        finally:
            actual = "\n".join(self.output) + "\n"
            (self.args.report_dir / (case.stem + ".actual")).write_text(actual, encoding="utf-8")
        expected_file = case.parent.parent / "r/mysql" / (case.stem + ".result")
        expected = expected_file.read_text(encoding="utf-8")
        # mysqltest preserves continuation indentation inconsistently; it is
        # immaterial to these cases, whose result cells have no leading spaces.
        def normalize(text):
            lines = [line.lstrip() for line in text.splitlines()]
            # Native EXPLAIN emits one optional trailing space after this
            # comma; avoid requiring trailing whitespace in golden files.
            lines = [line.rstrip() if line.startswith("is_index_back=") else line for line in lines]
            return "\n".join(lines) + "\n"
        actual, expected = normalize(actual), normalize(expected)
        if actual != expected:
            diff = "".join(difflib.unified_diff(expected.splitlines(True), actual.splitlines(True),
                                                fromfile=str(expected_file), tofile=case.stem + ".actual"))
            (self.args.report_dir / (case.stem + ".diff")).write_text(diff, encoding="utf-8")
            raise AssertionError(f"Result mismatch for {case.name}; inspect {self.args.report_dir}")
        print(f"PASS {case.name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2881)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--fixture-root", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--case", action="append", help="Suite/case stem, e.g. ai_column/ai_column_ddl")
    args = parser.parse_args()
    args.report_dir.mkdir(parents=True, exist_ok=True)
    args.fixture_root.mkdir(parents=True, exist_ok=True)
    runner = Runner(args)
    try:
        runner.start_mock(8000, function=True)
        cases = args.case or ["ai_function/ai_function_enhancement", "file_sql/file_sql_basic"] + [
            "ai_column/ai_column_" + name for name in ("ddl", "t1_dml", "worker", "retry_strict", "vector_index")]
        runner.query("CREATE DATABASE IF NOT EXISTS test", visible=False)
        runner.query("USE test", visible=False)
        for name in cases:
            suite, stem = name.split("/")
            runner.run(SUITES / suite / "t" / (stem + ".test"))
    finally:
        runner.close()


if __name__ == "__main__":
    main()
