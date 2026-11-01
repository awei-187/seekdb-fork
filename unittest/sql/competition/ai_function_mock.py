# Copyright (c) 2025 OceanBase.
# Licensed under the Apache License, Version 2.0.
"""Deterministic HTTP protocols for the AI function SQL regression tests."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(port):
    events = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def send_json(self, payload, status=200):
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            with lock:
                self.send_json(list(events))

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            with lock:
                events.append({"path": self.path, "body": body})
            if "/malformed/" in self.path:
                self.send_json({"choices": [], "data": "invalid", "embeddings": {}})
            elif self.path.endswith("/api/generate"):
                if body.get("stream") is not False:
                    self.send_json({"error": "stream must be false"}, 400)
                else:
                    self.send_json({"response": "mock:" + body.get("prompt", ""), "done": True})
            elif self.path.endswith("/embeddings") or self.path.endswith("/api/embed"):
                inputs = body.get("input", [])
                if not isinstance(inputs, list):
                    inputs = [inputs]
                dimension = int(body.get("dimensions", 768))
                vectors = [[float(value[3:]) + 1 if isinstance(value, str) and value.startswith("row")
                            and value[3:].isdigit() else float(i + 1)] + [0.0] * (dimension - 1)
                           for i, value in enumerate(inputs)]
                if self.path.endswith("/api/embed"):
                    self.send_json({"embeddings": vectors})
                else:
                    # A valid response need not use input order.
                    self.send_json({"data": [{"index": i, "embedding": vectors[i]}
                                             for i in reversed(range(len(vectors)))]})
            elif self.path.endswith("/rerank"):
                documents = body.get("documents", [])
                self.send_json({"results": [{"index": i, "relevance_score":
                                              1.0 if "apple" in documents[i] else 0.0}
                                             for i in reversed(range(len(documents)))]})
            else:
                messages = body.get("messages", [])
                prompt = next((m.get("content", "") for m in reversed(messages)
                               if m.get("role") == "user"), "")
                self.send_json({"choices": [{"message": {"content": "mock:" + prompt}}]})

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=19130)
    serve(parser.parse_args().port)
