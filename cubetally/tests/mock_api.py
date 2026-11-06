"""A local stand-in for the CubeTally public REST API.

It copies the behavior the connector depends on: bearer keys, /whoami, cursor
pages of {data, nextCursor}, 429 with Retry-After, the monthly allowance error
and the invalid cursor 400. It holds invented records only and answers on
127.0.0.1 only.

Run it for `fivetran debug`:  python tests/mock_api.py 8931
"""

import base64
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

GOOD_KEY = "ct_test_mockkey"
ALL_SCOPES = [
    "products:read",
    "customers:read",
    "suppliers:read",
    "warehouses:read",
    "vehicles:read",
    "purchaseOrders:read",
    "workOrders:read",
    "invoices:read",
    "estimates:read",
]


def sample_data():
    old = "2026-01-05T10:00:00.000Z"
    new = "2026-10-29T10:00:00.000Z"
    line = [{"id": "l1", "description": "Spring", "quantity": 2, "unitPrice": 40, "total": 80}]
    return {
        "products": [
            {
                "id": "p1",
                "name": "Spring",
                "sku": "SP-1",
                "price": 40,
                "quantity": 7,
                "createdAt": old,
                "updatedAt": old,
            },
            {
                "id": "p2",
                "name": "Opener",
                "sku": "OP-1",
                "price": 300,
                "quantity": 2,
                "createdAt": old,
                "updatedAt": new,
            },
            {"id": "p3", "name": "No date", "sku": "ND-1"},
        ],
        "customers": [
            {
                "id": "c1",
                "name": "Acme",
                "email": "ap@example.com",
                "totalSpent": 120.5,
                "createdAt": old,
            },
        ],
        "suppliers": [{"id": "s1", "name": "Door Supply", "createdAt": old}],
        "warehouses": [{"id": "w1", "name": "Main", "isDefault": True, "createdAt": old}],
        "vehicles": [
            {"id": "v1", "name": "Van 1", "inventory": [{"productId": "p1", "quantity": 1}]},
        ],
        "purchase-orders": [
            {
                "id": "po1",
                "poNumber": "PO-1",
                "total": 80,
                "lineItems": line,
                "createdAt": old,
                "updatedAt": old,
            },
        ],
        "work-orders": [
            {
                "id": "wo1",
                "title": "Fix door",
                "status": "completed",
                "total": 80,
                "lineItems": line,
                "completedAt": new,
                "createdAt": old,
                "updatedAt": new,
            },
        ],
        "invoices": [
            {
                "id": "i1",
                "invoiceNumber": "INV-1",
                "total": 80,
                "lineItems": line,
                "dueDate": "2026-11-15",
                "createdAt": old,
                "updatedAt": new,
            },
        ],
        "estimates": [
            {
                "id": "e1",
                "estimateNumber": "EST-1",
                "total": 80,
                "lineItems": line,
                "createdAt": old,
                "updatedAt": old,
            },
        ],
        "stock-movements": [
            {"id": "m1", "productId": "p1", "type": "out", "quantity": 1, "createdAt": new},
            {"id": "m2", "productId": "p1", "type": "in", "quantity": 5, "createdAt": old},
        ],
        "transfers": [
            {"id": "t1", "vehicleId": "v1", "productId": "p1", "quantity": 1, "createdAt": old},
        ],
    }


class MockApi:
    def __init__(self):
        self.data = sample_data()
        self.scopes = list(ALL_SCOPES)
        self.requests = []
        self.methods = []
        # Each item is applied to the next list request:
        # {"status": int, "body": dict, "headers": dict}
        self.faults = []
        self.page_size_cap = 100
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def respond(self, status, body, headers=None):
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                api.methods.append("POST")
                self.respond(405, {"error": "method_not_allowed", "message": "Read only mock."})

            def do_GET(self):
                api.methods.append("GET")
                parts = urlsplit(self.path)
                query = parse_qs(parts.query)
                path = parts.path.removeprefix("/v1/")
                api.requests.append({"path": path, "query": query})
                if self.headers.get("Authorization") != "Bearer " + GOOD_KEY:
                    self.respond(401, {"error": "invalid_key", "message": "Bad key."})
                    return
                if path == "whoami":
                    self.respond(
                        200,
                        {
                            "organizationId": "org_mock",
                            "scopes": api.scopes,
                            "environment": "test",
                            "writesEnabled": False,
                        },
                    )
                    return
                if api.faults:
                    fault = api.faults.pop(0)
                    self.respond(fault["status"], fault["body"], fault.get("headers"))
                    return
                if path not in api.data:
                    self.respond(404, {"error": "not_found", "message": "No route."})
                    return
                self.send_page(path, query)

            def send_page(self, path, query):
                rows = api.data[path]
                limit = min(int(query.get("limit", ["50"])[0]), api.page_size_cap)
                start = 0
                cursor = query.get("cursor", [None])[0]
                if cursor:
                    anchor = base64.urlsafe_b64decode(cursor + "===").decode()
                    ids = [row["id"] for row in rows]
                    if anchor not in ids:
                        self.respond(
                            400, {"error": "invalid_request", "message": "Invalid cursor."}
                        )
                        return
                    start = ids.index(anchor) + 1
                page = rows[start : start + limit]
                more = start + limit < len(rows)
                next_cursor = None
                if more and page:
                    raw = page[-1]["id"].encode()
                    next_cursor = base64.urlsafe_b64encode(raw).decode().rstrip("=")
                self.respond(200, {"data": page, "nextCursor": next_cursor})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self):
        return "http://127.0.0.1:" + str(self.server.server_address[1]) + "/v1"

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def list_requests(self, path):
        return [item for item in self.requests if item["path"] == path]


if __name__ == "__main__":
    mock = MockApi()
    mock.server.server_close()
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8931
    mock.server = ThreadingHTTPServer(("127.0.0.1", port), mock.server.RequestHandlerClass)
    print("Mock CubeTally API on http://127.0.0.1:" + str(port) + "/v1", flush=True)
    mock.server.serve_forever()
