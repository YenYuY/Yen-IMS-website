import hashlib
import hmac
import json
import mimetypes
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "inventory.db"
SESSION_COOKIE = "inventory_session"
SESSION_MINUTES = 30
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "Admin@123456"

ROLE_PERMISSIONS = {
    "admin": {
        "manage_users",
        "manage_products",
        "stock_in",
        "stock_out",
        "view_reports",
    },
    "manager": {
        "manage_products",
        "stock_in",
        "stock_out",
        "view_reports",
    },
    "operator": {
        "stock_in",
        "stock_out",
        "view_reports",
    },
    "viewer": {
        "view_reports",
    },
}


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def session_expiry_iso():
    return (
        datetime.now(timezone.utc) + timedelta(minutes=SESSION_MINUTES)
    ).replace(microsecond=0).isoformat()


def hash_password(password, salt=None):
    salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120000,
    )
    return salt, derived.hex()


def verify_password(password, salt, expected_hash):
    _, candidate = hash_password(password, salt)
    return hmac.compare_digest(candidate, expected_hash)


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def init_db():
    DATA_DIR.mkdir(exist_ok=True)
    with get_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT 'pcs',
                quantity INTEGER NOT NULL DEFAULT 0,
                min_quantity INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stock_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                movement_type TEXT NOT NULL CHECK (movement_type IN ('in', 'out')),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                note TEXT NOT NULL DEFAULT '',
                reference_no TEXT NOT NULL DEFAULT '',
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            );
            """
        )

        exists = db.execute(
            "SELECT id FROM users WHERE username = ?",
            (ADMIN_USERNAME,),
        ).fetchone()
        if not exists:
            salt, password_hash = hash_password(ADMIN_PASSWORD)
            db.execute(
                """
                INSERT INTO users (username, full_name, password_salt, password_hash, role, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ADMIN_USERNAME,
                    "系統管理員",
                    salt,
                    password_hash,
                    "admin",
                    now_iso(),
                ),
            )


def row_to_user(row):
    return {
        "id": row["id"],
        "username": row["username"],
        "fullName": row["full_name"],
        "role": row["role"],
        "permissions": sorted(ROLE_PERMISSIONS.get(row["role"], [])),
        "createdAt": row["created_at"],
    }


class InventoryHandler(BaseHTTPRequestHandler):
    server_version = "InventorySystem/1.0"

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self.serve_static("index.html")
        if parsed.path.startswith("/static/"):
            return self.serve_static(parsed.path.replace("/static/", "", 1))
        if parsed.path == "/api/session":
            return self.handle_session()
        if parsed.path == "/api/users":
            return self.handle_list_users()
        if parsed.path == "/api/products":
            return self.handle_list_products()
        if parsed.path == "/api/movements":
            return self.handle_list_movements(parsed.query)
        if parsed.path == "/api/dashboard":
            return self.handle_dashboard()
        return self.send_json({"error": "Not Found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/login":
            return self.handle_login()
        if parsed.path == "/api/logout":
            return self.handle_logout()
        if parsed.path == "/api/change-password":
            return self.handle_change_password()
        if parsed.path == "/api/users":
            return self.handle_create_user()
        if parsed.path == "/api/products":
            return self.handle_create_product()
        if parsed.path == "/api/stock-in":
            return self.handle_stock_change("in")
        if parsed.path == "/api/stock-out":
            return self.handle_stock_change("out")
        return self.send_json({"error": "Not Found"}, status=404)

    def log_message(self, format_, *args):
        return

    def serve_static(self, relative_path):
        safe_path = (STATIC_DIR / relative_path).resolve()
        if not str(safe_path).startswith(str(STATIC_DIR.resolve())) or not safe_path.exists():
            return self.send_json({"error": "Not Found"}, status=404)

        content_type, _ = mimetypes.guess_type(str(safe_path))
        with open(safe_path, "rb") as file_obj:
            data = file_obj.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def parse_json_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError("JSON 格式錯誤")

    def send_json(self, payload, status=200, extra_headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def read_cookie(self, name):
        raw = self.headers.get("Cookie")
        if not raw:
            return None
        jar = cookies.SimpleCookie()
        jar.load(raw)
        morsel = jar.get(name)
        return morsel.value if morsel else None

    def get_current_user(self):
        token = self.read_cookie(SESSION_COOKIE)
        if not token:
            return None

        with get_db() as db:
            row = db.execute(
                """
                SELECT users.*
                FROM sessions
                JOIN users ON users.id = sessions.user_id
                WHERE sessions.token = ? AND sessions.expires_at > ?
                """,
                (token, now_iso()),
            ).fetchone()
            if not row:
                db.execute("DELETE FROM sessions WHERE token = ?", (token,))
                return None
            db.execute(
                "UPDATE sessions SET expires_at = ? WHERE token = ?",
                (session_expiry_iso(), token),
            )
            return row_to_user(row)

    def require_auth(self):
        user = self.get_current_user()
        if not user:
            self.send_json({"error": "請先登入"}, status=401)
            return None
        return user

    def require_permission(self, permission):
        user = self.require_auth()
        if not user:
            return None
        if permission not in ROLE_PERMISSIONS.get(user["role"], set()):
            self.send_json({"error": "目前帳號沒有操作權限"}, status=403)
            return None
        return user

    def handle_session(self):
        user = self.get_current_user()
        return self.send_json({"authenticated": bool(user), "user": user})

    def handle_login(self):
        try:
            payload = self.parse_json_body()
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        username = str(payload.get("username", "")).strip()
        password = str(payload.get("password", ""))
        if not username or not password:
            return self.send_json({"error": "請輸入帳號與密碼"}, status=400)

        with get_db() as db:
            row = db.execute(
                "SELECT * FROM users WHERE username = ?",
                (username,),
            ).fetchone()
            if not row or not verify_password(password, row["password_salt"], row["password_hash"]):
                return self.send_json({"error": "帳號或密碼錯誤"}, status=401)

            token = secrets.token_urlsafe(32)
            db.execute("DELETE FROM sessions WHERE user_id = ?", (row["id"],))
            db.execute(
                """
                INSERT INTO sessions (token, user_id, expires_at, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (token, row["id"], session_expiry_iso(), now_iso()),
            )

        header = f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax"
        return self.send_json(
            {"message": "登入成功", "user": row_to_user(row)},
            extra_headers={"Set-Cookie": header},
        )

    def handle_logout(self):
        token = self.read_cookie(SESSION_COOKIE)
        if token:
            with get_db() as db:
                db.execute("DELETE FROM sessions WHERE token = ?", (token,))
        return self.send_json(
            {"message": "已登出"},
            extra_headers={"Set-Cookie": f"{SESSION_COOKIE}=deleted; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"},
        )

    def handle_change_password(self):
        user = self.require_auth()
        if not user:
            return
        try:
            payload = self.parse_json_body()
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        old_password = str(payload.get("oldPassword", ""))
        new_password = str(payload.get("newPassword", ""))
        confirm_password = str(payload.get("confirmPassword", ""))

        if not old_password or not new_password or not confirm_password:
            return self.send_json({"error": "請完整填寫密碼欄位"}, status=400)
        if new_password != confirm_password:
            return self.send_json({"error": "新密碼與確認密碼不一致"}, status=400)
        if len(new_password) < 8:
            return self.send_json({"error": "新密碼至少需要 8 碼"}, status=400)

        with get_db() as db:
            row = db.execute(
                "SELECT password_salt, password_hash FROM users WHERE id = ?",
                (user["id"],),
            ).fetchone()
            if not row or not verify_password(old_password, row["password_salt"], row["password_hash"]):
                return self.send_json({"error": "舊密碼錯誤"}, status=401)

            salt, password_hash = hash_password(new_password)
            db.execute(
                "UPDATE users SET password_salt = ?, password_hash = ? WHERE id = ?",
                (salt, password_hash, user["id"]),
            )
            db.execute("DELETE FROM sessions WHERE user_id = ?", (user["id"],))

        return self.send_json(
            {"message": "密碼已更新，請重新登入"},
            extra_headers={"Set-Cookie": f"{SESSION_COOKIE}=deleted; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"},
        )

    def handle_list_users(self):
        user = self.require_permission("manage_users")
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT id, username, full_name, role, created_at
                FROM users
                ORDER BY id ASC
                """
            ).fetchall()
        return self.send_json({"users": [row_to_user(row) for row in rows], "currentUser": user})

    def handle_create_user(self):
        current_user = self.require_permission("manage_users")
        if not current_user:
            return
        try:
            payload = self.parse_json_body()
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        username = str(payload.get("username", "")).strip()
        full_name = str(payload.get("fullName", "")).strip()
        password = str(payload.get("password", ""))
        role = str(payload.get("role", "")).strip()

        if not username or not full_name or not password or not role:
            return self.send_json({"error": "請完整填寫使用者資料"}, status=400)
        if role not in ROLE_PERMISSIONS:
            return self.send_json({"error": "角色不存在"}, status=400)
        if len(password) < 8:
            return self.send_json({"error": "密碼至少需要 8 碼"}, status=400)

        salt, password_hash = hash_password(password)
        try:
            with get_db() as db:
                db.execute(
                    """
                    INSERT INTO users (username, full_name, password_salt, password_hash, role, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (username, full_name, salt, password_hash, role, now_iso()),
                )
        except sqlite3.IntegrityError:
            return self.send_json({"error": "帳號已存在"}, status=409)

        return self.send_json({"message": "使用者已建立"})

    def handle_list_products(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            products = db.execute(
                """
                SELECT id, sku, name, description, unit, quantity, min_quantity, created_at, updated_at
                FROM products
                ORDER BY id DESC
                """
            ).fetchall()
        return self.send_json(
            {
                "products": [
                    {
                        "id": row["id"],
                        "sku": row["sku"],
                        "name": row["name"],
                        "description": row["description"],
                        "unit": row["unit"],
                        "quantity": row["quantity"],
                        "minQuantity": row["min_quantity"],
                        "createdAt": row["created_at"],
                        "updatedAt": row["updated_at"],
                        "isLowStock": row["quantity"] <= row["min_quantity"],
                    }
                    for row in products
                ],
                "user": user,
            }
        )

    def handle_create_product(self):
        user = self.require_permission("manage_products")
        if not user:
            return
        try:
            payload = self.parse_json_body()
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        sku = str(payload.get("sku", "")).strip()
        name = str(payload.get("name", "")).strip()
        description = str(payload.get("description", "")).strip()
        unit = str(payload.get("unit", "pcs")).strip() or "pcs"
        min_quantity = payload.get("minQuantity", 0)

        try:
            min_quantity = int(min_quantity)
        except (TypeError, ValueError):
            return self.send_json({"error": "安全庫存需為整數"}, status=400)

        if not sku or not name:
            return self.send_json({"error": "請輸入商品編號與名稱"}, status=400)
        if min_quantity < 0:
            return self.send_json({"error": "安全庫存不能小於 0"}, status=400)

        timestamp = now_iso()
        try:
            with get_db() as db:
                db.execute(
                    """
                    INSERT INTO products (sku, name, description, unit, quantity, min_quantity, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (sku, name, description, unit, min_quantity, timestamp, timestamp),
                )
        except sqlite3.IntegrityError:
            return self.send_json({"error": "商品編號已存在"}, status=409)

        return self.send_json({"message": "商品已建立"})

    def handle_stock_change(self, movement_type):
        permission = "stock_in" if movement_type == "in" else "stock_out"
        user = self.require_permission(permission)
        if not user:
            return
        try:
            payload = self.parse_json_body()
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        product_id = payload.get("productId")
        quantity = payload.get("quantity")
        note = str(payload.get("note", "")).strip()
        reference_no = str(payload.get("referenceNo", "")).strip()

        try:
            product_id = int(product_id)
            quantity = int(quantity)
        except (TypeError, ValueError):
            return self.send_json({"error": "商品與數量格式錯誤"}, status=400)

        if quantity <= 0:
            return self.send_json({"error": "數量必須大於 0"}, status=400)

        with get_db() as db:
            product = db.execute(
                "SELECT id, name, quantity FROM products WHERE id = ?",
                (product_id,),
            ).fetchone()
            if not product:
                return self.send_json({"error": "找不到商品"}, status=404)

            new_quantity = product["quantity"] + quantity if movement_type == "in" else product["quantity"] - quantity
            if new_quantity < 0:
                return self.send_json({"error": "出庫後庫存不能小於 0"}, status=400)

            timestamp = now_iso()
            db.execute(
                "UPDATE products SET quantity = ?, updated_at = ? WHERE id = ?",
                (new_quantity, timestamp, product_id),
            )
            db.execute(
                """
                INSERT INTO stock_movements (
                    product_id, movement_type, quantity, note, reference_no, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (product_id, movement_type, quantity, note, reference_no, user["id"], timestamp),
            )

        movement_label = "入庫" if movement_type == "in" else "出庫"
        return self.send_json({"message": f"{movement_label}完成"})

    def handle_list_movements(self, query):
        user = self.require_auth()
        if not user:
            return
        params = parse_qs(query)
        limit = 50
        if "limit" in params:
            try:
                limit = max(1, min(200, int(params["limit"][0])))
            except ValueError:
                pass

        with get_db() as db:
            rows = db.execute(
                """
                SELECT
                    stock_movements.id,
                    stock_movements.movement_type,
                    stock_movements.quantity,
                    stock_movements.note,
                    stock_movements.reference_no,
                    stock_movements.created_at,
                    products.name AS product_name,
                    products.sku AS product_sku,
                    users.full_name AS created_by_name,
                    users.username AS created_by_username
                FROM stock_movements
                JOIN products ON products.id = stock_movements.product_id
                JOIN users ON users.id = stock_movements.created_by
                ORDER BY stock_movements.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return self.send_json(
            {
                "movements": [
                    {
                        "id": row["id"],
                        "movementType": row["movement_type"],
                        "quantity": row["quantity"],
                        "note": row["note"],
                        "referenceNo": row["reference_no"],
                        "createdAt": row["created_at"],
                        "productName": row["product_name"],
                        "productSku": row["product_sku"],
                        "createdBy": row["created_by_name"] or row["created_by_username"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_dashboard(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            product_count = db.execute("SELECT COUNT(*) AS count FROM products").fetchone()["count"]
            user_count = db.execute("SELECT COUNT(*) AS count FROM users").fetchone()["count"]
            low_stock_count = db.execute(
                "SELECT COUNT(*) AS count FROM products WHERE quantity <= min_quantity"
            ).fetchone()["count"]
            total_units = db.execute("SELECT COALESCE(SUM(quantity), 0) AS total FROM products").fetchone()["total"]
        return self.send_json(
            {
                "user": user,
                "stats": {
                    "productCount": product_count,
                    "userCount": user_count,
                    "lowStockCount": low_stock_count,
                    "totalUnits": total_units,
                },
                "roles": [
                    {"role": role, "permissions": sorted(list(permissions))}
                    for role, permissions in ROLE_PERMISSIONS.items()
                ],
                "defaultAdmin": {
                    "username": ADMIN_USERNAME,
                    "password": ADMIN_PASSWORD,
                },
            }
        )


def run():
    init_db()
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), InventoryHandler)
    print(f"Inventory system running at http://127.0.0.1:{port}")
    print(f"Default admin: {ADMIN_USERNAME} / {ADMIN_PASSWORD}")
    server.serve_forever()


if __name__ == "__main__":
    run()
