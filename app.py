import csv
import hashlib
import hmac
import io
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

ALL_PERMISSIONS = {
    "manage_users",
    "reset_passwords",
    "manage_products",
    "delete_products",
    "manage_partners",
    "manage_warehouses",
    "manage_documents",
    "complete_documents",
    "manage_stocktakes",
    "manage_manual_stock",
    "stock_in",
    "stock_out",
    "view_reports",
    "export_reports",
    "view_audit",
}

ROLE_PERMISSIONS = {
    "admin": set(ALL_PERMISSIONS),
    "manager": {
        "manage_products",
        "manage_partners",
        "manage_warehouses",
        "manage_documents",
        "complete_documents",
        "manage_stocktakes",
        "manage_manual_stock",
        "stock_in",
        "stock_out",
        "view_reports",
        "export_reports",
        "view_audit",
    },
    "operator": {
        "manage_documents",
        "complete_documents",
        "manage_stocktakes",
        "manage_manual_stock",
        "stock_in",
        "stock_out",
        "view_reports",
    },
    "viewer": {
        "view_reports",
    },
}

PARTNER_TYPES = {"supplier", "customer"}
DOCUMENT_TYPES = {"purchase", "sales", "return", "stocktake"}
DOCUMENT_STATUSES = {"draft", "completed", "cancelled"}


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


def json_loads_or_default(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def json_dumps(value):
    return json.dumps(value, ensure_ascii=False)


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_permission_list(value):
    if isinstance(value, list):
        permissions = [str(item).strip() for item in value if str(item).strip()]
    elif isinstance(value, str):
        permissions = json_loads_or_default(value, [])
    else:
        permissions = []
    return sorted({item for item in permissions if item in ALL_PERMISSIONS})


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def table_columns(db, table_name):
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()}


def ensure_columns(db, table_name, column_map):
    existing = table_columns(db, table_name)
    for column_name, sql in column_map.items():
        if column_name not in existing:
            db.execute(f"ALTER TABLE {table_name} ADD COLUMN {sql}")


def compute_permissions(role, extra_permissions):
    return sorted(ROLE_PERMISSIONS.get(role, set()) | set(extra_permissions))


def row_to_user(row):
    extra_permissions = parse_permission_list(row["extra_permissions"])
    return {
        "id": row["id"],
        "username": row["username"],
        "fullName": row["full_name"],
        "role": row["role"],
        "permissions": compute_permissions(row["role"], extra_permissions),
        "extraPermissions": extra_permissions,
        "status": row["status"],
        "mustResetPassword": bool(row["must_reset_password"]),
        "createdAt": row["created_at"],
    }


def ensure_default_warehouse_location(db):
    warehouse = db.execute(
        "SELECT id FROM warehouses WHERE code = ?",
        ("MAIN",),
    ).fetchone()
    if not warehouse:
        db.execute(
            """
            INSERT INTO warehouses (code, name, address, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("MAIN", "主倉", "", "active", now_iso()),
        )
        warehouse = db.execute(
            "SELECT id FROM warehouses WHERE code = ?",
            ("MAIN",),
        ).fetchone()

    location = db.execute(
        "SELECT id FROM locations WHERE warehouse_id = ? AND code = ?",
        (warehouse["id"], "DEFAULT"),
    ).fetchone()
    if not location:
        db.execute(
            """
            INSERT INTO locations (warehouse_id, code, name, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (warehouse["id"], "DEFAULT", "預設儲位", "active", now_iso()),
        )
        location = db.execute(
            "SELECT id FROM locations WHERE warehouse_id = ? AND code = ?",
            (warehouse["id"], "DEFAULT"),
        ).fetchone()
    return warehouse["id"], location["id"]


def recalculate_product_quantity(db, product_id):
    total = db.execute(
        "SELECT COALESCE(SUM(quantity), 0) AS total FROM stock_levels WHERE product_id = ?",
        (product_id,),
    ).fetchone()["total"]
    db.execute(
        "UPDATE products SET quantity = ?, updated_at = ? WHERE id = ?",
        (total, now_iso(), product_id),
    )


def migrate_existing_stock(db):
    default_warehouse_id, default_location_id = ensure_default_warehouse_location(db)
    products = db.execute(
        "SELECT id, quantity FROM products WHERE deleted_at IS NULL"
    ).fetchall()
    for product in products:
        has_level = db.execute(
            "SELECT id FROM stock_levels WHERE product_id = ? LIMIT 1",
            (product["id"],),
        ).fetchone()
        if has_level:
            continue
        if product["quantity"] <= 0:
            continue
        db.execute(
            """
            INSERT INTO stock_levels (
                product_id, warehouse_id, location_id, batch_no, serial_no, expiry_date, quantity, updated_at
            ) VALUES (?, ?, ?, '', '', '', ?, ?)
            """,
            (
                product["id"],
                default_warehouse_id,
                default_location_id,
                product["quantity"],
                now_iso(),
            ),
        )


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

            CREATE TABLE IF NOT EXISTS business_partners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_type TEXT NOT NULL,
                name TEXT NOT NULL,
                contact_name TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                tax_id TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS warehouses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                address TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                warehouse_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
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

            CREATE TABLE IF NOT EXISTS stock_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                warehouse_id INTEGER NOT NULL,
                location_id INTEGER NOT NULL,
                batch_no TEXT NOT NULL DEFAULT '',
                serial_no TEXT NOT NULL DEFAULT '',
                expiry_date TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                FOREIGN KEY (location_id) REFERENCES locations(id)
            );

            CREATE TABLE IF NOT EXISTS inventory_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                warehouse_id INTEGER NOT NULL,
                location_id INTEGER NOT NULL,
                transaction_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                reference_no TEXT NOT NULL DEFAULT '',
                batch_no TEXT NOT NULL DEFAULT '',
                serial_no TEXT NOT NULL DEFAULT '',
                expiry_date TEXT NOT NULL DEFAULT '',
                partner_id INTEGER,
                document_id INTEGER,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                FOREIGN KEY (location_id) REFERENCES locations(id),
                FOREIGN KEY (partner_id) REFERENCES business_partners(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_type TEXT NOT NULL,
                doc_no TEXT NOT NULL UNIQUE,
                partner_id INTEGER,
                warehouse_id INTEGER NOT NULL,
                location_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                unit_price REAL NOT NULL DEFAULT 0,
                counted_quantity INTEGER NOT NULL DEFAULT 0,
                batch_no TEXT NOT NULL DEFAULT '',
                serial_no TEXT NOT NULL DEFAULT '',
                expiry_date TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                note TEXT NOT NULL DEFAULT '',
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (partner_id) REFERENCES business_partners(id),
                FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                FOREIGN KEY (location_id) REFERENCES locations(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL DEFAULT 0,
                detail TEXT NOT NULL DEFAULT '',
                created_by INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY (created_by) REFERENCES users(id)
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

        ensure_columns(
            db,
            "users",
            {
                "extra_permissions": "extra_permissions TEXT NOT NULL DEFAULT '[]'",
                "status": "status TEXT NOT NULL DEFAULT 'active'",
                "must_reset_password": "must_reset_password INTEGER NOT NULL DEFAULT 0",
            },
        )
        ensure_columns(
            db,
            "products",
            {
                "barcode": "barcode TEXT NOT NULL DEFAULT ''",
                "qr_code": "qr_code TEXT NOT NULL DEFAULT ''",
                "supplier_id": "supplier_id INTEGER REFERENCES business_partners(id)",
                "active": "active INTEGER NOT NULL DEFAULT 1",
                "deleted_at": "deleted_at TEXT NOT NULL DEFAULT ''",
                "track_batch": "track_batch INTEGER NOT NULL DEFAULT 0",
                "track_serial": "track_serial INTEGER NOT NULL DEFAULT 0",
                "track_expiry": "track_expiry INTEGER NOT NULL DEFAULT 0",
            },
        )

        exists = db.execute(
            "SELECT id FROM users WHERE username = ?",
            (ADMIN_USERNAME,),
        ).fetchone()
        if not exists:
            salt, password_hash = hash_password(ADMIN_PASSWORD)
            db.execute(
                """
                INSERT INTO users (
                    username, full_name, password_salt, password_hash, role,
                    created_at, extra_permissions, status, must_reset_password
                )
                VALUES (?, ?, ?, ?, ?, ?, '[]', 'active', 0)
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

        ensure_default_warehouse_location(db)
        migrate_existing_stock(db)

        product_ids = db.execute("SELECT id FROM products").fetchall()
        for product in product_ids:
            recalculate_product_quantity(db, product["id"])


def require_text(payload, key, label):
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ValueError(f"請輸入{label}")
    return value


def log_audit(db, action, entity_type, entity_id, detail, user_id):
    db.execute(
        """
        INSERT INTO audit_logs (action, entity_type, entity_id, detail, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (action, entity_type, entity_id, detail, user_id, now_iso()),
    )


def generate_document_no(db, doc_type):
    prefix_map = {
        "purchase": "PO",
        "sales": "SO",
        "return": "RT",
        "stocktake": "ST",
    }
    prefix = prefix_map.get(doc_type, "DOC")
    date_part = datetime.now().strftime("%Y%m%d")
    count = db.execute(
        "SELECT COUNT(*) AS count FROM documents WHERE doc_type = ? AND substr(created_at, 1, 10) = ?",
        (doc_type, datetime.now(timezone.utc).date().isoformat()),
    ).fetchone()["count"]
    return f"{prefix}-{date_part}-{count + 1:03d}"


def get_current_stock(db, product_id, warehouse_id, location_id, batch_no="", serial_no="", expiry_date=""):
    query = """
        SELECT COALESCE(SUM(quantity), 0) AS total
        FROM stock_levels
        WHERE product_id = ? AND warehouse_id = ? AND location_id = ?
    """
    params = [product_id, warehouse_id, location_id]
    if batch_no:
        query += " AND batch_no = ?"
        params.append(batch_no)
    if serial_no:
        query += " AND serial_no = ?"
        params.append(serial_no)
    if expiry_date:
        query += " AND expiry_date = ?"
        params.append(expiry_date)
    return db.execute(query, params).fetchone()["total"]


def apply_stock_transaction(
    db,
    *,
    product_id,
    warehouse_id,
    location_id,
    direction,
    quantity,
    transaction_type,
    note,
    reference_no,
    user_id,
    batch_no="",
    serial_no="",
    expiry_date="",
    partner_id=None,
    document_id=None,
):
    if quantity <= 0:
        raise ValueError("數量必須大於 0")

    timestamp = now_iso()

    if direction == "in":
        row = db.execute(
            """
            SELECT id, quantity FROM stock_levels
            WHERE product_id = ? AND warehouse_id = ? AND location_id = ?
              AND batch_no = ? AND serial_no = ? AND expiry_date = ?
            """,
            (product_id, warehouse_id, location_id, batch_no, serial_no, expiry_date),
        ).fetchone()
        if row:
            db.execute(
                "UPDATE stock_levels SET quantity = ?, updated_at = ? WHERE id = ?",
                (row["quantity"] + quantity, timestamp, row["id"]),
            )
        else:
            db.execute(
                """
                INSERT INTO stock_levels (
                    product_id, warehouse_id, location_id, batch_no, serial_no, expiry_date, quantity, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_id,
                    warehouse_id,
                    location_id,
                    batch_no,
                    serial_no,
                    expiry_date,
                    quantity,
                    timestamp,
                ),
            )
    else:
        candidates = db.execute(
            """
            SELECT id, quantity
            FROM stock_levels
            WHERE product_id = ? AND warehouse_id = ? AND location_id = ?
              AND (? = '' OR batch_no = ?)
              AND (? = '' OR serial_no = ?)
              AND (? = '' OR expiry_date = ?)
            ORDER BY
              CASE WHEN expiry_date = '' THEN 1 ELSE 0 END,
              expiry_date ASC,
              id ASC
            """,
            (
                product_id,
                warehouse_id,
                location_id,
                batch_no,
                batch_no,
                serial_no,
                serial_no,
                expiry_date,
                expiry_date,
            ),
        ).fetchall()
        remaining = quantity
        for candidate in candidates:
            if remaining <= 0:
                break
            available = candidate["quantity"]
            if available <= 0:
                continue
            consume = min(available, remaining)
            new_quantity = available - consume
            db.execute(
                "UPDATE stock_levels SET quantity = ?, updated_at = ? WHERE id = ?",
                (new_quantity, timestamp, candidate["id"]),
            )
            remaining -= consume
        if remaining > 0:
            raise ValueError("庫存不足，無法完成此操作")
        db.execute("DELETE FROM stock_levels WHERE quantity <= 0")

    db.execute(
        """
        INSERT INTO inventory_transactions (
            product_id, warehouse_id, location_id, transaction_type, direction, quantity,
            note, reference_no, batch_no, serial_no, expiry_date, partner_id, document_id,
            created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            product_id,
            warehouse_id,
            location_id,
            transaction_type,
            direction,
            quantity,
            note,
            reference_no,
            batch_no,
            serial_no,
            expiry_date,
            partner_id,
            document_id,
            user_id,
            timestamp,
        ),
    )
    recalculate_product_quantity(db, product_id)


class InventoryHandler(BaseHTTPRequestHandler):
    server_version = "InventorySystem/2.0"

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self.serve_static("index.html")
        if parsed.path.startswith("/static/"):
            return self.serve_static(parsed.path.replace("/static/", "", 1))

        routes = {
            "/api/session": self.handle_session,
            "/api/dashboard": self.handle_dashboard,
            "/api/users": self.handle_list_users,
            "/api/products": self.handle_list_products,
            "/api/partners": self.handle_list_partners,
            "/api/warehouses": self.handle_list_warehouses,
            "/api/locations": self.handle_list_locations,
            "/api/stock-levels": self.handle_list_stock_levels,
            "/api/movements": lambda: self.handle_list_movements(parsed.query),
            "/api/documents": self.handle_list_documents,
            "/api/audit-logs": self.handle_list_audit_logs,
            "/api/export": lambda: self.handle_export(parsed.query),
        }
        handler = routes.get(parsed.path)
        if handler:
            return handler()
        return self.send_json({"error": "Not Found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        routes = {
            "/api/login": self.handle_login,
            "/api/logout": self.handle_logout,
            "/api/change-password": self.handle_change_password,
            "/api/users": self.handle_upsert_user,
            "/api/users/reset-password": self.handle_reset_user_password,
            "/api/products": self.handle_upsert_product,
            "/api/products/toggle-status": self.handle_toggle_product_status,
            "/api/products/delete": self.handle_delete_product,
            "/api/partners": self.handle_upsert_partner,
            "/api/partners/toggle-status": self.handle_toggle_partner_status,
            "/api/warehouses": self.handle_upsert_warehouse,
            "/api/warehouses/toggle-status": self.handle_toggle_warehouse_status,
            "/api/locations": self.handle_upsert_location,
            "/api/locations/toggle-status": self.handle_toggle_location_status,
            "/api/manual-movement": self.handle_manual_movement,
            "/api/documents": self.handle_create_document,
            "/api/documents/complete": self.handle_complete_document,
            "/api/stocktakes/adjust": self.handle_stocktake_adjustment,
            "/api/stock-in": self.handle_legacy_stock_in,
            "/api/stock-out": self.handle_legacy_stock_out,
        }
        handler = routes.get(parsed.path)
        if handler:
            return handler()
        return self.send_json({"error": "Not Found"}, status=404)

    def log_message(self, format_, *args):
        return

    def serve_static(self, relative_path):
        safe_path = (STATIC_DIR / relative_path).resolve()
        if not str(safe_path).startswith(str(STATIC_DIR.resolve())) or not safe_path.exists():
            return self.send_json({"error": "Not Found"}, status=404)

        content_type, _ = mimetypes.guess_type(str(safe_path))
        data = safe_path.read_bytes()
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

    def send_csv(self, filename, rows, headers):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
        data = output.getvalue().encode("utf-8-sig")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(data)

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
            if not row or row["status"] != "active":
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
        if permission not in set(user["permissions"]):
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
            if not row or row["status"] != "active":
                return self.send_json({"error": "帳號或密碼錯誤"}, status=401)
            if not verify_password(password, row["password_salt"], row["password_hash"]):
                return self.send_json({"error": "帳號或密碼錯誤"}, status=401)

            token = secrets.token_urlsafe(32)
            db.execute("DELETE FROM sessions WHERE user_id = ?", (row["id"],))
            db.execute(
                "INSERT INTO sessions (token, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
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
                """
                UPDATE users
                SET password_salt = ?, password_hash = ?, must_reset_password = 0
                WHERE id = ?
                """,
                (salt, password_hash, user["id"]),
            )
            db.execute("DELETE FROM sessions WHERE user_id = ?", (user["id"],))
            log_audit(db, "change_password", "user", user["id"], "使用者自行修改密碼", user["id"])

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
                "SELECT * FROM users ORDER BY id ASC"
            ).fetchall()
        return self.send_json(
            {
                "users": [row_to_user(row) for row in rows],
                "permissionCatalog": sorted(ALL_PERMISSIONS),
            }
        )

    def handle_upsert_user(self):
        current_user = self.require_permission("manage_users")
        if not current_user:
            return
        try:
            payload = self.parse_json_body()
            username = require_text(payload, "username", "帳號")
            full_name = require_text(payload, "fullName", "姓名")
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        role = str(payload.get("role", "")).strip()
        status = "active" if str(payload.get("status", "active")).strip() != "inactive" else "inactive"
        password = str(payload.get("password", ""))
        user_id = payload.get("id")
        extra_permissions = parse_permission_list(payload.get("extraPermissions", []))

        if role not in ROLE_PERMISSIONS:
            return self.send_json({"error": "角色不存在"}, status=400)
        if user_id in (None, "") and len(password) < 8:
            return self.send_json({"error": "新使用者密碼至少需要 8 碼"}, status=400)

        try:
            with get_db() as db:
                if user_id in (None, ""):
                    salt, password_hash = hash_password(password)
                    cursor = db.execute(
                        """
                        INSERT INTO users (
                            username, full_name, password_salt, password_hash, role, created_at,
                            extra_permissions, status, must_reset_password
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                        """,
                        (
                            username,
                            full_name,
                            salt,
                            password_hash,
                            role,
                            now_iso(),
                            json_dumps(extra_permissions),
                            status,
                        ),
                    )
                    log_audit(db, "create", "user", cursor.lastrowid, f"建立帳號 {username}", current_user["id"])
                else:
                    params = [
                        username,
                        full_name,
                        role,
                        json_dumps(extra_permissions),
                        status,
                        int(user_id),
                    ]
                    db.execute(
                        """
                        UPDATE users
                        SET username = ?, full_name = ?, role = ?, extra_permissions = ?, status = ?
                        WHERE id = ?
                        """,
                        params,
                    )
                    if password:
                        if len(password) < 8:
                            return self.send_json({"error": "密碼至少需要 8 碼"}, status=400)
                        salt, password_hash = hash_password(password)
                        db.execute(
                            "UPDATE users SET password_salt = ?, password_hash = ? WHERE id = ?",
                            (salt, password_hash, int(user_id)),
                        )
                    log_audit(db, "update", "user", int(user_id), f"更新帳號 {username}", current_user["id"])
        except sqlite3.IntegrityError:
            return self.send_json({"error": "帳號已存在"}, status=409)
        return self.send_json({"message": "使用者資料已儲存"})

    def handle_reset_user_password(self):
        current_user = self.require_permission("reset_passwords")
        if not current_user:
            return
        try:
            payload = self.parse_json_body()
            user_id = int(payload.get("userId"))
            temp_password = require_text(payload, "tempPassword", "暫時密碼")
        except (TypeError, ValueError):
            return self.send_json({"error": "重設資料格式錯誤"}, status=400)

        if len(temp_password) < 8:
            return self.send_json({"error": "暫時密碼至少需要 8 碼"}, status=400)

        salt, password_hash = hash_password(temp_password)
        with get_db() as db:
            row = db.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
            if not row:
                return self.send_json({"error": "找不到使用者"}, status=404)
            db.execute(
                """
                UPDATE users
                SET password_salt = ?, password_hash = ?, must_reset_password = 1
                WHERE id = ?
                """,
                (salt, password_hash, user_id),
            )
            db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            log_audit(db, "reset_password", "user", user_id, f"管理員重設 {row['username']} 密碼", current_user["id"])
        return self.send_json({"message": "使用者密碼已重設，對方下次登入需修改密碼"})

    def handle_list_products(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT
                    products.*,
                    business_partners.name AS supplier_name
                FROM products
                LEFT JOIN business_partners ON business_partners.id = products.supplier_id
                WHERE products.deleted_at = ''
                ORDER BY products.id DESC
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
                        "barcode": row["barcode"],
                        "qrCode": row["qr_code"],
                        "supplierId": row["supplier_id"],
                        "supplierName": row["supplier_name"] or "",
                        "active": bool(row["active"]),
                        "trackBatch": bool(row["track_batch"]),
                        "trackSerial": bool(row["track_serial"]),
                        "trackExpiry": bool(row["track_expiry"]),
                        "createdAt": row["created_at"],
                        "updatedAt": row["updated_at"],
                        "isLowStock": row["quantity"] <= row["min_quantity"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_upsert_product(self):
        user = self.require_permission("manage_products")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            sku = require_text(payload, "sku", "商品編號")
            name = require_text(payload, "name", "商品名稱")
            min_quantity = int(payload.get("minQuantity", 0))
        except (TypeError, ValueError):
            return self.send_json({"error": "商品資料格式錯誤"}, status=400)
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        product_id = payload.get("id")
        description = str(payload.get("description", "")).strip()
        unit = str(payload.get("unit", "pcs")).strip() or "pcs"
        supplier_id = payload.get("supplierId") or None
        barcode = str(payload.get("barcode", "")).strip()
        qr_code = str(payload.get("qrCode", "")).strip()
        active = 1 if parse_bool(payload.get("active", True)) else 0
        track_batch = 1 if parse_bool(payload.get("trackBatch", False)) else 0
        track_serial = 1 if parse_bool(payload.get("trackSerial", False)) else 0
        track_expiry = 1 if parse_bool(payload.get("trackExpiry", False)) else 0

        if min_quantity < 0:
            return self.send_json({"error": "安全庫存不能小於 0"}, status=400)

        try:
            with get_db() as db:
                if product_id in (None, ""):
                    cursor = db.execute(
                        """
                        INSERT INTO products (
                            sku, name, description, unit, quantity, min_quantity, created_at, updated_at,
                            barcode, qr_code, supplier_id, active, deleted_at, track_batch, track_serial, track_expiry
                        ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)
                        """,
                        (
                            sku,
                            name,
                            description,
                            unit,
                            min_quantity,
                            now_iso(),
                            now_iso(),
                            barcode,
                            qr_code,
                            supplier_id,
                            active,
                            track_batch,
                            track_serial,
                            track_expiry,
                        ),
                    )
                    log_audit(db, "create", "product", cursor.lastrowid, f"建立商品 {sku}", user["id"])
                else:
                    db.execute(
                        """
                        UPDATE products
                        SET sku = ?, name = ?, description = ?, unit = ?, min_quantity = ?, updated_at = ?,
                            barcode = ?, qr_code = ?, supplier_id = ?, active = ?,
                            track_batch = ?, track_serial = ?, track_expiry = ?
                        WHERE id = ? AND deleted_at = ''
                        """,
                        (
                            sku,
                            name,
                            description,
                            unit,
                            min_quantity,
                            now_iso(),
                            barcode,
                            qr_code,
                            supplier_id,
                            active,
                            track_batch,
                            track_serial,
                            track_expiry,
                            int(product_id),
                        ),
                    )
                    log_audit(db, "update", "product", int(product_id), f"更新商品 {sku}", user["id"])
        except sqlite3.IntegrityError:
            return self.send_json({"error": "商品編號已存在"}, status=409)
        return self.send_json({"message": "商品資料已儲存"})

    def handle_toggle_product_status(self):
        user = self.require_permission("manage_products")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            product_id = int(payload.get("productId"))
            active = 1 if parse_bool(payload.get("active", True)) else 0
        except (TypeError, ValueError):
            return self.send_json({"error": "商品狀態資料錯誤"}, status=400)

        with get_db() as db:
            db.execute(
                "UPDATE products SET active = ?, updated_at = ? WHERE id = ? AND deleted_at = ''",
                (active, now_iso(), product_id),
            )
            log_audit(db, "toggle_status", "product", product_id, f"商品狀態調整為 {active}", user["id"])
        return self.send_json({"message": "商品狀態已更新"})

    def handle_delete_product(self):
        user = self.require_permission("delete_products")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            product_id = int(payload.get("productId"))
        except (TypeError, ValueError):
            return self.send_json({"error": "商品刪除資料錯誤"}, status=400)

        with get_db() as db:
            db.execute(
                "UPDATE products SET deleted_at = ?, active = 0, updated_at = ? WHERE id = ?",
                (now_iso(), now_iso(), product_id),
            )
            log_audit(db, "delete", "product", product_id, "軟刪除商品", user["id"])
        return self.send_json({"message": "商品已刪除"})

    def handle_list_partners(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                "SELECT * FROM business_partners ORDER BY id DESC"
            ).fetchall()
        return self.send_json(
            {
                "partners": [
                    {
                        "id": row["id"],
                        "partnerType": row["partner_type"],
                        "name": row["name"],
                        "contactName": row["contact_name"],
                        "phone": row["phone"],
                        "email": row["email"],
                        "taxId": row["tax_id"],
                        "address": row["address"],
                        "status": row["status"],
                        "createdAt": row["created_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_upsert_partner(self):
        user = self.require_permission("manage_partners")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            partner_type = require_text(payload, "partnerType", "對象類型")
            name = require_text(payload, "name", "名稱")
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)
        if partner_type not in PARTNER_TYPES:
            return self.send_json({"error": "對象類型不存在"}, status=400)

        partner_id = payload.get("id")
        values = (
            partner_type,
            name,
            str(payload.get("contactName", "")).strip(),
            str(payload.get("phone", "")).strip(),
            str(payload.get("email", "")).strip(),
            str(payload.get("taxId", "")).strip(),
            str(payload.get("address", "")).strip(),
            str(payload.get("status", "active")).strip() or "active",
        )
        with get_db() as db:
            if partner_id in (None, ""):
                cursor = db.execute(
                    """
                    INSERT INTO business_partners (
                        partner_type, name, contact_name, phone, email, tax_id, address, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values + (now_iso(),),
                )
                log_audit(db, "create", "partner", cursor.lastrowid, f"建立對象 {name}", user["id"])
            else:
                db.execute(
                    """
                    UPDATE business_partners
                    SET partner_type = ?, name = ?, contact_name = ?, phone = ?, email = ?,
                        tax_id = ?, address = ?, status = ?
                    WHERE id = ?
                    """,
                    values + (int(partner_id),),
                )
                log_audit(db, "update", "partner", int(partner_id), f"更新對象 {name}", user["id"])
        return self.send_json({"message": "往來對象資料已儲存"})

    def handle_toggle_partner_status(self):
        user = self.require_permission("manage_partners")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            partner_id = int(payload.get("partnerId"))
            status = str(payload.get("status", "active")).strip()
        except (TypeError, ValueError):
            return self.send_json({"error": "對象狀態資料錯誤"}, status=400)
        with get_db() as db:
            db.execute("UPDATE business_partners SET status = ? WHERE id = ?", (status, partner_id))
            log_audit(db, "toggle_status", "partner", partner_id, f"對象狀態調整為 {status}", user["id"])
        return self.send_json({"message": "往來對象狀態已更新"})

    def handle_list_warehouses(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute("SELECT * FROM warehouses ORDER BY id ASC").fetchall()
        return self.send_json(
            {
                "warehouses": [
                    {
                        "id": row["id"],
                        "code": row["code"],
                        "name": row["name"],
                        "address": row["address"],
                        "status": row["status"],
                        "createdAt": row["created_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_upsert_warehouse(self):
        user = self.require_permission("manage_warehouses")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            code = require_text(payload, "code", "倉庫代碼")
            name = require_text(payload, "name", "倉庫名稱")
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        warehouse_id = payload.get("id")
        address = str(payload.get("address", "")).strip()
        status = str(payload.get("status", "active")).strip() or "active"
        try:
            with get_db() as db:
                if warehouse_id in (None, ""):
                    cursor = db.execute(
                        """
                        INSERT INTO warehouses (code, name, address, status, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (code, name, address, status, now_iso()),
                    )
                    log_audit(db, "create", "warehouse", cursor.lastrowid, f"建立倉庫 {code}", user["id"])
                else:
                    db.execute(
                        "UPDATE warehouses SET code = ?, name = ?, address = ?, status = ? WHERE id = ?",
                        (code, name, address, status, int(warehouse_id)),
                    )
                    log_audit(db, "update", "warehouse", int(warehouse_id), f"更新倉庫 {code}", user["id"])
        except sqlite3.IntegrityError:
            return self.send_json({"error": "倉庫代碼已存在"}, status=409)
        return self.send_json({"message": "倉庫資料已儲存"})

    def handle_toggle_warehouse_status(self):
        user = self.require_permission("manage_warehouses")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            warehouse_id = int(payload.get("warehouseId"))
            status = str(payload.get("status", "active")).strip()
        except (TypeError, ValueError):
            return self.send_json({"error": "倉庫狀態資料錯誤"}, status=400)
        with get_db() as db:
            db.execute("UPDATE warehouses SET status = ? WHERE id = ?", (status, warehouse_id))
            log_audit(db, "toggle_status", "warehouse", warehouse_id, f"倉庫狀態調整為 {status}", user["id"])
        return self.send_json({"message": "倉庫狀態已更新"})

    def handle_list_locations(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT locations.*, warehouses.name AS warehouse_name, warehouses.code AS warehouse_code
                FROM locations
                JOIN warehouses ON warehouses.id = locations.warehouse_id
                ORDER BY warehouses.id ASC, locations.id ASC
                """
            ).fetchall()
        return self.send_json(
            {
                "locations": [
                    {
                        "id": row["id"],
                        "warehouseId": row["warehouse_id"],
                        "warehouseName": row["warehouse_name"],
                        "warehouseCode": row["warehouse_code"],
                        "code": row["code"],
                        "name": row["name"],
                        "status": row["status"],
                        "createdAt": row["created_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_upsert_location(self):
        user = self.require_permission("manage_warehouses")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            warehouse_id = int(payload.get("warehouseId"))
            code = require_text(payload, "code", "儲位代碼")
            name = require_text(payload, "name", "儲位名稱")
        except (TypeError, ValueError):
            return self.send_json({"error": "儲位資料格式錯誤"}, status=400)
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        location_id = payload.get("id")
        status = str(payload.get("status", "active")).strip() or "active"
        with get_db() as db:
            if location_id in (None, ""):
                cursor = db.execute(
                    """
                    INSERT INTO locations (warehouse_id, code, name, status, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (warehouse_id, code, name, status, now_iso()),
                )
                log_audit(db, "create", "location", cursor.lastrowid, f"建立儲位 {code}", user["id"])
            else:
                db.execute(
                    """
                    UPDATE locations
                    SET warehouse_id = ?, code = ?, name = ?, status = ?
                    WHERE id = ?
                    """,
                    (warehouse_id, code, name, status, int(location_id)),
                )
                log_audit(db, "update", "location", int(location_id), f"更新儲位 {code}", user["id"])
        return self.send_json({"message": "儲位資料已儲存"})

    def handle_toggle_location_status(self):
        user = self.require_permission("manage_warehouses")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            location_id = int(payload.get("locationId"))
            status = str(payload.get("status", "active")).strip()
        except (TypeError, ValueError):
            return self.send_json({"error": "儲位狀態資料錯誤"}, status=400)
        with get_db() as db:
            db.execute("UPDATE locations SET status = ? WHERE id = ?", (status, location_id))
            log_audit(db, "toggle_status", "location", location_id, f"儲位狀態調整為 {status}", user["id"])
        return self.send_json({"message": "儲位狀態已更新"})

    def handle_list_stock_levels(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT
                    stock_levels.*,
                    products.name AS product_name,
                    products.sku AS product_sku,
                    warehouses.name AS warehouse_name,
                    warehouses.code AS warehouse_code,
                    locations.name AS location_name,
                    locations.code AS location_code
                FROM stock_levels
                JOIN products ON products.id = stock_levels.product_id
                JOIN warehouses ON warehouses.id = stock_levels.warehouse_id
                JOIN locations ON locations.id = stock_levels.location_id
                WHERE products.deleted_at = ''
                ORDER BY stock_levels.id DESC
                """
            ).fetchall()
        return self.send_json(
            {
                "stockLevels": [
                    {
                        "id": row["id"],
                        "productId": row["product_id"],
                        "productName": row["product_name"],
                        "productSku": row["product_sku"],
                        "warehouseId": row["warehouse_id"],
                        "warehouseName": row["warehouse_name"],
                        "warehouseCode": row["warehouse_code"],
                        "locationId": row["location_id"],
                        "locationName": row["location_name"],
                        "locationCode": row["location_code"],
                        "batchNo": row["batch_no"],
                        "serialNo": row["serial_no"],
                        "expiryDate": row["expiry_date"],
                        "quantity": row["quantity"],
                        "updatedAt": row["updated_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_manual_movement(self):
        user = self.require_permission("manage_manual_stock")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            product_id = int(payload.get("productId"))
            warehouse_id = int(payload.get("warehouseId"))
            location_id = int(payload.get("locationId"))
            quantity = int(payload.get("quantity"))
            direction = str(payload.get("direction", "")).strip()
            transaction_type = str(payload.get("transactionType", "manual")).strip() or "manual"
        except (TypeError, ValueError):
            return self.send_json({"error": "庫存異動資料格式錯誤"}, status=400)

        if direction not in {"in", "out"}:
            return self.send_json({"error": "異動方向不存在"}, status=400)

        note = str(payload.get("note", "")).strip()
        reference_no = str(payload.get("referenceNo", "")).strip()
        batch_no = str(payload.get("batchNo", "")).strip()
        serial_no = str(payload.get("serialNo", "")).strip()
        expiry_date = str(payload.get("expiryDate", "")).strip()

        try:
            with get_db() as db:
                apply_stock_transaction(
                    db,
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    location_id=location_id,
                    direction=direction,
                    quantity=quantity,
                    transaction_type=transaction_type,
                    note=note,
                    reference_no=reference_no,
                    user_id=user["id"],
                    batch_no=batch_no,
                    serial_no=serial_no,
                    expiry_date=expiry_date,
                )
                log_audit(
                    db,
                    "manual_movement",
                    "stock",
                    product_id,
                    f"{direction} {quantity} @ {reference_no or 'manual'}",
                    user["id"],
                )
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)

        return self.send_json({"message": "庫存異動已完成"})

    def handle_create_document(self):
        user = self.require_permission("manage_documents")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            doc_type = require_text(payload, "docType", "單據類型")
            warehouse_id = int(payload.get("warehouseId"))
            location_id = int(payload.get("locationId"))
            product_id = int(payload.get("productId"))
        except (TypeError, ValueError):
            return self.send_json({"error": "單據資料格式錯誤"}, status=400)
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)
        if doc_type not in DOCUMENT_TYPES:
            return self.send_json({"error": "單據類型不存在"}, status=400)

        partner_id = payload.get("partnerId") or None
        quantity = int(payload.get("quantity", 0) or 0)
        counted_quantity = int(payload.get("countedQuantity", 0) or 0)
        if doc_type != "stocktake" and quantity <= 0:
            return self.send_json({"error": "單據數量必須大於 0"}, status=400)
        if doc_type == "stocktake" and counted_quantity < 0:
            return self.send_json({"error": "盤點數量不能小於 0"}, status=400)

        with get_db() as db:
            doc_no = str(payload.get("docNo", "")).strip() or generate_document_no(db, doc_type)
            try:
                cursor = db.execute(
                    """
                    INSERT INTO documents (
                        doc_type, doc_no, partner_id, warehouse_id, location_id, product_id, quantity,
                        unit_price, counted_quantity, batch_no, serial_no, expiry_date, status,
                        note, created_by, created_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, '')
                    """,
                    (
                        doc_type,
                        doc_no,
                        partner_id,
                        warehouse_id,
                        location_id,
                        product_id,
                        quantity,
                        float(payload.get("unitPrice", 0) or 0),
                        counted_quantity,
                        str(payload.get("batchNo", "")).strip(),
                        str(payload.get("serialNo", "")).strip(),
                        str(payload.get("expiryDate", "")).strip(),
                        str(payload.get("note", "")).strip(),
                        user["id"],
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                return self.send_json({"error": "單號已存在"}, status=409)

            if parse_bool(payload.get("completeNow", False)):
                self._complete_document(db, cursor.lastrowid, user["id"])
            else:
                log_audit(db, "create", "document", cursor.lastrowid, f"建立單據 {doc_no}", user["id"])
        return self.send_json({"message": "單據已建立"})

    def _complete_document(self, db, document_id, user_id):
        row = db.execute(
            "SELECT * FROM documents WHERE id = ?",
            (document_id,),
        ).fetchone()
        if not row:
            raise ValueError("找不到單據")
        if row["status"] == "completed":
            raise ValueError("單據已完成")

        note = row["note"]
        partner_id = row["partner_id"]

        if row["doc_type"] == "purchase":
            apply_stock_transaction(
                db,
                product_id=row["product_id"],
                warehouse_id=row["warehouse_id"],
                location_id=row["location_id"],
                direction="in",
                quantity=row["quantity"],
                transaction_type="purchase",
                note=note,
                reference_no=row["doc_no"],
                user_id=user_id,
                batch_no=row["batch_no"],
                serial_no=row["serial_no"],
                expiry_date=row["expiry_date"],
                partner_id=partner_id,
                document_id=document_id,
            )
        elif row["doc_type"] == "sales":
            apply_stock_transaction(
                db,
                product_id=row["product_id"],
                warehouse_id=row["warehouse_id"],
                location_id=row["location_id"],
                direction="out",
                quantity=row["quantity"],
                transaction_type="sales",
                note=note,
                reference_no=row["doc_no"],
                user_id=user_id,
                batch_no=row["batch_no"],
                serial_no=row["serial_no"],
                expiry_date=row["expiry_date"],
                partner_id=partner_id,
                document_id=document_id,
            )
        elif row["doc_type"] == "return":
            partner = None
            if partner_id:
                partner = db.execute(
                    "SELECT partner_type FROM business_partners WHERE id = ?",
                    (partner_id,),
                ).fetchone()
            direction = "in" if partner and partner["partner_type"] == "customer" else "out"
            apply_stock_transaction(
                db,
                product_id=row["product_id"],
                warehouse_id=row["warehouse_id"],
                location_id=row["location_id"],
                direction=direction,
                quantity=row["quantity"],
                transaction_type="return",
                note=note,
                reference_no=row["doc_no"],
                user_id=user_id,
                batch_no=row["batch_no"],
                serial_no=row["serial_no"],
                expiry_date=row["expiry_date"],
                partner_id=partner_id,
                document_id=document_id,
            )
        elif row["doc_type"] == "stocktake":
            current_qty = get_current_stock(
                db,
                row["product_id"],
                row["warehouse_id"],
                row["location_id"],
                row["batch_no"],
                row["serial_no"],
                row["expiry_date"],
            )
            counted_qty = row["counted_quantity"]
            diff = counted_qty - current_qty
            if diff != 0:
                apply_stock_transaction(
                    db,
                    product_id=row["product_id"],
                    warehouse_id=row["warehouse_id"],
                    location_id=row["location_id"],
                    direction="in" if diff > 0 else "out",
                    quantity=abs(diff),
                    transaction_type="stocktake",
                    note=note or "盤點差異調整",
                    reference_no=row["doc_no"],
                    user_id=user_id,
                    batch_no=row["batch_no"],
                    serial_no=row["serial_no"],
                    expiry_date=row["expiry_date"],
                    document_id=document_id,
                )
        else:
            raise ValueError("單據類型不存在")

        db.execute(
            "UPDATE documents SET status = 'completed', completed_at = ? WHERE id = ?",
            (now_iso(), document_id),
        )
        log_audit(db, "complete", "document", document_id, f"完成單據 {row['doc_no']}", user_id)

    def handle_complete_document(self):
        user = self.require_permission("complete_documents")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            document_id = int(payload.get("documentId"))
        except (TypeError, ValueError):
            return self.send_json({"error": "單據完成資料錯誤"}, status=400)

        try:
            with get_db() as db:
                self._complete_document(db, document_id, user["id"])
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)
        return self.send_json({"message": "單據已完成，庫存已同步"})

    def handle_stocktake_adjustment(self):
        user = self.require_permission("manage_stocktakes")
        if not user:
            return
        try:
            payload = self.parse_json_body()
            payload["docType"] = "stocktake"
            payload["completeNow"] = True
            if not payload.get("countedQuantity", "") and payload.get("countedQuantity", 0) != 0:
                raise ValueError("請輸入盤點數量")
        except ValueError as exc:
            return self.send_json({"error": str(exc)}, status=400)
        return self.handle_create_document_from_payload(payload, user["id"])

    def handle_create_document_from_payload(self, payload, user_id):
        with get_db() as db:
            doc_no = str(payload.get("docNo", "")).strip() or generate_document_no(db, "stocktake")
            cursor = db.execute(
                """
                INSERT INTO documents (
                    doc_type, doc_no, partner_id, warehouse_id, location_id, product_id, quantity,
                    unit_price, counted_quantity, batch_no, serial_no, expiry_date, status,
                    note, created_by, created_at, completed_at
                ) VALUES ('stocktake', ?, NULL, ?, ?, ?, 0, 0, ?, ?, ?, ?, 'draft', ?, ?, ?, '')
                """,
                (
                    doc_no,
                    int(payload.get("warehouseId")),
                    int(payload.get("locationId")),
                    int(payload.get("productId")),
                    int(payload.get("countedQuantity")),
                    str(payload.get("batchNo", "")).strip(),
                    str(payload.get("serialNo", "")).strip(),
                    str(payload.get("expiryDate", "")).strip(),
                    str(payload.get("note", "")).strip(),
                    user_id,
                    now_iso(),
                ),
            )
            self._complete_document(db, cursor.lastrowid, user_id)
        return self.send_json({"message": "盤點差異已完成調整"})

    def handle_list_documents(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT
                    documents.*,
                    products.name AS product_name,
                    products.sku AS product_sku,
                    business_partners.name AS partner_name,
                    business_partners.partner_type AS partner_type,
                    warehouses.name AS warehouse_name,
                    locations.name AS location_name,
                    users.full_name AS created_by_name
                FROM documents
                JOIN products ON products.id = documents.product_id
                JOIN warehouses ON warehouses.id = documents.warehouse_id
                JOIN locations ON locations.id = documents.location_id
                LEFT JOIN business_partners ON business_partners.id = documents.partner_id
                LEFT JOIN users ON users.id = documents.created_by
                ORDER BY documents.id DESC
                """
            ).fetchall()
        return self.send_json(
            {
                "documents": [
                    {
                        "id": row["id"],
                        "docType": row["doc_type"],
                        "docNo": row["doc_no"],
                        "partnerId": row["partner_id"],
                        "partnerName": row["partner_name"] or "",
                        "partnerType": row["partner_type"] or "",
                        "warehouseId": row["warehouse_id"],
                        "warehouseName": row["warehouse_name"],
                        "locationId": row["location_id"],
                        "locationName": row["location_name"],
                        "productId": row["product_id"],
                        "productName": row["product_name"],
                        "productSku": row["product_sku"],
                        "quantity": row["quantity"],
                        "unitPrice": row["unit_price"],
                        "countedQuantity": row["counted_quantity"],
                        "batchNo": row["batch_no"],
                        "serialNo": row["serial_no"],
                        "expiryDate": row["expiry_date"],
                        "status": row["status"],
                        "note": row["note"],
                        "createdBy": row["created_by_name"] or "",
                        "createdAt": row["created_at"],
                        "completedAt": row["completed_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_list_movements(self, query):
        user = self.require_auth()
        if not user:
            return
        params = parse_qs(query)
        limit = min(max(int(params.get("limit", ["100"])[0]), 1), 300)
        with get_db() as db:
            new_rows = db.execute(
                """
                SELECT
                    inventory_transactions.id,
                    inventory_transactions.transaction_type AS movement_type,
                    inventory_transactions.direction,
                    inventory_transactions.quantity,
                    inventory_transactions.note,
                    inventory_transactions.reference_no,
                    inventory_transactions.created_at,
                    products.name AS product_name,
                    products.sku AS product_sku,
                    warehouses.name AS warehouse_name,
                    locations.name AS location_name,
                    users.full_name AS created_by_name
                FROM inventory_transactions
                JOIN products ON products.id = inventory_transactions.product_id
                JOIN warehouses ON warehouses.id = inventory_transactions.warehouse_id
                JOIN locations ON locations.id = inventory_transactions.location_id
                JOIN users ON users.id = inventory_transactions.created_by
                ORDER BY inventory_transactions.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            old_rows = db.execute(
                """
                SELECT
                    stock_movements.id,
                    stock_movements.movement_type,
                    stock_movements.movement_type AS direction,
                    stock_movements.quantity,
                    stock_movements.note,
                    stock_movements.reference_no,
                    stock_movements.created_at,
                    products.name AS product_name,
                    products.sku AS product_sku,
                    '' AS warehouse_name,
                    '' AS location_name,
                    users.full_name AS created_by_name
                FROM stock_movements
                JOIN products ON products.id = stock_movements.product_id
                JOIN users ON users.id = stock_movements.created_by
                ORDER BY stock_movements.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        combined = []
        for row in list(new_rows) + list(old_rows):
            combined.append(
                {
                    "id": row["id"],
                    "movementType": row["movement_type"],
                    "direction": row["direction"],
                    "quantity": row["quantity"],
                    "note": row["note"],
                    "referenceNo": row["reference_no"],
                    "createdAt": row["created_at"],
                    "productName": row["product_name"],
                    "productSku": row["product_sku"],
                    "warehouseName": row["warehouse_name"],
                    "locationName": row["location_name"],
                    "createdBy": row["created_by_name"],
                }
            )
        combined.sort(key=lambda item: item["createdAt"], reverse=True)
        return self.send_json({"movements": combined[:limit]})

    def handle_list_audit_logs(self):
        user = self.require_permission("view_audit")
        if not user:
            return
        with get_db() as db:
            rows = db.execute(
                """
                SELECT audit_logs.*, users.full_name AS user_name
                FROM audit_logs
                LEFT JOIN users ON users.id = audit_logs.created_by
                ORDER BY audit_logs.id DESC
                LIMIT 300
                """
            ).fetchall()
        return self.send_json(
            {
                "auditLogs": [
                    {
                        "id": row["id"],
                        "action": row["action"],
                        "entityType": row["entity_type"],
                        "entityId": row["entity_id"],
                        "detail": row["detail"],
                        "createdBy": row["user_name"] or "",
                        "createdAt": row["created_at"],
                    }
                    for row in rows
                ]
            }
        )

    def handle_export(self, query):
        user = self.require_permission("export_reports")
        if not user:
            return
        params = parse_qs(query)
        export_type = params.get("type", ["products"])[0]
        with get_db() as db:
            if export_type == "products":
                rows = db.execute(
                    """
                    SELECT sku, name, quantity, min_quantity, barcode, qr_code, unit, active, updated_at
                    FROM products
                    WHERE deleted_at = ''
                    ORDER BY id DESC
                    """
                ).fetchall()
                return self.send_csv(
                    "products.csv",
                    [
                        [
                            row["sku"],
                            row["name"],
                            row["quantity"],
                            row["min_quantity"],
                            row["barcode"],
                            row["qr_code"],
                            row["unit"],
                            "active" if row["active"] else "inactive",
                            row["updated_at"],
                        ]
                        for row in rows
                    ],
                    ["SKU", "Name", "Quantity", "Min Quantity", "Barcode", "QR Code", "Unit", "Status", "Updated At"],
                )
            if export_type == "movements":
                rows = db.execute(
                    """
                    SELECT transaction_type, direction, quantity, reference_no, note, created_at
                    FROM inventory_transactions
                    ORDER BY id DESC
                    """
                ).fetchall()
                return self.send_csv(
                    "movements.csv",
                    [
                        [row["transaction_type"], row["direction"], row["quantity"], row["reference_no"], row["note"], row["created_at"]]
                        for row in rows
                    ],
                    ["Type", "Direction", "Quantity", "Reference No", "Note", "Created At"],
                )
            if export_type == "documents":
                rows = db.execute(
                    "SELECT doc_no, doc_type, status, quantity, counted_quantity, created_at, completed_at FROM documents ORDER BY id DESC"
                ).fetchall()
                return self.send_csv(
                    "documents.csv",
                    [
                        [row["doc_no"], row["doc_type"], row["status"], row["quantity"], row["counted_quantity"], row["created_at"], row["completed_at"]]
                        for row in rows
                    ],
                    ["Doc No", "Type", "Status", "Quantity", "Counted Quantity", "Created At", "Completed At"],
                )
            if export_type == "partners":
                rows = db.execute(
                    "SELECT partner_type, name, contact_name, phone, email, status FROM business_partners ORDER BY id DESC"
                ).fetchall()
                return self.send_csv(
                    "partners.csv",
                    [[row["partner_type"], row["name"], row["contact_name"], row["phone"], row["email"], row["status"]] for row in rows],
                    ["Partner Type", "Name", "Contact", "Phone", "Email", "Status"],
                )
        return self.send_json({"error": "報表類型不存在"}, status=400)

    def handle_dashboard(self):
        user = self.require_auth()
        if not user:
            return
        with get_db() as db:
            stats = {
                "productCount": db.execute("SELECT COUNT(*) AS count FROM products WHERE deleted_at = ''").fetchone()["count"],
                "activeProductCount": db.execute("SELECT COUNT(*) AS count FROM products WHERE deleted_at = '' AND active = 1").fetchone()["count"],
                "partnerCount": db.execute("SELECT COUNT(*) AS count FROM business_partners").fetchone()["count"],
                "supplierCount": db.execute("SELECT COUNT(*) AS count FROM business_partners WHERE partner_type = 'supplier'").fetchone()["count"],
                "customerCount": db.execute("SELECT COUNT(*) AS count FROM business_partners WHERE partner_type = 'customer'").fetchone()["count"],
                "warehouseCount": db.execute("SELECT COUNT(*) AS count FROM warehouses").fetchone()["count"],
                "locationCount": db.execute("SELECT COUNT(*) AS count FROM locations").fetchone()["count"],
                "documentCount": db.execute("SELECT COUNT(*) AS count FROM documents").fetchone()["count"],
                "pendingDocumentCount": db.execute("SELECT COUNT(*) AS count FROM documents WHERE status = 'draft'").fetchone()["count"],
                "auditCount": db.execute("SELECT COUNT(*) AS count FROM audit_logs").fetchone()["count"],
                "lowStockCount": db.execute(
                    "SELECT COUNT(*) AS count FROM products WHERE deleted_at = '' AND quantity <= min_quantity"
                ).fetchone()["count"],
                "totalUnits": db.execute("SELECT COALESCE(SUM(quantity), 0) AS total FROM products WHERE deleted_at = ''").fetchone()["total"],
            }
        return self.send_json(
            {
                "user": user,
                "stats": stats,
                "roles": [
                    {"role": role, "permissions": sorted(list(perms))}
                    for role, perms in ROLE_PERMISSIONS.items()
                ],
                "permissionCatalog": sorted(ALL_PERMISSIONS),
            }
        )

    def handle_legacy_stock_in(self):
        user = self.require_permission("stock_in")
        if not user:
            return
        return self._handle_legacy_stock_change("in", user)

    def handle_legacy_stock_out(self):
        user = self.require_permission("stock_out")
        if not user:
            return
        return self._handle_legacy_stock_change("out", user)

    def _handle_legacy_stock_change(self, direction, user):
        try:
            payload = self.parse_json_body()
            product_id = int(payload.get("productId"))
            quantity = int(payload.get("quantity"))
        except (TypeError, ValueError):
            return self.send_json({"error": "商品與數量格式錯誤"}, status=400)

        with get_db() as db:
            warehouse_id, location_id = ensure_default_warehouse_location(db)
            try:
                apply_stock_transaction(
                    db,
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    location_id=location_id,
                    direction=direction,
                    quantity=quantity,
                    transaction_type="legacy_manual",
                    note=str(payload.get("note", "")).strip(),
                    reference_no=str(payload.get("referenceNo", "")).strip(),
                    user_id=user["id"],
                )
            except ValueError as exc:
                return self.send_json({"error": str(exc)}, status=400)
        return self.send_json({"message": "庫存異動完成"})


def run():
    init_db()
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), InventoryHandler)
    print(f"Inventory system running at http://127.0.0.1:{port}")
    print(f"Default admin: {ADMIN_USERNAME} / {ADMIN_PASSWORD}")
    server.serve_forever()


if __name__ == "__main__":
    run()
