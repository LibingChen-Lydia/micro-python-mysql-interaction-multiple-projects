from flask import Flask, request, jsonify, g
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from typing import Optional, Callable
import os, jwt

from mysql_helper import MySqlHelper  # 你的封装类，需提供 execute_query / execute_non_query

# ========== Flask & CORS ==========
app = Flask(__name__)
CORS(app)

# ========== 配置 ==========
# JWT
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_ALG = "HS256"
JWT_EXPIRE_HOURS = 24

# MySQL（可用环境变量覆盖；与你现在的 testdb 一致）
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "12345678"),
    "database": os.environ.get("DB_NAME", "testdb"),
    "charset": "utf8mb4",
    "cursorclass": __import__("pymysql").cursors.DictCursor,
}

# ========== DB 实例 ==========
db = MySqlHelper(**DB_CONFIG)

# ========== JWT 工具 ==========
def create_jwt(payload: dict) -> str:
    exp = datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
    data = {**payload, "exp": exp}
    return jwt.encode(data, JWT_SECRET, algorithm=JWT_ALG)

def decode_jwt(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def get_token_from_header() -> Optional[str]:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:]
    return None

def require_auth(fn: Callable):
    """统一鉴权装饰器：校验 JWT，并把用户信息放到 g.user"""
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = get_token_from_header()
        data = decode_jwt(token) if token else None
        if not data:
            return jsonify({"error": "unauthorized"}), 401
        g.user = data
        return fn(*args, **kwargs)
    return wrapper

# ========== 健康检查 ==========
@app.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})

# ========== 认证相关 ==========
@app.post("/api/auth/register")
def register():
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    # 新增：允许前端传 username；如果没传，用邮箱前缀
    username = (data.get("username") or (email.split("@")[0] if "@" in email else email)).strip()

    if not email or not password:
        return jsonify({"error": "email and password required"}), 400

    # 已存在？
    rows = db.execute_query("SELECT id FROM users WHERE email=%s", (email,))
    if rows:
        return jsonify({"error": "email already exists"}), 409

    pwd_hash = generate_password_hash(password, method="pbkdf2:sha256", salt_length=16)

    # 关键：把 username 一起写入
    db.execute_non_query(
        "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
        (username, email, pwd_hash),
    )
    return jsonify({"message": "ok"})

@app.post("/api/auth/login")
def login():
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"error": "email and password required"}), 400

    rows = db.execute_query("SELECT id, password_hash FROM users WHERE email=%s", (email,))
    if not rows or not check_password_hash(rows[0]["password_hash"], password):
        return jsonify({"error": "invalid credentials"}), 401

    token = create_jwt({"uid": rows[0]["id"], "email": email})
    return jsonify({"token": token})

@app.get("/api/me")
@require_auth
def me():
    return jsonify({"uid": g.user["uid"], "email": g.user["email"]})

# ========== Demo（可留着给你页面自测 GET/POST 按钮） ==========
@app.get("/echo")
def echo():
    q = request.args.get("q", "")
    return jsonify({"message": f"参数是{q}"})

@app.post("/echo2")
def echo2():
    param = request.args.get("param", "")
    data = request.get_json(silent=True) or {}
    body_val = data.get("body", "")
    return jsonify({"message": f"body中的参数是{body_val}，param中的参数是{param}"})


# ========== 真实数据接口：与前端路径一一对应 ==========
# 1) 按年份统计（折线图）
@app.get("/api/movies/by-year")
@require_auth
def movies_by_year():
    sql = """
        SELECT year AS year, COUNT(*) AS cnt
        FROM douban_movies
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    rows = db.execute_query(sql)
    return jsonify(rows)

# 2) 按类型占比（饼图）
@app.get("/api/movies/by-genre")
@require_auth
def movies_by_genre():
    # 依赖：douban_movie_genre(movie_id, genre_id)，douban_genre(id, name)
    sql = """
        SELECT g.name AS name, COUNT(*) AS cnt
        FROM douban_movie_genre mg
        JOIN douban_genre g ON mg.genre_id = g.id
        GROUP BY g.id, g.name
        ORDER BY cnt DESC
    """
    rows = db.execute_query(sql)
    return jsonify(rows)

# 3) 按国家占比（饼图）
@app.get("/api/movies/by-country")
@require_auth
def movies_by_country():
    # 依赖：douban_movie_country(movie_id, country_id)，douban_country(id, name)
    sql = """
        SELECT c.name AS name, COUNT(*) AS cnt
        FROM douban_movie_country mc
        JOIN douban_country c ON mc.country_id = c.id
        GROUP BY c.id, c.name
        ORDER BY cnt DESC
    """
    rows = db.execute_query(sql)
    return jsonify(rows)


# ========== 入口 ==========
if __name__ == "__main__":
    # 前端 package.json 里有 "proxy": "http://127.0.0.1:5000"
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")), debug=True)