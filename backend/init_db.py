from mysql_helper import MySqlHelper

config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '12345678',
    'database': 'testdb',
    'charset': 'utf8mb4'
}

db = MySqlHelper(**config)
create_sql = """
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
db.execute_non_query(create_sql)
print("用户表已创建！")