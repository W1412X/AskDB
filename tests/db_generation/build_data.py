"""
测试数据库建设脚本：使用与 config/json/database.json 一致的 MySQL 连接，
创建数据库（若不存在）并建表、填充工业智能维护样例数据。
运行前请确保 MySQL 已启动，且 config/json/database.json 中连接参数正确。
"""
import os
import random
import sys
from datetime import datetime, timedelta

# 保证可引用项目 config（建议在项目根 src 下执行：python tests/db_generation/build_data.py）
if __name__ == "__main__" and os.path.dirname(os.path.abspath(__file__)) != os.getcwd():
    _src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if _src not in sys.path:
        sys.path.insert(0, _src)

import pymysql
from faker import Faker

# 初始化
fake = Faker("zh_CN")

# 连接参数：与 config/json/database.json 一致（host/port/user/password），库名固定为测试库
TEST_DB_NAME = "test_db_industrial_monitoring"

def get_db_config():
    try:
        from config.app_config import get_app_config
        c = get_app_config().get_database_connection()
        return {
            "host": c.host or "localhost",
            "port": int(c.port or 3306),
            "user": c.user or "root",
            "password": c.password or "",
            "database": TEST_DB_NAME,
            "charset": c.charset or "utf8mb4",
        }
    except Exception:
        return {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": os.environ.get("DB_PASSWORD", ""),
            "database": TEST_DB_NAME,
            "charset": "utf8mb4",
        }


# 工业相关常量
EQUIPMENT_TYPES = [
    ("CNC机床", "DMG MORI", 10),
    ("注塑机", "海天", 8),
    ("机器人手臂", "ABB", 12),
    ("PLC控制器", "西门子", 15),
    ("空压机", "阿特拉斯", 7),
    ("激光切割机", "通快", 9),
    ("传送带系统", "西门子", 6),
    ("焊接机器人", "发那科", 10),
]

SENSOR_TYPES = [("温度", "°C"), ("振动", "mm/s"), ("电流", "A"), ("电压", "V")]

FACTORY_LOCATIONS = ["苏州工业园", "深圳宝安", "成都高新", "武汉光谷", "西安经开区"]


def ensure_database(config: dict):
    """
    若数据库不存在则创建，然后返回连接到该数据库的 connection。
    默认库名：test_db_industrial_monitoring。
    """
    db_name = config["database"]
    # 先不指定 database，连接服务器后创建库
    conn = pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        charset=config["charset"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4")
        conn.commit()
    finally:
        conn.close()
    # 再连接到目标库
    conn = pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=db_name,
        charset=config["charset"],
    )
    return conn


def create_tables(conn):
    cursor = conn.cursor()
    # 先按依赖逆序删除表，保证本脚本定义的 schema 生效（避免旧表缺列）
    for table in (
        "sensor_readings", "sensors", "maintenance_records", "quality_inspections",
        "energy_consumption", "equipment", "production_lines", "technicians",
        "equipment_types", "factories",
    ):
        cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
    # MySQL：主键使用 AUTO_INCREMENT，占位符用 %s
    # 1. factories
    cursor.execute("""
        CREATE TABLE factories (
            factory_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            location VARCHAR(255) NOT NULL,
            capacity_kw DOUBLE NOT NULL
        )
    """)
    # 2. production_lines
    cursor.execute("""
        CREATE TABLE production_lines (
            line_id INT AUTO_INCREMENT PRIMARY KEY,
            factory_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            status VARCHAR(32) CHECK(status IN ('active', 'idle', 'maintenance')),
            FOREIGN KEY(factory_id) REFERENCES factories(factory_id)
        )
    """)
    # 3. equipment_types
    cursor.execute("""
        CREATE TABLE equipment_types (
            type_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            manufacturer VARCHAR(255) NOT NULL,
            expected_lifetime_years INT NOT NULL
        )
    """)
    # 4. equipment
    cursor.execute("""
        CREATE TABLE equipment (
            equipment_id INT AUTO_INCREMENT PRIMARY KEY,
            line_id INT NOT NULL,
            type_id INT NOT NULL,
            serial_no VARCHAR(64) UNIQUE NOT NULL,
            install_date DATE NOT NULL,
            FOREIGN KEY(line_id) REFERENCES production_lines(line_id),
            FOREIGN KEY(type_id) REFERENCES equipment_types(type_id)
        )
    """)
    # 5. technicians
    cursor.execute("""
        CREATE TABLE technicians (
            technician_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            skill_level INT CHECK(skill_level BETWEEN 1 AND 5),
            factory_id INT NOT NULL,
            FOREIGN KEY(factory_id) REFERENCES factories(factory_id)
        )
    """)
    # 6. sensors
    cursor.execute("""
        CREATE TABLE sensors (
            sensor_id INT AUTO_INCREMENT PRIMARY KEY,
            equipment_id INT NOT NULL,
            type VARCHAR(64) NOT NULL,
            unit VARCHAR(32) NOT NULL,
            FOREIGN KEY(equipment_id) REFERENCES equipment(equipment_id)
        )
    """)
    # 7. sensor_readings
    cursor.execute("""
        CREATE TABLE sensor_readings (
            reading_id INT AUTO_INCREMENT PRIMARY KEY,
            sensor_id INT NOT NULL,
            timestamp DATETIME NOT NULL,
            value DOUBLE NOT NULL,
            FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id)
        )
    """)
    # 8. maintenance_records
    cursor.execute("""
        CREATE TABLE maintenance_records (
            record_id INT AUTO_INCREMENT PRIMARY KEY,
            equipment_id INT NOT NULL,
            technician_id INT NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME,
            description TEXT,
            FOREIGN KEY(equipment_id) REFERENCES equipment(equipment_id),
            FOREIGN KEY(technician_id) REFERENCES technicians(technician_id)
        )
    """)
    # 9. quality_inspections
    cursor.execute("""
        CREATE TABLE quality_inspections (
            inspection_id INT AUTO_INCREMENT PRIMARY KEY,
            line_id INT NOT NULL,
            batch_id VARCHAR(64) NOT NULL,
            pass_rate DOUBLE CHECK(pass_rate BETWEEN 0 AND 1),
            inspector_id INT NOT NULL,
            FOREIGN KEY(line_id) REFERENCES production_lines(line_id)
        )
    """)
    # 10. energy_consumption
    cursor.execute("""
        CREATE TABLE energy_consumption (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            factory_id INT NOT NULL,
            timestamp DATETIME NOT NULL,
            power_kwh DOUBLE NOT NULL,
            water_m3 DOUBLE NOT NULL,
            FOREIGN KEY(factory_id) REFERENCES factories(factory_id)
        )
    """)
    conn.commit()


def insert_data(conn):
    cursor = conn.cursor()
    # 1. factories
    factories = []
    for i in range(5):
        name = f"{fake.company()}制造基地"
        loc = random.choice(FACTORY_LOCATIONS)
        cap = round(random.uniform(500, 5000), 2)
        cursor.execute(
            "INSERT INTO factories (name, location, capacity_kw) VALUES (%s, %s, %s)",
            (name, loc, cap),
        )
        factories.append(cursor.lastrowid)
    # 2. equipment_types
    type_ids = []
    for name, manu, life in EQUIPMENT_TYPES:
        cursor.execute(
            "INSERT INTO equipment_types (name, manufacturer, expected_lifetime_years) VALUES (%s, %s, %s)",
            (name, manu, life),
        )
        type_ids.append(cursor.lastrowid)
    # 3. production_lines & equipment & sensors
    line_ids = []
    equipment_ids = []
    sensor_ids = []
    sensor_type_by_id = {}
    for fid in factories:
        for j in range(3):
            line_name = f"生产线-{chr(65 + j)}"
            status = random.choice(["active", "active", "idle"])
            cursor.execute(
                "INSERT INTO production_lines (factory_id, name, status) VALUES (%s, %s, %s)",
                (fid, line_name, status),
            )
            lid = cursor.lastrowid
            line_ids.append(lid)
            for _ in range(random.randint(8, 15)):
                tid = random.choice(type_ids)
                serial = fake.unique.bothify(text="EQ-####-??")
                install_date = fake.date_between(start_date="-5y", end_date="today")
                cursor.execute(
                    """
                    INSERT INTO equipment (line_id, type_id, serial_no, install_date)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (lid, tid, serial, install_date),
                )
                eq_id = cursor.lastrowid
                equipment_ids.append(eq_id)
                for _ in range(random.randint(2, 4)):
                    s_type, unit = random.choice(SENSOR_TYPES)
                    cursor.execute(
                        "INSERT INTO sensors (equipment_id, type, unit) VALUES (%s, %s, %s)",
                        (eq_id, s_type, unit),
                    )
                    sid = cursor.lastrowid
                    sensor_ids.append(sid)
                    sensor_type_by_id[sid] = s_type
    # 4. technicians
    tech_ids = []
    for fid in factories:
        for _ in range(5):
            name = fake.name()
            level = random.randint(2, 5)
            cursor.execute(
                "INSERT INTO technicians (name, skill_level, factory_id) VALUES (%s, %s, %s)",
                (name, level, fid),
            )
            tech_ids.append(cursor.lastrowid)
    # 5. sensor_readings
    start_time = datetime.now() - timedelta(days=30)
    for sid in sensor_ids:
        num_readings = random.randint(100, 300)
        s_type = sensor_type_by_id.get(sid, "温度")
        for _ in range(num_readings):
            ts = start_time + timedelta(minutes=random.randint(1, 43200))
            if s_type == "温度":
                val = round(random.uniform(20, 120), 2)
            elif s_type == "振动":
                val = round(random.uniform(0.1, 10.0), 3)
            else:
                val = round(random.uniform(0, 500), 2)
            cursor.execute(
                "INSERT INTO sensor_readings (sensor_id, timestamp, value) VALUES (%s, %s, %s)",
                (sid, ts, val),
            )
    # 6. maintenance_records
    for eq_id in random.sample(equipment_ids, k=min(200, len(equipment_ids))):
        tech_id = random.choice(tech_ids)
        start = fake.date_time_between(start_date="-2y", end_date="now")
        duration = timedelta(hours=random.randint(1, 48))
        end = start + duration
        desc = fake.sentence(nb_words=6)
        cursor.execute(
            """
            INSERT INTO maintenance_records (equipment_id, technician_id, start_time, end_time, description)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (eq_id, tech_id, start, end, desc),
        )
    # 7. quality_inspections
    for lid in line_ids:
        for _ in range(50):
            batch = fake.bothify(text="BATCH-########")
            pass_rate = round(random.uniform(0.85, 1.0), 4)
            inspector = random.choice(tech_ids)
            cursor.execute(
                """
                INSERT INTO quality_inspections (line_id, batch_id, pass_rate, inspector_id)
                VALUES (%s, %s, %s, %s)
                """,
                (lid, batch, pass_rate, inspector),
            )
    # 8. energy_consumption
    for fid in factories:
        current = datetime.now() - timedelta(days=60)
        while current < datetime.now():
            power = round(random.uniform(100, 2000), 2)
            water = round(random.uniform(5, 100), 2)
            cursor.execute(
                """
                INSERT INTO energy_consumption (factory_id, timestamp, power_kwh, water_m3)
                VALUES (%s, %s, %s, %s)
                """,
                (fid, current, power, water),
            )
            current += timedelta(hours=1)
    conn.commit()
    print("✅ 数据填充完成！")
    print(f"   - 工厂: {len(factories)}")
    print(f"   - 设备: {len(equipment_ids)}")
    print(f"   - 传感器读数: {len(sensor_ids) * 200}+")
    print("   - 维护记录: 200+")


def main():
    config = get_db_config()
    print(f"连接: {config['host']}:{config['port']} 用户={config['user']} 数据库={config['database']}")
    conn = ensure_database(config)
    try:
        create_tables(conn)
        insert_data(conn)
    finally:
        conn.close()
    print(f"\n📁 数据库已就绪: {config['database']}")


if __name__ == "__main__":
    main()
