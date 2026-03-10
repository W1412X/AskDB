"""
数据库工具类
提供数据库查询相关操作，支持连接池管理
仅支持查询操作，不支持增删改操作
"""

import pymysql
import threading
from dbutils.pooled_db import PooledDB
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager
import time
from config.app_config import get_app_config
from utils.logger import get_logger

logger = get_logger("database_tool")


class DatabaseTool:
    """数据库工具类，提供查询操作和连接池管理"""
    
    def __init__(
        self,
        host: str,
        port: int = 3306,
        user: str = None,
        password: str = None,
        database: str = None,
        charset: str = 'utf8mb4',
        mincached: int = 1,
        maxcached: int = 5,
        maxshared: int = 3,
        maxconnections: int = 10,
        blocking: bool = True,
        maxusage: int = 1000,
        setsession: list = None,
        reset: bool = True,
        failures: tuple = None,
        ping: int = 1
    ):
        """
        初始化数据库工具类
        
        Args:
            host: 数据库主机地址
            port: 数据库端口，默认3306
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称（可选，可在后续操作中指定）
            charset: 字符集，默认utf8mb4
            mincached: 连接池中空闲连接的最小数量
            maxcached: 连接池中空闲连接的最大数量
            maxshared: 最大共享连接数
            maxconnections: 最大连接数
            blocking: 当连接数达到最大值时是否阻塞等待
            maxusage: 单个连接的最大使用次数
            setsession: 会话设置列表
            reset: 连接返回池时是否重置
            failures: 异常类型元组，遇到这些异常时重试
            ping: 检查连接是否有效的间隔（0=从不，1=每次使用，2=每2次使用，以此类推）
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        
        # 连接池配置
        # 设置默认的异常类型，用于连接重试
        if failures is None:
            failures = (pymysql.Error, pymysql.OperationalError, pymysql.InterfaceError)
        
        self.pool_config = {
            'creator': pymysql,
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset,
            'cursorclass': pymysql.cursors.DictCursor,
            'mincached': mincached,
            'maxcached': maxcached,
            'maxshared': maxshared,
            'maxconnections': maxconnections,
            'blocking': blocking,
            'maxusage': maxusage,
            'setsession': setsession or [],
            'reset': reset,
            'failures': failures,
            'ping': ping
        }
        
        self.pool: Optional[PooledDB] = None
        self._pool_lock = threading.Lock()
        # Lazy pool initialization:
        # - avoids import-time side effects
        # - enables workflows/tests that don't touch DB
        # Pool is initialized on first actual query/connection request.
    
    def _initialize_pool(self):
        """初始化连接池"""
        try:
            logger.info(
                f"初始化数据库连接池",
                host=self.host,
                port=self.port,
                database=self.database,
                maxconnections=self.pool_config.get('maxconnections', 10)
            )
            self.pool = PooledDB(**self.pool_config)
            logger.info(
                f"数据库连接池初始化成功",
                host=self.host,
                port=self.port,
                database=self.database
            )
        except Exception as e:
            logger.exception(
                f"数据库连接池初始化失败",
                exception=e,
                host=self.host,
                port=self.port,
                database=self.database
            )
            raise
    
    def _get_connection(self, database: str = None):
        """
        从连接池获取连接
        
        Args:
            database: 可选，指定要连接的数据库名称
            
        Returns:
            数据库连接对象
        """
        if self.pool is None:
            with self._pool_lock:
                if self.pool is None:
                    self._initialize_pool()
        if self.pool is None:
            logger.error("连接池未初始化")
            raise RuntimeError("连接池未初始化")
        
        logger.debug(f"从连接池获取连接", database=database)
        conn = self.pool.connection()
        
        # 如果指定了数据库，切换到该数据库
        if database:
            try:
                # 尝试使用 select_db 方法（pymysql 原生连接）
                if hasattr(conn, 'select_db'):
                    conn.select_db(database)
                else:
                    # 如果连接对象没有 select_db 方法，使用 SQL 语句
                    with conn.cursor() as cursor:
                        cursor.execute(f"USE `{database}`")
                logger.debug(f"切换到数据库: {database}", database=database)
            except Exception as e:
                logger.warning(f"切换数据库失败", exception=e, database=database)
        
        return conn
    
    @contextmanager
    def _get_cursor(self, database: str = None, *, readonly: bool = False, timeout_ms: int | None = None):
        """
        获取数据库游标的上下文管理器
        
        Args:
            database: 可选，指定要连接的数据库名称
            
        Yields:
            数据库游标对象
        """
        conn = None
        cursor = None
        previous_max_execution_time = None
        try:
            conn = self._get_connection(database)
            cursor = conn.cursor()
            if timeout_ms is not None:
                timeout_ms = max(1, int(timeout_ms))
                try:
                    cursor.execute("SELECT @@SESSION.max_execution_time AS max_execution_time")
                    row = cursor.fetchone() or {}
                    previous_max_execution_time = int(row.get("max_execution_time") or 0)
                    cursor.execute("SET SESSION max_execution_time = %s", (timeout_ms,))
                    logger.debug("设置会话执行超时成功", database=database, timeout_ms=timeout_ms)
                except Exception as e:
                    logger.exception("设置会话执行超时失败", exception=e, database=database, timeout_ms=timeout_ms)
                    raise RuntimeError(f"无法为当前查询设置真实超时: {e}") from e
            if readonly:
                try:
                    cursor.execute("START TRANSACTION READ ONLY")
                    logger.debug("只读事务已开启", database=database)
                except Exception as e:
                    logger.exception("开启只读事务失败", exception=e, database=database)
                    raise RuntimeError(f"无法为当前查询开启只读事务: {e}") from e
            yield cursor
            if readonly:
                conn.rollback()
                logger.debug("只读事务已回滚结束", database=database)
            else:
                conn.commit()
                logger.debug(f"数据库操作提交成功", database=database)
        except Exception as e:
            if conn:
                conn.rollback()
                logger.debug(f"数据库操作回滚", database=database)
            logger.exception(
                f"数据库操作错误",
                exception=e,
                database=database
            )
            raise
        finally:
            if cursor:
                if previous_max_execution_time is not None:
                    try:
                        cursor.execute("SET SESSION max_execution_time = %s", (previous_max_execution_time,))
                    except Exception as e:
                        logger.warning("恢复会话执行超时失败", exception=e, database=database)
                cursor.close()
            if conn:
                conn.close()
                logger.debug(f"数据库连接已关闭", database=database)
    
    def execute_query(
        self,
        sql: str,
        params: tuple = None,
        database: str = None,
        fetch_one: bool = False,
        *,
        readonly: bool = False,
        timeout_ms: int | None = None,
    ) -> List[Dict[str, Any]]:
        """
        执行查询SQL语句
        
        Args:
            sql: SQL查询语句
            params: SQL参数（用于参数化查询）
            database: 可选，指定要查询的数据库名称
            fetch_one: 是否只获取一条记录
            
        Returns:
            查询结果列表，每个元素是一个字典（字段名: 值）
        """
        start_time = time.time()
        
        logger.function_call(
            "execute_query",
            inputs={
                "sql": sql[:200] if len(sql) > 200 else sql,  # 限制SQL长度
                "params": str(params) if params else None,
                "database": database,
                "fetch_one": fetch_one,
                "readonly": readonly,
                "timeout_ms": timeout_ms,
            }
        )
        
        try:
            with self._get_cursor(database, readonly=readonly, timeout_ms=timeout_ms) as cursor:
                cursor.execute(sql, params)
                if fetch_one:
                    result = cursor.fetchone()
                    results = [result] if result else []
                else:
                    results = cursor.fetchall()
                
                duration = time.time() - start_time
                row_count = len(results)
                
                logger.function_result(
                    "execute_query",
                    result=f"查询成功，返回 {row_count} 条记录",
                    duration=duration,
                    database=database,
                    row_count=row_count,
                    fetch_one=fetch_one,
                    readonly=readonly,
                    timeout_ms=timeout_ms,
                )
                
                return results
        
        except Exception as e:
            duration = time.time() - start_time
            logger.exception(
                f"执行SQL查询失败",
                exception=e,
                sql=sql[:200] if len(sql) > 200 else sql,
                database=database,
                duration=duration
            )
            raise
    
    # ==================== 数据库实例信息查询 ====================
    
    def get_server_version(self) -> str:
        """
        获取MySQL服务器版本
        
        Returns:
            服务器版本字符串
        """
        result = self.execute_query("SELECT VERSION() as version", fetch_one=True)
        return result[0]['version'] if result else None
    
    def get_server_status(self) -> Dict[str, Any]:
        """
        获取MySQL服务器状态信息
        
        Returns:
            服务器状态信息字典
        """
        result = self.execute_query("SHOW STATUS", fetch_one=False)
        return {row['Variable_name']: row['Value'] for row in result}
    
    def get_server_variables(self) -> Dict[str, Any]:
        """
        获取MySQL服务器变量信息
        
        Returns:
            服务器变量信息字典
        """
        result = self.execute_query("SHOW VARIABLES", fetch_one=False)
        return {row['Variable_name']: row['Value'] for row in result}
    
    # ==================== 数据库信息查询 ====================
    
    def list_databases(self) -> List[str]:
        """
        列出所有数据库
        
        Returns:
            数据库名称列表
        """
        result = self.execute_query("SHOW DATABASES")
        return [row['Database'] for row in result]
    
    def get_database_info(self, database: str) -> Dict[str, Any]:
        """
        获取指定数据库的详细信息
        
        Args:
            database: 数据库名称
            
        Returns:
            数据库信息字典，包括字符集、排序规则等
        """
        sql = """
            SELECT 
                SCHEMA_NAME as database_name,
                DEFAULT_CHARACTER_SET_NAME as charset,
                DEFAULT_COLLATION_NAME as collation
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME = %s
        """
        result = self.execute_query(sql, (database,), fetch_one=True)
        return result[0] if result else {}
    
    def get_database_size(self, database: str) -> Dict[str, Any]:
        """
        获取数据库大小信息
        
        Args:
            database: 数据库名称
            
        Returns:
            数据库大小信息字典
        """
        sql = """
            SELECT 
                table_schema as database_name,
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb,
                ROUND(SUM(data_length) / 1024 / 1024, 2) AS data_size_mb,
                ROUND(SUM(index_length) / 1024 / 1024, 2) AS index_size_mb
            FROM information_schema.tables
            WHERE table_schema = %s
            GROUP BY table_schema
        """
        result = self.execute_query(sql, (database,), fetch_one=True)
        return result[0] if result else {}
    
    # ==================== 表信息查询 ====================
    
    def list_tables(self, database: str) -> List[str]:
        """
        列出指定数据库中的所有表
        
        Args:
            database: 数据库名称
            
        Returns:
            表名称列表
        """
        result = self.execute_query(f"SHOW TABLES FROM `{database}`")
        # 获取第一个字段的值（表名）
        table_key = f'Tables_in_{database}'
        return [row[table_key] for row in result]
    
    def get_table_info(self, database: str, table: str) -> Dict[str, Any]:
        """
        获取表的详细信息（表结构、引擎、字符集等）
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            表信息字典
        """
        sql = """
            SELECT 
                TABLE_NAME as table_name,
                TABLE_TYPE as table_type,
                ENGINE as engine,
                TABLE_ROWS as table_rows,
                AVG_ROW_LENGTH as avg_row_length,
                DATA_LENGTH as data_length,
                MAX_DATA_LENGTH as max_data_length,
                INDEX_LENGTH as index_length,
                DATA_FREE as data_free,
                AUTO_INCREMENT as auto_increment,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time,
                TABLE_COLLATION as table_collation,
                TABLE_COMMENT as table_comment
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        result = self.execute_query(sql, (database, table), fetch_one=True)
        return result[0] if result else {}
    
    def get_table_columns(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        获取表的所有列信息
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            列信息列表，每个元素包含列名、类型、是否为空、默认值等信息
        """
        sql = """
            SELECT 
                COLUMN_NAME as column_name,
                ORDINAL_POSITION as ordinal_position,
                COLUMN_DEFAULT as column_default,
                IS_NULLABLE as is_nullable,
                DATA_TYPE as data_type,
                CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                CHARACTER_OCTET_LENGTH as character_octet_length,
                NUMERIC_PRECISION as numeric_precision,
                NUMERIC_SCALE as numeric_scale,
                DATETIME_PRECISION as datetime_precision,
                CHARACTER_SET_NAME as character_set_name,
                COLLATION_NAME as collation_name,
                COLUMN_TYPE as column_type,
                COLUMN_KEY as column_key,
                EXTRA as extra,
                COLUMN_COMMENT as column_comment
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(sql, (database, table))
    
    def get_table_indexes(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        获取表的所有索引信息
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            索引信息列表
        """
        sql = """
            SELECT 
                INDEX_NAME as index_name,
                COLUMN_NAME as column_name,
                SEQ_IN_INDEX as seq_in_index,
                COLLATION as collation,
                CARDINALITY as cardinality,
                SUB_PART as sub_part,
                PACKED as packed,
                NULLABLE as nullable,
                INDEX_TYPE as index_type,
                COMMENT as comment
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """
        return self.execute_query(sql, (database, table))
    
    def get_table_foreign_keys(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        获取表的外键信息
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            外键信息列表
        """
        sql = """
            SELECT 
                CONSTRAINT_NAME as constraint_name,
                COLUMN_NAME as column_name,
                REFERENCED_TABLE_SCHEMA as referenced_table_schema,
                REFERENCED_TABLE_NAME as referenced_table_name,
                REFERENCED_COLUMN_NAME as referenced_column_name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        return self.execute_query(sql, (database, table))
    
    def get_table_create_sql(self, database: str, table: str) -> str:
        """
        获取表的CREATE TABLE语句
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            CREATE TABLE语句字符串
        """
        result = self.execute_query(
            f"SHOW CREATE TABLE `{database}`.`{table}`",
            fetch_one=True
        )
        return result[0]['Create Table'] if result else None
    
    def get_table_schema(self, database: str, table: str) -> Dict[str, Any]:
        """
        获取表的完整schema信息（包括列、索引、外键等）
        
        Args:
            database: 数据库名称
            table: 表名称
            
        Returns:
            完整的表schema信息字典
        """
        return {
            'table_info': self.get_table_info(database, table),
            'columns': self.get_table_columns(database, table),
            'indexes': self.get_table_indexes(database, table),
            'foreign_keys': self.get_table_foreign_keys(database, table),
            'create_sql': self.get_table_create_sql(database, table)
        }
    
    def get_all_tables_schema(self, database: str) -> Dict[str, Dict[str, Any]]:
        """
        获取数据库中所有表的schema信息
        
        Args:
            database: 数据库名称
            
        Returns:
            字典，key为表名，value为表的schema信息
        """
        tables = self.list_tables(database)
        schemas = {}
        for table in tables:
            schemas[table] = self.get_table_schema(database, table)
        return schemas
    
    # ==================== 数据查询 ====================
    
    def query_table(
        self,
        database: str,
        table: str,
        columns: List[str] = None,
        where: str = None,
        order_by: str = None,
        limit: int = None,
        offset: int = None
    ) -> List[Dict[str, Any]]:
        """
        查询表数据
        
        Args:
            database: 数据库名称
            table: 表名称
            columns: 要查询的列名列表，None表示查询所有列
            where: WHERE子句（不包含WHERE关键字）
            order_by: ORDER BY子句（不包含ORDER BY关键字）
            limit: 限制返回的记录数
            offset: 偏移量
            
        Returns:
            查询结果列表
        """
        # 构建SELECT子句
        if columns:
            columns_str = ', '.join([f"`{col}`" for col in columns])
        else:
            columns_str = '*'
        
        # 构建SQL
        sql = f"SELECT {columns_str} FROM `{database}`.`{table}`"
        
        # 添加WHERE子句
        if where:
            sql += f" WHERE {where}"
        
        # 添加ORDER BY子句
        if order_by:
            sql += f" ORDER BY {order_by}"
        
        # 添加LIMIT子句
        if limit is not None:
            sql += f" LIMIT {limit}"
            if offset is not None:
                sql += f" OFFSET {offset}"
        
        return self.execute_query(sql, database=database)
    
    def count_table_rows(self, database: str, table: str, where: str = None) -> int:
        """
        统计表的记录数
        
        Args:
            database: 数据库名称
            table: 表名称
            where: WHERE子句（不包含WHERE关键字）
            
        Returns:
            记录数
        """
        sql = f"SELECT COUNT(*) as count FROM `{database}`.`{table}`"
        if where:
            sql += f" WHERE {where}"
        
        result = self.execute_query(sql, database=database, fetch_one=True)
        return result[0]['count'] if result else 0
    
    def get_table_sample(
        self,
        database: str,
        table: str,
        limit: int = 10,
        random: bool = False
    ) -> List[Dict[str, Any]]:
        """
        获取表的样本数据
        
        Args:
            database: 数据库名称
            table: 表名称
            limit: 返回的记录数，默认10条
            random: 是否随机采样
            
        Returns:
            样本数据列表
        """
        if random:
            sql = f"SELECT * FROM `{database}`.`{table}` ORDER BY RAND() LIMIT {limit}"
        else:
            sql = f"SELECT * FROM `{database}`.`{table}` LIMIT {limit}"
        
        return self.execute_query(sql, database=database)
    
    # ==================== 连接池管理 ====================
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        获取连接池状态信息
        
        Returns:
            连接池状态信息字典
        """
        if self.pool is None:
            return {'status': 'not_initialized'}
        
        return {
            'mincached': self.pool_config['mincached'],
            'maxcached': self.pool_config['maxcached'],
            'maxshared': self.pool_config['maxshared'],
            'maxconnections': self.pool_config['maxconnections'],
            'current_connections': len(self.pool._connections) if hasattr(self.pool, '_connections') else 'unknown'
        }
    
    def close_pool(self):
        """关闭连接池"""
        with self._pool_lock:
            if self.pool:
                self.pool.close()
                self.pool = None
                logger.info("数据库连接池已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_pool()
        return False

# 全局数据库工具实例（需要在初始化时设置）
_DEFAULT_DB = get_app_config().get_database_connection()
_db_tool: Optional[DatabaseTool] = DatabaseTool(
    host=_DEFAULT_DB.host,
    port=_DEFAULT_DB.port,
    user=_DEFAULT_DB.user,
    password=_DEFAULT_DB.password,
    database=_DEFAULT_DB.database,
    charset=_DEFAULT_DB.charset,
    mincached=_DEFAULT_DB.mincached,
    maxcached=_DEFAULT_DB.maxcached,
    maxshared=_DEFAULT_DB.maxshared,
    maxconnections=_DEFAULT_DB.maxconnections,
    blocking=_DEFAULT_DB.blocking,
    maxusage=_DEFAULT_DB.maxusage,
    setsession=_DEFAULT_DB.setsession,
    reset=_DEFAULT_DB.reset,
    ping=_DEFAULT_DB.ping,
)
