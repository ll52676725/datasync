"""
db_adapter.py — 数据库适配器抽象层

提供统一的数据库操作接口，屏蔽 MySQL / DB2 底层差异。
所有数据库适配器均继承自 BaseDBAdapter，实现标准方法，
使得同步引擎可以透明地操作任意数据库组合：
  mysql→mysql, db2→db2, mysql→db2, db2→mysql

设计模式：策略模式 (Strategy Pattern) + 工厂模式 (Factory Pattern)
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Any, Dict

logger = logging.getLogger(__name__)


class BaseDBAdapter(ABC):
    """
    数据库适配器抽象基类。

    定义所有数据库操作的标准接口，包括：
    - 连接管理（创建 / 关闭）
    - 元数据查询（表列表、列、主键、行数、DDL）
    - 数据读取（按主键范围分块读取）
    - 数据写入（批量插入 / 清空表）
    - DDL 同步（在目标库创建表）
    """

    def __init__(self, cfg: Dict[str, Any]):
        """
        初始化适配器。

        Args:
            cfg: 数据库连接配置字典，至少包含 host/port/user/password/database/type
        """
        self.cfg = cfg
        self.db_type = cfg.get("type", "mysql").lower()

    # ------------------------------------------------------------------ #
    #                        连接管理
    # ------------------------------------------------------------------ #

    @abstractmethod
    def create_connection(self, use_dict_cursor: bool = False):
        """
        创建数据库连接。

        Args:
            use_dict_cursor: 是否使用字典游标（返回 dict 行），默认返回 tuple 行

        Returns:
            数据库连接对象
        """
        ...

    @abstractmethod
    def close_connection(self, conn):
        """安全关闭数据库连接。"""
        ...

    # ------------------------------------------------------------------ #
    #                        元数据查询
    # ------------------------------------------------------------------ #

    @abstractmethod
    def list_tables(self, conn) -> List[str]:
        """
        获取数据库中所有用户表名。

        Args:
            conn: 已建立的数据库连接

        Returns:
            表名列表，如 ['users', 'orders', ...]
        """
        ...

    @abstractmethod
    def get_table_columns(self, conn, table_name: str) -> List[str]:
        """
        获取表的列名（按定义顺序）。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名

        Returns:
            列名列表，如 ['id', 'name', 'age', ...]
        """
        ...

    @abstractmethod
    def get_primary_key(self, conn, table_name: str) -> Optional[str]:
        """
        获取表的主键列名（仅取第一个主键列）。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名

        Returns:
            主键列名，若无主键则返回 None
        """
        ...

    @abstractmethod
    def get_table_row_count(self, conn, table_name: str) -> int:
        """
        获取表的近似行数。优先使用统计信息，回退到 COUNT(*)。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名

        Returns:
            近似行数
        """
        ...

    @abstractmethod
    def get_pk_range(self, conn, table_name: str, pk_col: str) -> Tuple[int, int]:
        """
        获取主键列的最小值和最大值。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名
            pk_col: 主键列名

        Returns:
            (min_pk, max_pk) 元组，若表为空则返回 (0, 0)
        """
        ...

    @abstractmethod
    def get_create_table_ddl(self, conn, table_name: str) -> str:
        """
        获取表的 CREATE TABLE 语句。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名

        Returns:
            DDL 语句字符串
        """
        ...

    # ------------------------------------------------------------------ #
    #                        数据读取
    # ------------------------------------------------------------------ #

    @abstractmethod
    def fetch_chunk(self, conn, table_name: str, columns: List[str],
                    pk_col: str, start_pk: int, end_pk: int,
                    batch_size: int):
        """
        按主键范围读取一个数据块，返回生成器/游标。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名
            columns: 列名列表
            pk_col: 主键列名
            start_pk: 起始主键值（包含）
            end_pk: 结束主键值（包含）
            batch_size: 每次 fetchmany 的批量大小

        Returns:
            迭代器，每次返回一批 dict 行
        """
        ...

    @abstractmethod
    def fetch_all_streaming(self, conn, table_name: str, columns: List[str],
                            batch_size: int):
        """
        无主键时全表流式读取。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名
            columns: 列名列表
            batch_size: 每次 fetchmany 的批量大小

        Returns:
            迭代器，每次返回一批 dict 行
        """
        ...

    # ------------------------------------------------------------------ #
    #                        数据写入
    # ------------------------------------------------------------------ #

    @abstractmethod
    def table_exists(self, conn, table_name: str) -> bool:
        """
        检查表是否已存在。

        Args:
            conn: 已建立的数据库连接
            table_name: 表名

        Returns:
            True / False
        """
        ...

    @abstractmethod
    def create_table_from_ddl(self, conn, ddl: str):
        """
        执行 DDL 语句在目标库创建表。

        Args:
            conn: 目标数据库连接
            ddl: CREATE TABLE 语句
        """
        ...

    @abstractmethod
    def truncate_table(self, conn, table_name: str):
        """
        清空目标表数据。

        Args:
            conn: 目标数据库连接
            table_name: 表名
        """
        ...

    @abstractmethod
    def batch_insert(self, conn, table_name: str, columns: List[str],
                     rows: List[tuple]):
        """
        批量插入数据到目标表。

        Args:
            conn: 目标数据库连接
            table_name: 表名
            columns: 列名列表
            rows: 元组列表，每个元组对应一行数据
        """
        ...

    @abstractmethod
    def test_connection(self, cfg: Dict[str, Any]) -> Tuple[bool, str]:
        """
        测试数据库连接是否可用。

        Args:
            cfg: 数据库连接配置

        Returns:
            (成功标志, 消息) 元组
        """
        ...

    # ------------------------------------------------------------------ #
    #                        SQL 标识符引用
    # ------------------------------------------------------------------ #

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """
        为 SQL 标识符（表名/列名）添加引号，避免与保留字冲突。

        MySQL 使用反引号 `name`，DB2 使用双引号 "NAME"。

        Args:
            name: 标识符名称

        Returns:
            加引号后的标识符
        """
        ...

    # ------------------------------------------------------------------ #
    #                        CDC (Change Data Capture)
    # ------------------------------------------------------------------ #

    def supports_cdc(self) -> bool:
        """
        检查该数据库是否支持 CDC 实时捕获。

        Returns:
            True 表示支持 CDC，False 表示不支持
        """
        return False

    def create_cdc_stream(self, tables: List[str], callback, **kwargs):
        """
        创建 CDC 数据流，用于实时捕获数据变更。

        Args:
            tables: 需要监听的表名列表
            callback: 变更事件回调函数，参数为 (event_type, table_name, data)
            **kwargs: 额外的 CDC 配置参数

        Returns:
            CDC 流对象，可用于停止监听

        Raises:
            NotImplementedError: 如果该数据库不支持 CDC
        """
        raise NotImplementedError(f"CDC 未在 {self.__class__.__name__} 中实现")

    def get_current_binlog_position(self, conn) -> Optional[Dict[str, Any]]:
        """
        获取当前 binlog 位置（用于 CDC 断点续传）。

        Args:
            conn: 数据库连接

        Returns:
            binlog 位置信息字典，不支持则返回 None
        """
        return None

    def upsert_row(self, conn, table_name: str, data: Dict[str, Any], primary_key: str):
        """
        插入或更新一行数据（用于 CDC 同步 INSERT/UPDATE 事件）。

        Args:
            conn: 数据库连接
            table_name: 表名
            data: 行数据字典
            primary_key: 主键列名
        """
        raise NotImplementedError(f"upsert_row 未在 {self.__class__.__name__} 中实现")

    def delete_row(self, conn, table_name: str, primary_key: str, pk_value: Any):
        """
        删除一行数据（用于 CDC 同步 DELETE 事件）。

        Args:
            conn: 数据库连接
            table_name: 表名
            primary_key: 主键列名
            pk_value: 主键值
        """
        raise NotImplementedError(f"delete_row 未在 {self.__class__.__name__} 中实现")


# ====================================================================== #
#                        MySQL 适配器实现
# ====================================================================== #

class MySQLAdapter(BaseDBAdapter):
    """
    MySQL 数据库适配器。

    基于 pymysql 驱动，实现 BaseDBAdapter 定义的所有接口。
    适用于 MySQL 5.7+ / 8.0+。
    """

    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        logger.debug("MySQLAdapter 初始化: host=%s, port=%s, database=%s",
                     cfg.get("host"), cfg.get("port"), cfg.get("database"))

    def create_connection(self, use_dict_cursor: bool = False):
        import pymysql
        from pymysql.cursors import SSDictCursor

        cursor_cls = SSDictCursor if use_dict_cursor else None
        conn = pymysql.connect(
            host=self.cfg["host"],
            port=int(self.cfg["port"]),
            user=self.cfg["user"],
            password=self.cfg["password"],
            database=self.cfg["database"],
            charset=self.cfg.get("charset", "utf8mb4"),
            cursorclass=cursor_cls,
            read_timeout=3600,
            write_timeout=3600,
        )
        logger.debug("MySQL 连接已建立: %s:%d/%s",
                     self.cfg["host"], int(self.cfg["port"]), self.cfg["database"])
        return conn

    def close_connection(self, conn):
        try:
            conn.close()
            logger.debug("MySQL 连接已关闭")
        except Exception:
            pass

    def list_tables(self, conn) -> List[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME",
                (self.cfg["database"],),
            )
            tables = [r[0] for r in cur.fetchall()]
        logger.debug("MySQL 获取表列表: %d 个表", len(tables))
        return tables

    def get_table_columns(self, conn, table_name: str) -> List[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
                (self.cfg["database"], table_name),
            )
            columns = [r[0] for r in cur.fetchall()]
        logger.debug("MySQL 获取表 %s 的列: %s", table_name, columns)
        return columns

    def get_primary_key(self, conn, table_name: str) -> Optional[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "AND CONSTRAINT_NAME = 'PRIMARY' "
                "ORDER BY ORDINAL_POSITION LIMIT 1",
                (self.cfg["database"], table_name),
            )
            row = cur.fetchone()
        pk = row[0] if row else None
        logger.debug("MySQL 表 %s 主键: %s", table_name, pk)
        return pk

    def get_table_row_count(self, conn, table_name: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_ROWS FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.cfg["database"], table_name),
            )
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                return row[0]
        cur2 = conn.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM {self.quote_identifier(table_name)}")
        cnt = cur2.fetchone()[0]
        cur2.close()
        logger.debug("MySQL 表 %s 行数: %d", table_name, cnt)
        return cnt

    def get_pk_range(self, conn, table_name: str, pk_col: str) -> Tuple[int, int]:
        qpk = self.quote_identifier(pk_col)
        qt = self.quote_identifier(table_name)
        with conn.cursor() as cur:
            cur.execute(f"SELECT MIN({qpk}), MAX({qpk}) FROM {qt}")
            row = cur.fetchone()
            if row and row[0] is not None:
                return row[0], row[1]
        return 0, 0

    def get_create_table_ddl(self, conn, table_name: str) -> str:
        with conn.cursor() as cur:
            cur.execute("SHOW CREATE TABLE %s" % self.quote_identifier(table_name))
            ddl = cur.fetchone()[1]
        logger.debug("MySQL 获取表 %s 的 DDL (长度=%d)", table_name, len(ddl))
        return ddl

    def fetch_chunk(self, conn, table_name: str, columns: List[str],
                    pk_col: str, start_pk: int, end_pk: int,
                    batch_size: int):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        qpk = self.quote_identifier(pk_col)
        qt = self.quote_identifier(table_name)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {col_list} FROM {qt} "
                f"WHERE {qpk} >= %s AND {qpk} <= %s "
                f"ORDER BY {qpk}",
                (start_pk, end_pk),
            )
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

    def fetch_all_streaming(self, conn, table_name: str, columns: List[str],
                            batch_size: int):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        qt = self.quote_identifier(table_name)
        with conn.cursor() as cur:
            cur.execute(f"SELECT {col_list} FROM {qt}")
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

    def table_exists(self, conn, table_name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.cfg["database"], table_name),
            )
            return cur.fetchone()[0] > 0

    def create_table_from_ddl(self, conn, ddl: str):
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        logger.info("MySQL 执行 DDL 创建表成功")

    def truncate_table(self, conn, table_name: str):
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {self.quote_identifier(table_name)}")
        conn.commit()
        logger.info("MySQL 清空表 %s", table_name)

    def batch_insert(self, conn, table_name: str, columns: List[str],
                     rows: List[tuple]):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        qt = self.quote_identifier(table_name)
        sql = f"INSERT INTO {qt} ({col_list}) VALUES ({placeholders})"
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

    def test_connection(self, cfg: Dict[str, Any] = None) -> Tuple[bool, str]:
        import pymysql
        test_cfg = cfg or self.cfg
        try:
            conn = pymysql.connect(
                host=test_cfg["host"],
                port=int(test_cfg["port"]),
                user=test_cfg["user"],
                password=test_cfg["password"],
                database=test_cfg["database"],
                connect_timeout=5,
            )
            conn.close()
            msg = f"MySQL 连接成功: {test_cfg['host']}:{test_cfg['port']}/{test_cfg['database']}"
            logger.info(msg)
            return True, msg
        except Exception as e:
            msg = f"MySQL 连接失败: {e}"
            logger.error(msg)
            return False, msg

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    # ------------------------------------------------------------------ #
    #                        CDC (Change Data Capture)
    # ------------------------------------------------------------------ #

    def supports_cdc(self) -> bool:
        return True

    def create_cdc_stream(self, tables: List[str], callback, **kwargs):
        """
        创建 MySQL binlog 数据流。

        使用 pymysqlreplication 库监听 binlog 事件。
        需要 MySQL 服务器开启 binlog，且用户具有 REPLICATION SLAVE 和 REPLICATION CLIENT 权限。

        Args:
            tables: 要监听的表名列表，格式: ["db.table1", "db.table2"] 或 ["table1", "table2"]
            callback: 事件回调函数，参数: (event_type, table_name, data)
                      event_type: "insert", "update", "delete"
            **kwargs: 可选参数
                      - server_id: MySQL 从服务器 ID，默认 100
                      - blocking: 是否阻塞，默认 True
                      - only_schemas: 监听的数据库列表
                      - resume_stream: 是否从上次位置继续
                      - log_file: binlog 文件名（断点续传用）
                      - log_pos: binlog 位置（断点续传用）

        Returns:
            BinLogStreamReader 对象，调用 stream.close() 停止
        """
        from pymysqlreplication import BinLogStreamReader
        from pymysqlreplication.row_event import (
            DeleteRowsEvent,
            UpdateRowsEvent,
            WriteRowsEvent,
        )

        server_id = kwargs.get("server_id", 100)
        blocking = kwargs.get("blocking", True)
        only_schemas = kwargs.get("only_schemas", [self.cfg["database"]])
        only_tables = kwargs.get("only_tables", tables)
        resume_stream = kwargs.get("resume_stream", False)
        log_file = kwargs.get("log_file")
        log_pos = kwargs.get("log_pos")

        mysql_settings = {
            "host": self.cfg["host"],
            "port": int(self.cfg["port"]),
            "user": self.cfg["user"],
            "passwd": self.cfg["password"],
        }

        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=server_id,
            blocking=blocking,
            resume_stream=resume_stream,
            only_schemas=only_schemas,
            only_tables=only_tables if only_tables else None,
            log_file=log_file,
            log_pos=log_pos,
        )

        def _event_processor():
            for binlog_event in stream:
                try:
                    if isinstance(binlog_event, WriteRowsEvent):
                        event_type = "insert"
                        for row in binlog_event.rows:
                            callback(event_type, binlog_event.table, row["values"])
                    elif isinstance(binlog_event, UpdateRowsEvent):
                        event_type = "update"
                        for row in binlog_event.rows:
                            callback(event_type, binlog_event.table, row["after_values"])
                    elif isinstance(binlog_event, DeleteRowsEvent):
                        event_type = "delete"
                        for row in binlog_event.rows:
                            callback(event_type, binlog_event.table, row["values"])
                except Exception as e:
                    logger.error("处理 binlog 事件出错: %s", e)
                    continue

        import threading
        t = threading.Thread(target=_event_processor, daemon=True)
        t.start()

        return stream, t

    def get_current_binlog_position(self, conn) -> Optional[Dict[str, Any]]:
        """获取当前 binlog 位置。"""
        try:
            cur = conn.cursor()
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
            cur.close()
            if row:
                return {
                    "log_file": row[0],
                    "log_pos": row[1],
                    "binlog_do_db": row[2] if len(row) > 2 else "",
                    "binlog_ignore_db": row[3] if len(row) > 3 else "",
                }
            return None
        except Exception as e:
            logger.error("获取 binlog 位置失败: %s", e)
            return None

    def upsert_row(self, conn, table_name: str, data: Dict[str, Any], primary_key: str):
        """MySQL 插入或更新一行数据。"""
        qt = self.quote_identifier(table_name)
        qpk = self.quote_identifier(primary_key)
        columns = list(data.keys())
        values = list(data.values())

        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_list = ", ".join(f"{self.quote_identifier(c)} = %s" for c in columns)

        sql = f"""
            INSERT INTO {qt} ({col_list}) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_list}
        """

        cur = conn.cursor()
        cur.execute(sql, values + values)
        conn.commit()
        cur.close()

    def delete_row(self, conn, table_name: str, primary_key: str, pk_value: Any):
        """MySQL 删除一行数据。"""
        qt = self.quote_identifier(table_name)
        qpk = self.quote_identifier(primary_key)
        sql = f"DELETE FROM {qt} WHERE {qpk} = %s"

        cur = conn.cursor()
        cur.execute(sql, (pk_value,))
        conn.commit()
        cur.close()


# ====================================================================== #
#                        DB2 适配器实现
# ====================================================================== #

class DB2Adapter(BaseDBAdapter):
    """
    IBM DB2 数据库适配器。

    基于 ibm_db / ibm_db_dbi 驱动，实现 BaseDBAdapter 定义的所有接口。
    适用于 DB2 LUW 10.5+ / DB2 on Cloud。

    注意：
    - DB2 的 schema 概念对应 MySQL 的 database
    - DB2 使用双引号引用标识符，且默认大写
    - information_schema 不存在于 DB2，使用 SYSCAT 系统目录视图替代
    """

    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.schema = cfg.get("database", "").upper()
        logger.debug("DB2Adapter 初始化: host=%s, port=%s, schema=%s",
                     cfg.get("host"), cfg.get("port"), self.schema)

    def create_connection(self, use_dict_cursor: bool = False):
        import ibm_db_dbi

        conn_str = (
            f"DATABASE={self.cfg['database']};"
            f"HOSTNAME={self.cfg['host']};"
            f"PORT={int(self.cfg['port'])};"
            f"PROTOCOL=TCPIP;"
            f"UID={self.cfg['user']};"
            f"PWD={self.cfg['password']};"
        )
        ibm_conn = ibm_db_dbi.connect(conn_str, "", "")
        logger.debug("DB2 连接已建立: %s:%d/%s",
                     self.cfg["host"], int(self.cfg["port"]), self.cfg["database"])
        return ibm_conn

    def close_connection(self, conn):
        try:
            conn.close()
            logger.debug("DB2 连接已关闭")
        except Exception:
            pass

    def _set_schema(self, conn):
        """设置当前 schema，确保后续查询在正确的 schema 下执行。"""
        cur = conn.cursor()
        cur.execute(f"SET CURRENT SCHEMA = {self.quote_identifier(self.schema)}")
        cur.close()

    def list_tables(self, conn) -> List[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABNAME FROM SYSCAT.TABLES "
            "WHERE TABSCHEMA = ? AND TYPE = 'T' "
            "ORDER BY TABNAME",
            (self.schema,),
        )
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
        logger.debug("DB2 获取表列表: %d 个表", len(tables))
        return tables

    def get_table_columns(self, conn, table_name: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT COLNAME FROM SYSCAT.COLUMNS "
            "WHERE TABSCHEMA = ? AND TABNAME = ? ORDER BY COLNO",
            (self.schema, table_name.upper()),
        )
        columns = [r[0] for r in cur.fetchall()]
        cur.close()
        logger.debug("DB2 获取表 %s 的列: %s", table_name, columns)
        return columns

    def get_primary_key(self, conn, table_name: str) -> Optional[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT COLNAMES FROM SYSCAT.INDEXES "
            "WHERE TABSCHEMA = ? AND TABNAME = ? AND UNIQUERULE = 'P' "
            "FETCH FIRST 1 ROWS ONLY",
            (self.schema, table_name.upper()),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            pk_str = row[0].strip()
            if pk_str.startswith('"') and pk_str.endswith('"'):
                pk_col = pk_str.strip('"')
            else:
                pk_col = pk_str.split("+")[0].strip()
            logger.debug("DB2 表 %s 主键: %s", table_name, pk_col)
            return pk_col
        logger.debug("DB2 表 %s 无主键", table_name)
        return None

    def get_table_row_count(self, conn, table_name: str) -> int:
        cur = conn.cursor()
        cur.execute(
            "SELECT CARD FROM SYSCAT.TABLES "
            "WHERE TABSCHEMA = ? AND TABNAME = ?",
            (self.schema, table_name.upper()),
        )
        row = cur.fetchone()
        if row and row[0] and row[0] > 0:
            cur.close()
            return row[0]
        cur.execute(f"SELECT COUNT(*) FROM {self.quote_identifier(table_name)}")
        cnt = cur.fetchone()[0]
        cur.close()
        logger.debug("DB2 表 %s 行数: %d", table_name, cnt)
        return cnt

    def get_pk_range(self, conn, table_name: str, pk_col: str) -> Tuple[int, int]:
        qpk = self.quote_identifier(pk_col)
        qt = self.quote_identifier(table_name)
        cur = conn.cursor()
        cur.execute(f"SELECT MIN({qpk}), MAX({qpk}) FROM {qt}")
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            return row[0], row[1]
        return 0, 0

    def get_create_table_ddl(self, conn, table_name: str) -> str:
        """
        DB2 不支持 SHOW CREATE TABLE，使用 db2look 命令或手工拼 DDL。
        此处采用 SYSCAT 系统目录视图重建 DDL。
        """
        columns = self.get_table_columns(conn, table_name)
        col_defs = []
        cur = conn.cursor()
        for col in columns:
            cur.execute(
                "SELECT TYPENAME, LENGTH, SCALE, NULLS "
                "FROM SYSCAT.COLUMNS "
                "WHERE TABSCHEMA = ? AND TABNAME = ? AND COLNAME = ?",
                (self.schema, table_name.upper(), col.upper()),
            )
            info = cur.fetchone()
            if info:
                type_name, length, scale, nulls = info
                if type_name in ("VARCHAR", "CHARACTER"):
                    col_def = f"{col} {type_name}({length})"
                elif type_name in ("DECIMAL", "NUMERIC"):
                    col_def = f"{col} {type_name}({length},{scale})"
                elif type_name in ("INTEGER", "BIGINT", "SMALLINT", "DATE", "TIME", "TIMESTAMP"):
                    col_def = f"{col} {type_name}"
                else:
                    col_def = f"{col} {type_name}({length})" if length else f"{col} {type_name}"
                if nulls == "N":
                    col_def += " NOT NULL"
                col_defs.append(col_def)
        cur.close()

        ddl = f"CREATE TABLE {self.quote_identifier(table_name)} (\n  " + \
              ",\n  ".join(col_defs) + "\n)"
        logger.debug("DB2 重建表 %s 的 DDL (长度=%d)", table_name, len(ddl))
        return ddl

    def fetch_chunk(self, conn, table_name: str, columns: List[str],
                    pk_col: str, start_pk: int, end_pk: int,
                    batch_size: int):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        qpk = self.quote_identifier(pk_col)
        qt = self.quote_identifier(table_name)
        cur = conn.cursor()
        cur.execute(
            f"SELECT {col_list} FROM {qt} "
            f"WHERE {qpk} >= ? AND {qpk} <= ? "
            f"ORDER BY {qpk}",
            (start_pk, end_pk),
        )
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]
        cur.close()

    def fetch_all_streaming(self, conn, table_name: str, columns: List[str],
                            batch_size: int):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        qt = self.quote_identifier(table_name)
        cur = conn.cursor()
        cur.execute(f"SELECT {col_list} FROM {qt}")
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]
        cur.close()

    def table_exists(self, conn, table_name: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM SYSCAT.TABLES "
            "WHERE TABSCHEMA = ? AND TABNAME = ?",
            (self.schema, table_name.upper()),
        )
        count = cur.fetchone()[0]
        cur.close()
        return count > 0

    def create_table_from_ddl(self, conn, ddl: str):
        cur = conn.cursor()
        cur.execute(ddl)
        cur.close()
        conn.commit()
        logger.info("DB2 执行 DDL 创建表成功")

    def truncate_table(self, conn, table_name: str):
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {self.quote_identifier(table_name)}")
        cur.close()
        conn.commit()
        logger.info("DB2 清空表 %s (使用 DELETE)", table_name)

    def batch_insert(self, conn, table_name: str, columns: List[str],
                     rows: List[tuple]):
        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        qt = self.quote_identifier(table_name)
        sql = f"INSERT INTO {qt} ({col_list}) VALUES ({placeholders})"
        cur = conn.cursor()
        cur.executemany(sql, rows)
        cur.close()
        conn.commit()

    def test_connection(self, cfg: Dict[str, Any] = None) -> Tuple[bool, str]:
        import ibm_db_dbi

        test_cfg = cfg or self.cfg
        try:
            conn_str = (
                f"DATABASE={test_cfg['database']};"
                f"HOSTNAME={test_cfg['host']};"
                f"PORT={int(test_cfg['port'])};"
                f"PROTOCOL=TCPIP;"
                f"UID={test_cfg['user']};"
                f"PWD={test_cfg['password']};"
            )
            conn = ibm_db_dbi.connect(conn_str, "", "")
            conn.close()
            msg = f"DB2 连接成功: {test_cfg['host']}:{test_cfg['port']}/{test_cfg['database']}"
            logger.info(msg)
            return True, msg
        except Exception as e:
            msg = f"DB2 连接失败: {e}"
            logger.error(msg)
            return False, msg

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    # ------------------------------------------------------------------ #
    #                        CDC (Change Data Capture)
    # ------------------------------------------------------------------ #

    def supports_cdc(self) -> bool:
        return False

    def upsert_row(self, conn, table_name: str, data: Dict[str, Any], primary_key: str):
        """DB2 插入或更新一行数据（使用 MERGE 语句）。"""
        qt = self.quote_identifier(table_name)
        qpk = self.quote_identifier(primary_key)
        columns = list(data.keys())
        values = list(data.values())
        pk_value = data.get(primary_key)

        col_list = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        update_list = ", ".join(f"{self.quote_identifier(c)} = ?" for c in columns if c != primary_key)
        update_values = [v for k, v in data.items() if k != primary_key]

        sql = f"""
            MERGE INTO {qt} AS t
            USING (VALUES (?)) AS s({qpk})
            ON t.{qpk} = s.{qpk}
            WHEN MATCHED THEN
                UPDATE SET {update_list}
            WHEN NOT MATCHED THEN
                INSERT ({col_list}) VALUES ({placeholders})
        """

        cur = conn.cursor()
        cur.execute(sql, [pk_value] + update_values + values)
        conn.commit()
        cur.close()

    def delete_row(self, conn, table_name: str, primary_key: str, pk_value: Any):
        """DB2 删除一行数据。"""
        qt = self.quote_identifier(table_name)
        qpk = self.quote_identifier(primary_key)
        sql = f"DELETE FROM {qt} WHERE {qpk} = ?"

        cur = conn.cursor()
        cur.execute(sql, (pk_value,))
        conn.commit()
        cur.close()


# ====================================================================== #
#                        适配器工厂
# ====================================================================== #

_ADAPTER_REGISTRY = {
    "mysql": MySQLAdapter,
    "db2": DB2Adapter,
}


def get_adapter(cfg: Dict[str, Any]) -> BaseDBAdapter:
    """
    适配器工厂方法：根据配置中的 type 字段创建对应的数据库适配器。

    支持的类型：
    - "mysql" → MySQLAdapter (pymysql)
    - "db2"   → DB2Adapter   (ibm_db)

    Args:
        cfg: 数据库连接配置字典，必须包含 type 字段

    Returns:
        BaseDBAdapter 实例

    Raises:
        ValueError: 不支持的数据库类型
    """
    db_type = cfg.get("type", "mysql").lower()
    adapter_cls = _ADAPTER_REGISTRY.get(db_type)
    if adapter_cls is None:
        supported = ", ".join(_ADAPTER_REGISTRY.keys())
        raise ValueError(f"不支持的数据库类型: '{db_type}'，当前支持: {supported}")

    logger.info("创建数据库适配器: type=%s, host=%s, port=%s, database=%s",
                db_type, cfg.get("host"), cfg.get("port"), cfg.get("database"))
    return adapter_cls(cfg)


def get_supported_types() -> List[str]:
    """返回当前支持的数据库类型列表。"""
    return list(_ADAPTER_REGISTRY.keys())
