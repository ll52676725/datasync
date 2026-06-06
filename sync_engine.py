"""
sync_engine.py — 多数据库同步引擎

基于适配器模式重构，支持以下同步组合：
  - mysql → mysql
  - db2   → db2
  - mysql → db2
  - db2   → mysql

核心流程：
  1. 根据配置中的 type 字段创建对应的数据库适配器
  2. 使用适配器的统一接口进行元数据查询、数据读取和写入
  3. 通过断点续传机制支持大表分块同步和中断恢复

使用方式：
  config 中 source/target 节需包含 type 字段（"mysql" 或 "db2"）
  其余字段与原格式兼容（host/port/user/password/database）
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_adapter import get_adapter, BaseDBAdapter

logger = logging.getLogger(__name__)

PROGRESS_TABLE = "sync_progress"


# ====================================================================== #
#                        进度跟踪表管理
# ====================================================================== #

def ensure_progress_table(tgt_cfg):
    """
    在目标数据库中创建断点进度跟踪表（如果不存在）。

    该表记录每张同步表的进度信息，包括：
    - total_rows / synced_rows: 总行数 / 已同步行数
    - primary_key / last_pk_value: 主键列名 / 最后处理的主键值
    - status: 当前状态 (pending/running/done/failed)

    Args:
        tgt_cfg: 目标数据库连接配置
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        if adapter.table_exists(conn, PROGRESS_TABLE):
            logger.debug("进度跟踪表 %s 已存在", PROGRESS_TABLE)
            return

        if adapter.db_type == "mysql":
            ddl = (
                f"CREATE TABLE `{PROGRESS_TABLE}` ("
                "`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                "`table_name` VARCHAR(128) NOT NULL UNIQUE,"
                "`total_rows` BIGINT NOT NULL DEFAULT 0,"
                "`synced_rows` BIGINT NOT NULL DEFAULT 0,"
                "`primary_key` VARCHAR(128) NOT NULL DEFAULT '',"
                "`last_pk_value` BIGINT NOT NULL DEFAULT 0,"
                "`status` VARCHAR(32) NOT NULL DEFAULT 'pending',"
                "`started_at` DATETIME NULL,"
                "`finished_at` DATETIME NULL,"
                "`updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                "INDEX `idx_status` (`status`),"
                "INDEX `idx_table_name` (`table_name`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
        elif adapter.db_type == "oracle":
            ddl = (
                f'CREATE TABLE "{PROGRESS_TABLE}" ('
                '"id" NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,'
                '"table_name" VARCHAR2(128) NOT NULL UNIQUE,'
                '"total_rows" NUMBER(19) NOT NULL DEFAULT 0,'
                '"synced_rows" NUMBER(19) NOT NULL DEFAULT 0,'
                '"primary_key" VARCHAR2(128) NOT NULL DEFAULT \'\','
                '"last_pk_value" NUMBER(19) NOT NULL DEFAULT 0,'
                '"status" VARCHAR2(32) NOT NULL DEFAULT \'pending\','
                '"started_at" TIMESTAMP NULL,'
                '"finished_at" TIMESTAMP NULL,'
                '"updated_at" TIMESTAMP NOT NULL DEFAULT SYSTIMESTAMP'
                ')'
            )
        else:
            ddl = (
                f'CREATE TABLE "{PROGRESS_TABLE}" ('
                '"id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,'
                '"table_name" VARCHAR(128) NOT NULL UNIQUE,'
                '"total_rows" BIGINT NOT NULL DEFAULT 0,'
                '"synced_rows" BIGINT NOT NULL DEFAULT 0,'
                '"primary_key" VARCHAR(128) NOT NULL DEFAULT \'\','
                '"last_pk_value" BIGINT NOT NULL DEFAULT 0,'
                '"status" VARCHAR(32) NOT NULL DEFAULT \'pending\','
                '"started_at" TIMESTAMP NULL,'
                '"finished_at" TIMESTAMP NULL,'
                '"updated_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'
                ')'
            )
        adapter.create_table_from_ddl(conn, ddl)
        logger.info("创建进度跟踪表: %s (数据库类型: %s)", PROGRESS_TABLE, adapter.db_type)
    finally:
        adapter.close_connection(conn)


# ====================================================================== #
#                        进度查询与更新
# ====================================================================== #

def _now_sql(adapter):
    """根据数据库类型返回获取当前时间的 SQL 表达式。"""
    if adapter.db_type == "mysql":
        return "NOW()"
    elif adapter.db_type == "oracle":
        return "SYSTIMESTAMP"
    else:
        return "CURRENT TIMESTAMP"


def _placeholder(adapter, index: int = 1) -> str:
    """根据数据库类型返回参数占位符。"""
    if adapter.db_type == "mysql":
        return "%s"
    elif adapter.db_type == "oracle":
        return f":{index}"
    else:
        return "?"


def get_progress(tgt_cfg, table_name):
    """
    查询指定表的同步进度。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 表名

    Returns:
        进度字典，若不存在则返回 None
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()
        ph = _placeholder(adapter, 1)
        cur.execute(
            f"SELECT * FROM {qt} WHERE table_name = {ph}",
            (table_name,),
        )
        row = cur.fetchone()
        if row:
            columns = [d[0] for d in cur.description]
            return dict(zip(columns, row))
        return None
    finally:
        adapter.close_connection(conn)


def init_progress(tgt_cfg, table_name, total_rows, primary_key):
    """
    初始化表的同步进度记录。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 表名
        total_rows: 总行数
        primary_key: 主键列名
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()

        if adapter.db_type == "mysql":
            cur.execute(
                f"INSERT INTO {qt} "
                "(table_name, total_rows, primary_key, status, started_at) "
                "VALUES (%s, %s, %s, 'running', NOW()) "
                "ON DUPLICATE KEY UPDATE "
                "total_rows = VALUES(total_rows), "
                "primary_key = VALUES(primary_key), "
                "status = 'running', "
                "started_at = NOW()",
                (table_name, total_rows, primary_key),
            )
        elif adapter.db_type == "oracle":
            cur.execute(
                f"SELECT COUNT(*) FROM {qt} WHERE table_name = :1",
                (table_name,),
            )
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    f"UPDATE {qt} SET total_rows = :1, primary_key = :2, "
                    f"status = 'running', started_at = SYSTIMESTAMP "
                    f"WHERE table_name = :3",
                    (total_rows, primary_key, table_name),
                )
            else:
                cur.execute(
                    f"INSERT INTO {qt} "
                    "(table_name, total_rows, primary_key, status, started_at) "
                    "VALUES (:1, :2, :3, 'running', SYSTIMESTAMP)",
                    (table_name, total_rows, primary_key),
                )
        else:
            cur.execute(
                f"SELECT COUNT(*) FROM {qt} WHERE table_name = ?",
                (table_name,),
            )
            exists = cur.fetchone()[0] > 0
            if exists:
                cur.execute(
                    f"UPDATE {qt} SET total_rows = ?, primary_key = ?, "
                    f"status = 'running', started_at = CURRENT TIMESTAMP "
                    f"WHERE table_name = ?",
                    (total_rows, primary_key, table_name),
                )
            else:
                cur.execute(
                    f"INSERT INTO {qt} "
                    "(table_name, total_rows, primary_key, status, started_at) "
                    "VALUES (?, ?, ?, 'running', CURRENT TIMESTAMP)",
                    (table_name, total_rows, primary_key),
                )
        conn.commit()
        logger.debug("初始化进度: table=%s, total_rows=%d, pk=%s",
                      table_name, total_rows, primary_key)
    finally:
        adapter.close_connection(conn)


def update_progress(tgt_cfg, table_name, synced_rows, last_pk_value):
    """
    更新表的同步进度。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 表名
        synced_rows: 已同步行数
        last_pk_value: 最后处理的主键值
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()
        if adapter.db_type == "mysql":
            cur.execute(
                f"UPDATE {qt} SET synced_rows = %s, last_pk_value = %s, "
                f"updated_at = NOW() WHERE table_name = %s",
                (synced_rows, last_pk_value, table_name),
            )
        elif adapter.db_type == "oracle":
            cur.execute(
                f"UPDATE {qt} SET synced_rows = :1, last_pk_value = :2, "
                f"updated_at = SYSTIMESTAMP WHERE table_name = :3",
                (synced_rows, last_pk_value, table_name),
            )
        else:
            cur.execute(
                f"UPDATE {qt} SET synced_rows = ?, last_pk_value = ?, "
                f"updated_at = CURRENT TIMESTAMP WHERE table_name = ?",
                (synced_rows, last_pk_value, table_name),
            )
        conn.commit()
    finally:
        adapter.close_connection(conn)


def mark_progress_done(tgt_cfg, table_name, total_synced):
    """
    标记表同步完成。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 表名
        total_synced: 总同步行数
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()
        if adapter.db_type == "mysql":
            cur.execute(
                f"UPDATE {qt} SET synced_rows = %s, status = 'done', "
                f"finished_at = NOW() WHERE table_name = %s",
                (total_synced, table_name),
            )
        elif adapter.db_type == "oracle":
            cur.execute(
                f"UPDATE {qt} SET synced_rows = :1, status = 'done', "
                f"finished_at = SYSTIMESTAMP WHERE table_name = :2",
                (total_synced, table_name),
            )
        else:
            cur.execute(
                f"UPDATE {qt} SET synced_rows = ?, status = 'done', "
                f"finished_at = CURRENT TIMESTAMP WHERE table_name = ?",
                (total_synced, table_name),
            )
        conn.commit()
        logger.info("进度标记完成: table=%s, rows=%d", table_name, total_synced)
    finally:
        adapter.close_connection(conn)


def mark_progress_failed(tgt_cfg, table_name, error):
    """
    标记表同步失败。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 表名
        error: 错误信息
    """
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()
        if adapter.db_type == "mysql":
            cur.execute(
                f"UPDATE {qt} SET status = 'failed', updated_at = NOW() "
                f"WHERE table_name = %s",
                (table_name,),
            )
        elif adapter.db_type == "oracle":
            cur.execute(
                f"UPDATE {qt} SET status = 'failed', updated_at = SYSTIMESTAMP "
                f"WHERE table_name = :1",
                (table_name,),
            )
        else:
            cur.execute(
                f"UPDATE {qt} SET status = 'failed', updated_at = CURRENT TIMESTAMP "
                f"WHERE table_name = ?",
                (table_name,),
            )
        conn.commit()
        logger.error("进度标记失败: table=%s, error=%s", table_name, error)
    finally:
        adapter.close_connection(conn)


def reset_progress(tgt_cfg, table_name=None):
    """
    重置同步进度。

    Args:
        tgt_cfg: 目标数据库连接配置
        table_name: 指定表名则只重置该表，None 则重置所有
    """
    ensure_progress_table(tgt_cfg)
    adapter = get_adapter(tgt_cfg)
    conn = adapter.create_connection()
    try:
        qt = adapter.quote_identifier(PROGRESS_TABLE)
        cur = conn.cursor()
        if table_name:
            if adapter.db_type == "mysql":
                cur.execute(f"DELETE FROM {qt} WHERE table_name = %s", (table_name,))
            elif adapter.db_type == "oracle":
                cur.execute(f"DELETE FROM {qt} WHERE table_name = :1", (table_name,))
            else:
                cur.execute(f"DELETE FROM {qt} WHERE table_name = ?", (table_name,))
            logger.info("重置表进度: %s", table_name)
        else:
            if adapter.db_type == "mysql":
                cur.execute(f"TRUNCATE TABLE {qt}")
            else:
                cur.execute(f"DELETE FROM {qt}")
            logger.info("重置所有进度")
        conn.commit()
    finally:
        adapter.close_connection(conn)


# ====================================================================== #
#                        目标表准备
# ====================================================================== #

def ensure_target_table(src_cfg, tgt_cfg, table_name, resume_mode=False):
    """
    确保目标数据库中存在与源表结构一致的表。

    如果目标表不存在，则从源库获取 DDL 并在目标库创建。
    如果目标表已存在且非断点续传模式，则清空目标表数据。

    注意：跨数据库类型（如 mysql→db2）时，DDL 可能需要适配。
    目前直接执行源库的 DDL，如有不兼容需要用户手动调整。

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 表名
        resume_mode: 是否断点续传模式
    """
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    src_conn = src_adapter.create_connection()
    tgt_conn = tgt_adapter.create_connection()
    try:
        ddl = src_adapter.get_create_table_ddl(src_conn, table_name)
        logger.info("获取源表 %s 的 DDL (类型: %s→%s)",
                     table_name, src_adapter.db_type, tgt_adapter.db_type)

        if not tgt_adapter.table_exists(tgt_conn, table_name):
            tgt_adapter.create_table_from_ddl(tgt_conn, ddl)
            logger.info("在目标库创建表: %s", table_name)
        elif not resume_mode:
            tgt_adapter.truncate_table(tgt_conn, table_name)
            logger.info("清空目标表: %s", table_name)
        else:
            logger.info("目标表 %s 已存在 (断点续传模式，保留数据)", table_name)
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)


# ====================================================================== #
#                        表同步核心逻辑
# ====================================================================== #

def sync_table(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size, resume_mode=False):
    """
    同步单张表（主键分块模式）。

    流程：
    1. 获取源表的列信息和主键
    2. 若无主键，回退到流式同步
    3. 按主键范围分块读取源数据
    4. 批量插入到目标表
    5. 定期更新进度，支持断点续传

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 表名
        chunk_size: 每个分块的主键范围大小
        batch_insert_size: 批量插入的行数
        resume_mode: 是否断点续传

    Returns:
        同步结果字典 {table, rows, time, error}
    """
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info("开始同步表: %s (源: %s → 目标: %s, resume=%s)",
                table_name, src_adapter.db_type, tgt_adapter.db_type, resume_mode)
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    try:
        columns = src_adapter.get_table_columns(src_conn, table_name)
        if not columns:
            logger.warning("表 %s 无列信息，跳过", table_name)
            return {"table": table_name, "rows": 0, "time": 0, "error": "no columns"}

        pk_col = src_adapter.get_primary_key(src_conn, table_name)
        if not pk_col:
            logger.error(
                "表 %s 无主键，无法按主键分块同步，回退到流式模式", table_name,
            )
            src_adapter.close_connection(src_conn)
            tgt_adapter.close_connection(tgt_conn)
            return _sync_table_streaming(
                src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
            )

        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        min_pk, max_pk = src_adapter.get_pk_range(src_conn, table_name, pk_col)
        logger.info("表 %s: 主键=%s [%d → %d], ~%d 行",
                     table_name, pk_col, min_pk, max_pk, total_rows)

        progress = get_progress(tgt_cfg, table_name)
        last_pk = min_pk - 1

        if resume_mode and progress:
            status = progress.get("status")
            last_pk = progress.get("last_pk_value", min_pk - 1)
            synced_rows = progress.get("synced_rows", 0)

            if status == "done":
                logger.info("表 %s 已完成 (%d 行)，跳过", table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            elif last_pk >= max_pk:
                logger.info("表 %s 进度显示已全部同步 (last_pk=%d >= max_pk=%d)，标记完成",
                            table_name, last_pk, max_pk)
                mark_progress_done(tgt_cfg, table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            else:
                logger.info("断点续传表 %s: 从 pk=%d 继续 (%d 行已同步)",
                            table_name, last_pk, synced_rows)

        init_progress(tgt_cfg, table_name, total_rows, pk_col)

        current_start = max(last_pk + 1, min_pk)
        last_report = time.time()

        while current_start <= max_pk:
            chunk_end = min(current_start + chunk_size - 1, max_pk)

            for batch_rows in src_adapter.fetch_chunk(
                src_conn, table_name, columns, pk_col,
                current_start, chunk_end, batch_insert_size,
            ):
                insert_batch = []
                for row in batch_rows:
                    insert_batch.append(tuple(row[c] for c in columns))

                    if len(insert_batch) >= batch_insert_size:
                        tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                        synced_rows += len(insert_batch)
                        update_progress(tgt_cfg, table_name, synced_rows, chunk_end)
                        insert_batch = []

                if insert_batch:
                    tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                    synced_rows += len(insert_batch)
                    insert_batch = []

            update_progress(tgt_cfg, table_name, synced_rows, chunk_end)

            now = time.time()
            if now - last_report >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "表 %s 进度: %d / ~%d 行 (%.1f%%), pk=%d / %d",
                    table_name, synced_rows, total_rows, pct, chunk_end, max_pk,
                )
                last_report = now

            current_start = chunk_end + 1

        mark_progress_done(tgt_cfg, table_name, synced_rows)

    except Exception as e:
        logger.error("同步表 %s 出错: %s", table_name, e)
        mark_progress_failed(tgt_cfg, table_name, str(e))
        return {"table": table_name, "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "完成表 %s 同步: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {"table": table_name, "rows": synced_rows, "time": elapsed, "error": None}


def _sync_table_streaming(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size):
    """
    流式同步（无主键回退方案）。

    不依赖主键分块，直接全表扫描读取并批量写入。
    适用于无主键的表，但不支持断点续传。

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 表名
        chunk_size: 每次读取的批量大小
        batch_insert_size: 批量插入的行数

    Returns:
        同步结果字典
    """
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info("流式同步 (无主键) 表: %s (%s → %s)",
                table_name, src_adapter.db_type, tgt_adapter.db_type)
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    try:
        columns = src_adapter.get_table_columns(src_conn, table_name)
        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        logger.info("表 %s 共 ~%d 行 (流式模式)", table_name, total_rows)

        for batch_rows in src_adapter.fetch_all_streaming(
            src_conn, table_name, columns, chunk_size,
        ):
            insert_batch = []
            for row in batch_rows:
                insert_batch.append(tuple(row[c] for c in columns))

                if len(insert_batch) >= batch_insert_size:
                    tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                    synced_rows += len(insert_batch)
                    insert_batch = []

            if insert_batch:
                tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                synced_rows += len(insert_batch)
                insert_batch = []

            now = time.time()
            if now - start_time >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "表 %s (流式) 进度: %d / ~%d 行 (%.1f%%)",
                    table_name, synced_rows, total_rows, pct,
                )
                start_time_report = now

    except Exception as e:
        logger.error("流式同步表 %s 出错: %s", table_name, e)
        return {"table": table_name, "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "完成流式同步表 %s: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {"table": table_name, "rows": synced_rows, "time": elapsed, "error": None}


# ====================================================================== #
#                        同步任务入口
# ====================================================================== #

def run_sync(config, resume_mode=False, restart=False):
    """
    执行完整的数据库同步任务。

    流程：
    1. 读取配置，确定源/目标数据库类型
    2. 如果指定了 restart 模式，重置所有进度
    3. 发现需要同步的表列表
    4. 确保目标表存在（DDL 同步）
    5. 并行执行各表的同步
    6. 汇总结果

    Args:
        config: 同步配置字典，包含 source/target/sync 节
        resume_mode: 是否断点续传模式
        restart: 是否重新开始模式（清空所有数据和进度）

    Returns:
        同步结果列表
    """
    src_cfg = config["source"]
    tgt_cfg = config["target"]
    sync_cfg = config["sync"]

    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info("=" * 60)
    logger.info("同步任务启动: %s → %s", src_adapter.db_type, tgt_adapter.db_type)
    logger.info("源: %s:%d/%s", src_cfg.get("host"), int(src_cfg.get("port", 0)), src_cfg.get("database"))
    logger.info("目标: %s:%d/%s", tgt_cfg.get("host"), int(tgt_cfg.get("port", 0)), tgt_cfg.get("database"))
    logger.info("=" * 60)

    parallel = sync_cfg.get("parallel_tables", 4)
    chunk_size = sync_cfg.get("chunk_size", 50000)
    batch_insert_size = sync_cfg.get("batch_insert_size", 5000)
    tables = sync_cfg.get("tables", [])

    ensure_progress_table(tgt_cfg)

    if restart:
        reset_progress(tgt_cfg)
        resume_mode = False
        logger.info("重新开始模式: 已重置所有进度，将清空并重新同步")

    if not tables:
        logger.info("未指定同步表，正在从源数据库发现所有表...")
        conn = src_adapter.create_connection()
        try:
            tables = src_adapter.list_tables(conn)
        finally:
            src_adapter.close_connection(conn)

    logger.info("待同步表: %s", tables)

    for tbl in tables:
        ensure_target_table(src_cfg, tgt_cfg, tbl, resume_mode=resume_mode)

    total_start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for tbl in tables:
            future = executor.submit(
                sync_table, src_cfg, tgt_cfg, tbl, chunk_size, batch_insert_size, resume_mode,
            )
            futures[future] = tbl

        for future in as_completed(futures):
            tbl = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error("表 %s 同步失败: %s", tbl, e)
                results.append({"table": tbl, "rows": 0, "time": 0, "error": str(e)})

    total_elapsed = time.time() - total_start
    total_rows = sum(r["rows"] for r in results)
    skipped = sum(1 for r in results if r.get("skipped"))
    errors = [r for r in results if r["error"]]

    logger.info("=" * 60)
    logger.info("同步完成")
    logger.info("总同步行数: %d", total_rows)
    logger.info("跳过的表 (已完成): %d", skipped)
    logger.info("总耗时: %.1fs", total_elapsed)
    logger.info("总体速度: %.0f 行/s", total_rows / total_elapsed if total_elapsed > 0 else 0)
    if errors:
        logger.error("出错的表: %s", [e["table"] for e in errors])
    for r in results:
        status = "OK" if not r["error"] else f"ERROR: {r['error']}"
        if r.get("skipped"):
            status = "SKIPPED (已完成)"
        logger.info("  %-40s %10d 行  %8.1fs  %s", r["table"], r["rows"], r["time"], status)
    logger.info("=" * 60)

    return results
