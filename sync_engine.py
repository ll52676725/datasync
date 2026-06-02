import time
import logging
import pymysql
from pymysql.cursors import SSDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

PROGRESS_TABLE = "sync_progress"


def create_connection(cfg, use_dict_cursor=False):
    cursor_cls = SSDictCursor if use_dict_cursor else None
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=cursor_cls,
        read_timeout=3600,
        write_timeout=3600,
    )


def ensure_progress_table(tgt_cfg):
    conn = create_connection(tgt_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (tgt_cfg["database"], PROGRESS_TABLE),
            )
            if cur.fetchone()[0] == 0:
                cur.execute(f"""
                    CREATE TABLE `{PROGRESS_TABLE}` (
                        `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `table_name` VARCHAR(128) NOT NULL UNIQUE,
                        `total_rows` BIGINT NOT NULL DEFAULT 0,
                        `synced_rows` BIGINT NOT NULL DEFAULT 0,
                        `primary_key` VARCHAR(128) NOT NULL DEFAULT '',
                        `last_pk_value` BIGINT NOT NULL DEFAULT 0,
                        `status` VARCHAR(32) NOT NULL DEFAULT 'pending',
                        `started_at` DATETIME NULL,
                        `finished_at` DATETIME NULL,
                        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        INDEX `idx_status` (`status`),
                        INDEX `idx_table_name` (`table_name`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                conn.commit()
                logger.info("Created progress tracking table: %s", PROGRESS_TABLE)
    finally:
        conn.close()


def get_progress(tgt_conn, table_name):
    with tgt_conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM `{PROGRESS_TABLE}` WHERE `table_name` = %s",
            (table_name,),
        )
        row = cur.fetchone()
        if row:
            columns = [d[0] for d in cur.description]
            return dict(zip(columns, row))
    return None


def init_progress(tgt_conn, table_name, total_rows, primary_key):
    with tgt_conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{PROGRESS_TABLE}` "
            "(`table_name`, `total_rows`, `primary_key`, `status`, `started_at`) "
            "VALUES (%s, %s, %s, 'running', NOW()) "
            "ON DUPLICATE KEY UPDATE "
            "`total_rows` = VALUES(`total_rows`), "
            "`primary_key` = VALUES(`primary_key`), "
            "`status` = 'running', "
            "`started_at` = NOW()",
            (table_name, total_rows, primary_key),
        )
        tgt_conn.commit()


def update_progress(tgt_conn, table_name, synced_rows, last_pk_value):
    with tgt_conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{PROGRESS_TABLE}` SET "
            "`synced_rows` = %s, `last_pk_value` = %s, `updated_at` = NOW() "
            "WHERE `table_name` = %s",
            (synced_rows, last_pk_value, table_name),
        )
        tgt_conn.commit()


def mark_progress_done(tgt_conn, table_name, total_synced):
    with tgt_conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{PROGRESS_TABLE}` SET "
            "`synced_rows` = %s, `status` = 'done', `finished_at` = NOW() "
            "WHERE `table_name` = %s",
            (total_synced, table_name),
        )
        tgt_conn.commit()


def mark_progress_failed(tgt_conn, table_name, error):
    with tgt_conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{PROGRESS_TABLE}` SET `status` = 'failed', `updated_at` = NOW() "
            "WHERE `table_name` = %s",
            (table_name,),
        )
        tgt_conn.commit()


def reset_progress(tgt_cfg, table_name=None):
    ensure_progress_table(tgt_cfg)
    conn = create_connection(tgt_cfg)
    try:
        with conn.cursor() as cur:
            if table_name:
                cur.execute(
                    f"DELETE FROM `{PROGRESS_TABLE}` WHERE `table_name` = %s",
                    (table_name,),
                )
                logger.info("Reset progress for table: %s", table_name)
            else:
                cur.execute(f"TRUNCATE TABLE `{PROGRESS_TABLE}`")
                logger.info("Reset all progress")
        conn.commit()
    finally:
        conn.close()


def get_table_row_count(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TABLE_ROWS FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (conn.db or conn.database, table_name),
        )
        row = cur.fetchone()
        if row and row[0] and row[0] > 0:
            return row[0]
    cur2 = conn.cursor()
    cur2.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    cnt = cur2.fetchone()[0]
    cur2.close()
    return cnt


def get_table_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (conn.db or conn.database, table_name),
        )
        return [r[0] for r in cur.fetchall()]


def get_primary_key(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "AND CONSTRAINT_NAME = 'PRIMARY' "
            "ORDER BY ORDINAL_POSITION LIMIT 1",
            (conn.db or conn.database, table_name),
        )
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def get_pk_range(conn, table_name, pk_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{table_name}`")
        row = cur.fetchone()
        if row and row[0] is not None:
            return row[0], row[1]
    return 0, 0


def ensure_target_table(src_cfg, tgt_cfg, table_name, resume_mode=False):
    src_conn = create_connection(src_cfg)
    tgt_conn = create_connection(tgt_cfg)
    try:
        with src_conn.cursor() as cur:
            cur.execute("SHOW CREATE TABLE `%s`" % table_name)
            ddl = cur.fetchone()[1]
        with tgt_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (tgt_cfg["database"], table_name),
            )
            if cur.fetchone()[0] == 0:
                tgt_cur = tgt_conn.cursor()
                tgt_cur.execute(ddl)
                tgt_cur.close()
                tgt_conn.commit()
                logger.info("Created target table: %s", table_name)
            elif not resume_mode:
                tgt_conn.cursor().execute(f"TRUNCATE TABLE `{table_name}`")
                tgt_conn.commit()
                logger.info("Truncated target table: %s", table_name)
            else:
                logger.info("Target table %s exists (resume mode, keeping data)", table_name)
    finally:
        src_conn.close()
        tgt_conn.close()


def sync_table(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size, resume_mode=False):
    logger.info("Starting sync for table: %s (resume=%s)", table_name, resume_mode)
    start_time = time.time()

    src_conn = create_connection(src_cfg, use_dict_cursor=True)
    tgt_conn = create_connection(tgt_cfg)

    synced_rows = 0
    try:
        columns = get_table_columns(src_conn, table_name)
        if not columns:
            logger.warning("No columns found for table %s, skipping", table_name)
            return {"table": table_name, "rows": 0, "time": 0, "error": "no columns"}

        pk_col = get_primary_key(src_conn, table_name)
        if not pk_col:
            logger.error(
                "Table %s has no primary key, cannot support resume-based chunking. "
                "Falling back to simple streaming.",
                table_name,
            )
            return _sync_table_streaming(
                src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
            )

        total_rows = get_table_row_count(src_conn, table_name)
        min_pk, max_pk = get_pk_range(src_conn, table_name, pk_col)
        logger.info(
            "Table %s: %s [%d -> %d], ~%d rows",
            table_name, pk_col, min_pk, max_pk, total_rows,
        )

        progress = get_progress(tgt_conn, table_name)
        last_pk = min_pk - 1

        if resume_mode and progress:
            status = progress.get("status")
            last_pk = progress.get("last_pk_value", min_pk - 1)
            synced_rows = progress.get("synced_rows", 0)

            if status == "done":
                logger.info("Table %s already completed (%d rows), skipping", table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            elif last_pk >= max_pk:
                logger.info(
                    "Table %s progress shows all rows synced (last_pk=%d >= max_pk=%d), marking done",
                    table_name, last_pk, max_pk,
                )
                mark_progress_done(tgt_conn, table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            else:
                logger.info(
                    "Resuming table %s from pk=%d (%d rows already synced)",
                    table_name, last_pk, synced_rows,
                )

        init_progress(tgt_conn, table_name, total_rows, pk_col)

        col_list = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"

        current_start = max(last_pk + 1, min_pk)
        chunk_end = min(current_start + chunk_size - 1, max_pk)
        last_report = time.time()

        while current_start <= max_pk:
            chunk_end = min(current_start + chunk_size - 1, max_pk)

            with src_conn.cursor() as cur:
                cur.execute(
                    f"SELECT {col_list} FROM `{table_name}` "
                    f"WHERE `{pk_col}` >= %s AND `{pk_col}` <= %s "
                    f"ORDER BY `{pk_col}`",
                    (current_start, chunk_end),
                )

                batch = []
                chunk_synced = 0
                while True:
                    rows = cur.fetchmany(batch_insert_size)
                    if not rows:
                        break

                    for row in rows:
                        batch.append(tuple(row[c] for c in columns))
                        chunk_synced += 1

                        if len(batch) >= batch_insert_size:
                            with tgt_conn.cursor() as tgt_cur:
                                tgt_cur.executemany(insert_sql, batch)
                            tgt_conn.commit()
                            synced_rows += len(batch)
                            update_progress(tgt_conn, table_name, synced_rows, chunk_end)
                            batch = []

                if batch:
                    with tgt_conn.cursor() as tgt_cur:
                        tgt_cur.executemany(insert_sql, batch)
                    tgt_conn.commit()
                    synced_rows += len(batch)
                    batch = []

            update_progress(tgt_conn, table_name, synced_rows, chunk_end)

            now = time.time()
            if now - last_report >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "Table %s progress: %d / ~%d rows (%.1f%%), pk=%d / %d",
                    table_name, synced_rows, total_rows, pct, chunk_end, max_pk,
                )
                last_report = now

            current_start = chunk_end + 1

        mark_progress_done(tgt_conn, table_name, synced_rows)

    except Exception as e:
        logger.error("Error syncing table %s: %s", table_name, e)
        mark_progress_failed(tgt_conn, table_name, str(e))
        return {"table": table_name, "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_conn.close()
        tgt_conn.close()

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "Finished table %s: %d rows in %.1fs (%.0f rows/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {"table": table_name, "rows": synced_rows, "time": elapsed, "error": None}


def _sync_table_streaming(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size):
    logger.info("Streaming sync (no PK) for table: %s", table_name)
    start_time = time.time()

    src_conn = create_connection(src_cfg, use_dict_cursor=True)
    tgt_conn = create_connection(tgt_cfg)
    synced_rows = 0

    try:
        columns = get_table_columns(src_conn, table_name)
        col_list = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"

        total_rows = get_table_row_count(src_conn, table_name)
        logger.info("Table %s has ~%d rows (streaming mode)", table_name, total_rows)

        with src_conn.cursor() as cur:
            cur.execute(f"SELECT {col_list} FROM `{table_name}`")
            batch = []
            last_report = time.time()

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break

                for row in rows:
                    batch.append(tuple(row[c] for c in columns))

                    if len(batch) >= batch_insert_size:
                        with tgt_conn.cursor() as tgt_cur:
                            tgt_cur.executemany(insert_sql, batch)
                        tgt_conn.commit()
                        synced_rows += len(batch)
                        batch = []

                if batch:
                    with tgt_conn.cursor() as tgt_cur:
                        tgt_cur.executemany(insert_sql, batch)
                    tgt_conn.commit()
                    synced_rows += len(batch)
                    batch = []

                now = time.time()
                if now - last_report >= 5.0:
                    pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                    logger.info(
                        "Table %s (stream) progress: %d / ~%d rows (%.1f%%)",
                        table_name, synced_rows, total_rows, pct,
                    )
                    last_report = now

    except Exception as e:
        logger.error("Error streaming table %s: %s", table_name, e)
        return {"table": table_name, "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_conn.close()
        tgt_conn.close()

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "Finished streaming table %s: %d rows in %.1fs (%.0f rows/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {"table": table_name, "rows": synced_rows, "time": elapsed, "error": None}


def run_sync(config, resume_mode=False, restart=False):
    src_cfg = config["source"]
    tgt_cfg = config["target"]
    sync_cfg = config["sync"]

    parallel = sync_cfg.get("parallel_tables", 4)
    chunk_size = sync_cfg.get("chunk_size", 50000)
    batch_insert_size = sync_cfg.get("batch_insert_size", 5000)
    tables = sync_cfg.get("tables", [])

    ensure_progress_table(tgt_cfg)

    if restart:
        reset_progress(tgt_cfg)
        resume_mode = False
        logger.info("Restart mode: all progress reset, will truncate and re-sync")

    if not tables:
        logger.info("No tables specified, discovering all tables from source...")
        conn = create_connection(src_cfg)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
                    (src_cfg["database"],),
                )
                tables = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()

    logger.info("Tables to sync: %s", tables)

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
                logger.error("Table %s failed: %s", tbl, e)
                results.append({"table": tbl, "rows": 0, "time": 0, "error": str(e)})

    total_elapsed = time.time() - total_start
    total_rows = sum(r["rows"] for r in results)
    skipped = sum(1 for r in results if r.get("skipped"))
    errors = [r for r in results if r["error"]]

    logger.info("=" * 60)
    logger.info("SYNC COMPLETE")
    logger.info("Total rows synced: %d", total_rows)
    logger.info("Tables skipped (already done): %d", skipped)
    logger.info("Total time: %.1fs", total_elapsed)
    logger.info("Overall speed: %.0f rows/s", total_rows / total_elapsed if total_elapsed > 0 else 0)
    if errors:
        logger.error("Tables with errors: %s", [e["table"] for e in errors])
    for r in results:
        status = "OK" if not r["error"] else f"ERROR: {r['error']}"
        if r.get("skipped"):
            status = "SKIPPED (already done)"
        logger.info("  %-40s %10d rows  %8.1fs  %s", r["table"], r["rows"], r["time"], status)
    logger.info("=" * 60)

    return results
