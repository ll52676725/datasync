"""
sync_engine_optimized.py — 优化版多数据库同步引擎

在原有同步引擎基础上集成复杂场景优化：
1. 大事务拆分 - 避免单事务过大导致锁表和回滚段溢出
2. 智能分页 - 基于数据分布动态调整查询策略
3. 错误重试 - 批量失败降级逐行，死信队列
4. 自适应调优 - 根据数据库负载动态调整参数
5. 一致性校验 - 同步前后数据完整性校验

与 sync_engine.py 完全兼容，可无缝替换使用。
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_adapter import get_adapter, BaseDBAdapter
from sync_optimizers import (
    TransactionSplitter,
    SmartPaginator,
    AdaptiveTuner,
    RetryManager,
    ConsistencyChecker,
    PerformanceMetrics,
    create_optimizer_config,
)

logger = logging.getLogger(__name__)

PROGRESS_TABLE = "sync_progress"


# ====================================================================== #
#                        进度跟踪（复用原有逻辑）
# ====================================================================== #
# 为了保持兼容性，这里复用 sync_engine.py 的进度管理函数
# 实际使用时可以根据需要选择使用文件进度或内存进度

from sync_engine import (
    ensure_progress_table,
    get_progress,
    init_progress,
    update_progress,
    mark_progress_done,
    mark_progress_failed,
    reset_progress,
    ensure_target_table,
)


# ====================================================================== #
#                        优化版表同步核心逻辑
# ====================================================================== #

def sync_table_optimized(
    src_cfg, tgt_cfg, table_name,
    chunk_size=50000, batch_insert_size=5000,
    resume_mode=False,
    enable_transaction_splitter=True,
    enable_smart_pagination=True,
    enable_retry=True,
    enable_adaptive_tuning=True,
    enable_consistency_check=False,
    max_transaction_rows=1000,
    max_retries=3,
):
    """
    优化版单表同步。

    相比 sync_table 增加了：
    - 大事务自动拆分
    - 智能分页查询
    - 自动重试和降级
    - 自适应性能调优
    - 可选的一致性校验

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 表名
        chunk_size: 分块大小
        batch_insert_size: 批量插入大小
        resume_mode: 是否断点续传
        enable_transaction_splitter: 是否启用大事务拆分
        enable_smart_pagination: 是否启用智能分页
        enable_retry: 是否启用重试
        enable_adaptive_tuning: 是否启用自适应调优
        enable_consistency_check: 是否启用一致性校验
        max_transaction_rows: 单事务最大行数
        max_retries: 最大重试次数

    Returns:
        同步结果字典
    """
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info(
        "[优化版] 开始同步表: %s (源: %s → 目标: %s, resume=%s)",
        table_name, src_adapter.db_type, tgt_adapter.db_type, resume_mode,
    )
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    tx_splitter = TransactionSplitter(max_transaction_rows=max_transaction_rows) if enable_transaction_splitter else None
    smart_paginator = SmartPaginator() if enable_smart_pagination else None
    retry_mgr = RetryManager(max_retries=max_retries) if enable_retry else None
    adaptive_tuner = AdaptiveTuner(initial_batch_size=batch_insert_size) if enable_adaptive_tuning else None
    consistency_checker = ConsistencyChecker() if enable_consistency_check else None

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
            return _sync_table_streaming_optimized(
                src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
                enable_retry=enable_retry,
                max_retries=max_retries,
                max_transaction_rows=max_transaction_rows,
            )

        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        min_pk, max_pk = src_adapter.get_pk_range(src_conn, table_name, pk_col)
        logger.info(
            "表 %s: 主键=%s [%d → %d], ~%d 行",
            table_name, pk_col, min_pk, max_pk, total_rows,
        )

        if enable_smart_pagination and smart_paginator:
            pk_range_size = max_pk - min_pk
            optimal_chunk = smart_paginator.estimate_optimal_chunk(pk_range_size, total_rows)
            chunk_size = optimal_chunk
            logger.info("智能分页优化: 推荐 chunk_size = %d", chunk_size)

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
                logger.info(
                    "表 %s 进度显示已全部同步 (last_pk=%d >= max_pk=%d)，标记完成",
                    table_name, last_pk, max_pk,
                )
                mark_progress_done(tgt_cfg, table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            else:
                logger.info(
                    "断点续传表 %s: 从 pk=%d 继续 (%d 行已同步)",
                    table_name, last_pk, synced_rows,
                )

        init_progress(tgt_cfg, table_name, total_rows, pk_col)

        current_start = max(last_pk + 1, min_pk)
        last_report = time.time()
        batch_start_time = time.time()

        while current_start <= max_pk:
            chunk_end = min(current_start + chunk_size - 1, max_pk)

            fetch_start = time.time()
            chunk_rows_fetched = 0

            for batch_rows in src_adapter.fetch_chunk(
                src_conn, table_name, columns, pk_col,
                current_start, chunk_end, batch_insert_size,
            ):
                fetch_time_ms = (time.time() - fetch_start) * 1000
                chunk_rows_fetched += len(batch_rows)

                if enable_smart_pagination and smart_paginator:
                    smart_paginator.adjust_chunk_size(fetch_time_ms, len(batch_rows))
                    chunk_size = smart_paginator.current_chunk_size

                if enable_adaptive_tuning and adaptive_tuner:
                    metrics = PerformanceMetrics(
                        source_response_time=fetch_time_ms,
                        target_response_time=0,
                        rows_per_second=len(batch_rows) / max(0.001, time.time() - batch_start_time),
                    )
                    adaptive_tuner.record_metrics(metrics)
                    batch_insert_size = adaptive_tuner.get_batch_size()

                insert_batch = []
                for row in batch_rows:
                    row_tuple = tuple(row[c] for c in columns)

                    if enable_transaction_splitter and tx_splitter:
                        need_commit = tx_splitter.add_row(row_tuple)
                        if need_commit:
                            batch_to_insert = tx_splitter.get_batch_and_reset()
                            _do_insert_with_retry(
                                tgt_adapter, tgt_conn, table_name, columns,
                                batch_to_insert, retry_mgr,
                            )
                            synced_rows += len(batch_to_insert)
                            update_progress(tgt_cfg, table_name, synced_rows, chunk_end)
                    else:
                        insert_batch.append(row_tuple)
                        if len(insert_batch) >= batch_insert_size:
                            _do_insert_with_retry(
                                tgt_adapter, tgt_conn, table_name, columns,
                                insert_batch, retry_mgr,
                            )
                            synced_rows += len(insert_batch)
                            update_progress(tgt_cfg, table_name, synced_rows, chunk_end)
                            insert_batch = []

                if insert_batch:
                    _do_insert_with_retry(
                        tgt_adapter, tgt_conn, table_name, columns,
                        insert_batch, retry_mgr,
                    )
                    synced_rows += len(insert_batch)
                    insert_batch = []

                fetch_start = time.time()

            if enable_transaction_splitter and tx_splitter and tx_splitter.has_pending():
                batch_to_insert = tx_splitter.get_batch_and_reset()
                _do_insert_with_retry(
                    tgt_adapter, tgt_conn, table_name, columns,
                    batch_to_insert, retry_mgr,
                )
                synced_rows += len(batch_to_insert)

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

        if enable_consistency_check and consistency_checker:
            logger.info("开始一致性校验: %s", table_name)
            check_result = consistency_checker.compare_tables(
                src_conn, tgt_conn, src_adapter, tgt_adapter,
                table_name, columns, pk_col, sample_mode=True,
            )
            if not check_result["consistent"]:
                logger.warning("表 %s 一致性校验未通过，建议进行全量校验", table_name)

        mark_progress_done(tgt_cfg, table_name, synced_rows)

    except Exception as e:
        logger.error("同步表 %s 出错: %s", table_name, e)
        mark_progress_failed(tgt_cfg, table_name, str(e))
        return {
            "table": table_name, "rows": synced_rows,
            "time": time.time() - start_time, "error": str(e),
        }
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "[优化版] 完成表 %s 同步: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {
        "table": table_name, "rows": synced_rows,
        "time": elapsed, "error": None,
        "optimized": True,
    }


def _do_insert_with_retry(tgt_adapter, tgt_conn, table_name, columns, rows, retry_mgr):
    """
    执行批量插入，支持重试和降级。

    Args:
        tgt_adapter: 目标数据库适配器
        tgt_conn: 目标数据库连接
        table_name: 表名
        columns: 列名列表
        rows: 数据行列表
        retry_mgr: 重试管理器（可为 None）
    """
    if not rows:
        return

    insert_start = time.time()

    if retry_mgr:
        result = retry_mgr.execute_batch_with_fallback(
            lambda r: tgt_adapter.batch_insert(tgt_conn, table_name, columns, r),
            rows,
            table_name=table_name,
        )
        if not result.success and result.failed_rows:
            logger.warning(
                "表 %s 批量插入部分失败: %d/%d 行成功, %d 行进入死信队列",
                table_name, result.rows_processed, len(rows), result.rows_failed,
            )
    else:
        tgt_adapter.batch_insert(tgt_conn, table_name, columns, rows)

    insert_time_ms = (time.time() - insert_start) * 1000
    if insert_time_ms > 3000:
        logger.warning(
            "表 %s 批量插入耗时较长: %.1fms (%d 行)",
            table_name, insert_time_ms, len(rows),
        )


def _sync_table_streaming_optimized(
    src_cfg, tgt_cfg, table_name,
    chunk_size=50000, batch_insert_size=5000,
    enable_retry=True, max_retries=3,
    max_transaction_rows=1000,
):
    """
    优化版流式同步（无主键回退方案）。
    """
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info(
        "[优化版] 流式同步 (无主键) 表: %s (%s → %s)",
        table_name, src_adapter.db_type, tgt_adapter.db_type,
    )
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    tx_splitter = TransactionSplitter(max_transaction_rows=max_transaction_rows)
    retry_mgr = RetryManager(max_retries=max_retries) if enable_retry else None

    try:
        columns = src_adapter.get_table_columns(src_conn, table_name)
        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        logger.info("表 %s 共 ~%d 行 (流式模式)", table_name, total_rows)

        for batch_rows in src_adapter.fetch_all_streaming(
            src_conn, table_name, columns, chunk_size,
        ):
            insert_batch = []
            for row in batch_rows:
                row_tuple = tuple(row[c] for c in columns)
                need_commit = tx_splitter.add_row(row_tuple)
                if need_commit:
                    batch_to_insert = tx_splitter.get_batch_and_reset()
                    _do_insert_with_retry(
                        tgt_adapter, tgt_conn, table_name, columns,
                        batch_to_insert, retry_mgr,
                    )
                    synced_rows += len(batch_to_insert)

            if tx_splitter.has_pending():
                batch_to_insert = tx_splitter.get_batch_and_reset()
                _do_insert_with_retry(
                    tgt_adapter, tgt_conn, table_name, columns,
                    batch_to_insert, retry_mgr,
                )
                synced_rows += len(batch_to_insert)

            now = time.time()
            if now - start_time >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "表 %s (流式优化) 进度: %d / ~%d 行 (%.1f%%)",
                    table_name, synced_rows, total_rows, pct,
                )

    except Exception as e:
        logger.error("流式同步表 %s 出错: %s", table_name, e)
        return {
            "table": table_name, "rows": synced_rows,
            "time": time.time() - start_time, "error": str(e),
        }
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "[优化版] 完成流式同步表 %s: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, synced_rows, elapsed, speed,
    )
    return {
        "table": table_name, "rows": synced_rows,
        "time": elapsed, "error": None,
        "optimized": True,
    }


# ====================================================================== #
#                        优化版同步任务入口
# ====================================================================== #

def run_sync_optimized(config, resume_mode=False, restart=False, use_optimized=True):
    """
    执行优化版数据库同步任务。

    与原 run_sync 接口完全兼容，增加优化配置支持。

    Args:
        config: 同步配置字典
        resume_mode: 是否断点续传
        restart: 是否重新开始
        use_optimized: 是否使用优化版引擎

    Returns:
        同步结果列表
    """
    if not use_optimized:
        from sync_engine import run_sync
        return run_sync(config, resume_mode=resume_mode, restart=restart)

    src_cfg = config["source"]
    tgt_cfg = config["target"]
    sync_cfg = config.get("sync", {})

    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)

    opt_config = create_optimizer_config(sync_cfg)

    logger.info("=" * 60)
    logger.info(
        "[优化版] 同步任务启动: %s → %s",
        src_adapter.db_type, tgt_adapter.db_type,
    )
    logger.info(
        "源: %s:%d/%s",
        src_cfg.get("host"), int(src_cfg.get("port", 0)), src_cfg.get("database"),
    )
    logger.info(
        "目标: %s:%d/%s",
        tgt_cfg.get("host"), int(tgt_cfg.get("port", 0)), tgt_cfg.get("database"),
    )
    logger.info(
        "优化特性: 大事务拆分=%s, 智能分页=%s, 重试=%s, 自适应调优=%s, 一致性校验=%s",
        sync_cfg.get("enable_transaction_splitter", True),
        sync_cfg.get("enable_smart_pagination", True),
        sync_cfg.get("enable_retry", True),
        sync_cfg.get("enable_adaptive_tuning", True),
        sync_cfg.get("enable_consistency_check", False),
    )
    logger.info("=" * 60)

    parallel = sync_cfg.get("parallel_tables", 4)
    chunk_size = sync_cfg.get("chunk_size", 50000)
    batch_insert_size = sync_cfg.get("batch_insert_size", 5000)
    tables = sync_cfg.get("tables", [])

    max_transaction_rows = opt_config["max_transaction_rows"]
    max_retries = opt_config["max_retries"]

    enable_transaction_splitter = sync_cfg.get("enable_transaction_splitter", True)
    enable_smart_pagination = sync_cfg.get("enable_smart_pagination", True)
    enable_retry = sync_cfg.get("enable_retry", True)
    enable_adaptive_tuning = sync_cfg.get("enable_adaptive_tuning", True)
    enable_consistency_check = sync_cfg.get("enable_consistency_check", False)

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
                sync_table_optimized,
                src_cfg, tgt_cfg, tbl,
                chunk_size, batch_insert_size,
                resume_mode=resume_mode,
                enable_transaction_splitter=enable_transaction_splitter,
                enable_smart_pagination=enable_smart_pagination,
                enable_retry=enable_retry,
                enable_adaptive_tuning=enable_adaptive_tuning,
                enable_consistency_check=enable_consistency_check,
                max_transaction_rows=max_transaction_rows,
                max_retries=max_retries,
            )
            futures[future] = tbl

        for future in as_completed(futures):
            tbl = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error("表 %s 同步失败: %s", tbl, e)
                results.append({
                    "table": tbl, "rows": 0, "time": 0, "error": str(e),
                })

    total_elapsed = time.time() - total_start
    total_rows = sum(r["rows"] for r in results)
    skipped = sum(1 for r in results if r.get("skipped"))
    errors = [r for r in results if r["error"]]

    logger.info("=" * 60)
    logger.info("[优化版] 同步完成")
    logger.info("总同步行数: %d", total_rows)
    logger.info("跳过的表 (已完成): %d", skipped)
    logger.info("总耗时: %.1fs", total_elapsed)
    logger.info(
        "总体速度: %.0f 行/s",
        total_rows / total_elapsed if total_elapsed > 0 else 0,
    )
    if errors:
        logger.error("出错的表: %s", [e["table"] for e in errors])
    for r in results:
        status = "OK" if not r["error"] else f"ERROR: {r['error']}"
        if r.get("skipped"):
            status = "SKIPPED (已完成)"
        opt_mark = "[优化]" if r.get("optimized") else ""
        logger.info(
            "  %-40s %10d 行  %8.1fs  %s %s",
            r["table"], r["rows"], r["time"], opt_mark, status,
        )
    logger.info("=" * 60)

    return results
