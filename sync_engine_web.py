"""
sync_engine_web.py — Web 版多数据库同步引擎

与 sync_engine.py 功能一致，但进度跟踪使用内存存储（db2_memory），
支持 Web UI 的任务管理和停止功能。

支持的同步组合：
  - mysql → mysql
  - db2   → db2
  - mysql → db2
  - db2   → mysql
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_adapter import get_adapter, BaseDBAdapter
from db2_memory import db2_store

logger = logging.getLogger(__name__)


def _rename_table_in_ddl(ddl: str, new_table_name: str) -> str:
    """在 DDL 语句中替换表名。支持 CREATE TABLE `xxx` 和 CREATE TABLE xxx 两种格式。"""
    import re
    pattern = r'(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)(`?\w+`?)'
    match = re.search(pattern, ddl, re.IGNORECASE)
    if match:
        prefix = match.group(1)
        return ddl[:match.start()] + prefix + f'`{new_table_name}`' + ddl[match.end():]
    return ddl


def ensure_target_table(src_cfg, tgt_cfg, table_name, resume_mode=False,
                        target_table_name=None):
    """
    确保目标数据库中存在与源表结构一致的表。

    如果目标表不存在，则从源库获取 DDL 并在目标库创建。
    如果目标表已存在且非断点续传模式，则清空目标表数据。

    Args:
        src_cfg: 源数据库配置（需含 type 字段）
        tgt_cfg: 目标数据库配置（需含 type 字段）
        table_name: 源表名
        resume_mode: 是否断点续传模式
        target_table_name: 目标表名（为空时与源表同名）
    """
    effective_target = target_table_name or table_name
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    src_conn = src_adapter.create_connection()
    tgt_conn = tgt_adapter.create_connection()
    try:
        ddl = src_adapter.get_create_table_ddl(src_conn, table_name)
        logger.info("获取源表 %s 的 DDL (类型: %s → %s)",
                     table_name, src_adapter.db_type, tgt_adapter.db_type)

        if effective_target != table_name:
            ddl = _rename_table_in_ddl(ddl, effective_target)

        if not tgt_adapter.table_exists(tgt_conn, effective_target):
            tgt_adapter.create_table_from_ddl(tgt_conn, ddl)
            logger.info("在目标库创建表: %s (源: %s)", effective_target, table_name)
        elif not resume_mode:
            tgt_adapter.truncate_table(tgt_conn, effective_target)
            logger.info("清空目标表: %s", effective_target)
        else:
            logger.info("目标表 %s 已存在 (断点续传模式，保留数据)", effective_target)
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)


def sync_table(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
               resume_mode=False, task_id=None, stop_flag=None,
               target_table_name=None, field_mapping=None):
    """
    同步单张表（主键分块模式，Web 版本）。

    与 sync_engine.sync_table 的区别：
    - 进度跟踪使用 db2_memory 内存存储
    - 支持通过 stop_flag 中断同步
    - 支持表名映射 (target_table_name) 和字段映射 (field_mapping)

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 源表名
        chunk_size: 每个分块的主键范围大小
        batch_insert_size: 批量插入的行数
        resume_mode: 是否断点续传
        task_id: 任务 ID（用于内存存储关联）
        stop_flag: 停止标志字典 {"stop": True/False}
        target_table_name: 目标表名（为空时与源表同名）
        field_mapping: 字段映射列表 [{"source":"col1","target":"col2"}, ...]

    Returns:
        同步结果字典
    """
    effective_target = target_table_name or table_name
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info("开始同步表: %s → %s (源: %s → 目标: %s, resume=%s)",
                table_name, effective_target, src_adapter.db_type, tgt_adapter.db_type, resume_mode)
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    try:
        src_columns = src_adapter.get_table_columns(src_conn, table_name)
        if not src_columns:
            logger.warning("表 %s 无列信息，跳过", table_name)
            return {"table": table_name, "rows": 0, "time": 0, "error": "no columns"}

        if field_mapping:
            src_col_set = set(src_columns)
            mapped_src_cols = [m["source"] for m in field_mapping if m.get("source") in src_col_set]
            read_columns = mapped_src_cols if mapped_src_cols else src_columns
            col_map = {m["source"]: m["target"] for m in field_mapping if m.get("source") and m.get("target")}
            tgt_columns = [col_map.get(c, c) for c in read_columns]
        else:
            read_columns = src_columns
            tgt_columns = src_columns
            col_map = {}

        columns = read_columns

        pk_col = src_adapter.get_primary_key(src_conn, table_name)
        if not pk_col:
            logger.error(
                "表 %s 无主键，无法按主键分块同步，回退到流式模式", table_name,
            )
            src_adapter.close_connection(src_conn)
            tgt_adapter.close_connection(tgt_conn)
            return _sync_table_streaming(
                src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
                target_table_name=effective_target, field_mapping=field_mapping,
            )

        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        min_pk, max_pk = src_adapter.get_pk_range(src_conn, table_name, pk_col)
        logger.info(
            "表 %s: 主键=%s [%d → %d], ~%d 行",
            table_name, pk_col, min_pk, max_pk, total_rows,
        )

        progress = db2_store.get_progress(table_name)
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
                db2_store.mark_progress_done(table_name, synced_rows)
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": 0, "error": None, "skipped": True,
                }
            else:
                logger.info(
                    "断点续传表 %s: 从 pk=%d 继续 (%d 行已同步)",
                    table_name, last_pk, synced_rows,
                )

        db2_store.init_progress(table_name, total_rows, pk_col, task_id)

        current_start = max(last_pk + 1, min_pk)
        last_report = time.time()

        while current_start <= max_pk:
            # 检查用户是否请求停止
            if stop_flag and stop_flag["stop"]:
                logger.info("用户停止同步表: %s", table_name)
                db2_store.mark_progress_failed(table_name, "用户停止")
                return {
                    "table": table_name, "rows": synced_rows,
                    "time": time.time() - start_time, "error": "stopped by user",
                }

            chunk_end = min(current_start + chunk_size - 1, max_pk)

            for batch_rows in src_adapter.fetch_chunk(
                src_conn, table_name, columns, pk_col,
                current_start, chunk_end, batch_insert_size,
            ):
                insert_batch = []
                for row in batch_rows:
                    insert_batch.append(tuple(row[c] for c in columns))

                    if len(insert_batch) >= batch_insert_size:
                        tgt_adapter.batch_insert(tgt_conn, effective_target, tgt_columns, insert_batch)
                        synced_rows += len(insert_batch)
                        db2_store.update_progress(table_name, synced_rows, chunk_end)
                        insert_batch = []

                if insert_batch:
                    tgt_adapter.batch_insert(tgt_conn, effective_target, tgt_columns, insert_batch)
                    synced_rows += len(insert_batch)
                    insert_batch = []

            db2_store.update_progress(table_name, synced_rows, chunk_end)

            now = time.time()
            if now - last_report >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "表 %s 进度: %d / ~%d 行 (%.1f%%), pk=%d / %d",
                    table_name, synced_rows, total_rows, pct, chunk_end, max_pk,
                )
                last_report = now

            current_start = chunk_end + 1

        db2_store.mark_progress_done(table_name, synced_rows)

    except Exception as e:
        logger.error("同步表 %s → %s 出错: %s", table_name, effective_target, e)
        db2_store.mark_progress_failed(table_name, str(e))
        return {"table": f"{table_name}→{effective_target}", "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "完成表 %s → %s 同步: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, effective_target, synced_rows, elapsed, speed,
    )
    return {"table": f"{table_name}→{effective_target}", "rows": synced_rows, "time": elapsed, "error": None}


def _sync_table_streaming(src_cfg, tgt_cfg, table_name, chunk_size, batch_insert_size,
                         target_table_name=None, field_mapping=None):
    """
    流式同步（无主键回退方案，Web 版本）。

    Args:
        src_cfg: 源数据库配置
        tgt_cfg: 目标数据库配置
        table_name: 源表名
        chunk_size: 每次读取的批量大小
        batch_insert_size: 批量插入的行数
        target_table_name: 目标表名（为空时与源表同名）
        field_mapping: 字段映射列表

    Returns:
        同步结果字典
    """
    effective_target = target_table_name or table_name
    src_adapter = get_adapter(src_cfg)
    tgt_adapter = get_adapter(tgt_cfg)
    logger.info("流式同步 (无主键) 表: %s → %s (%s → %s)",
                table_name, effective_target, src_adapter.db_type, tgt_adapter.db_type)
    start_time = time.time()

    src_conn = src_adapter.create_connection(use_dict_cursor=True)
    tgt_conn = tgt_adapter.create_connection()
    synced_rows = 0

    try:
        src_columns = src_adapter.get_table_columns(src_conn, table_name)
        total_rows = src_adapter.get_table_row_count(src_conn, table_name)
        logger.info("表 %s 共 ~%d 行 (流式模式)", table_name, total_rows)

        if field_mapping:
            src_col_set = set(src_columns)
            mapped_src_cols = [m["source"] for m in field_mapping if m.get("source") in src_col_set]
            read_columns = mapped_src_cols if mapped_src_cols else src_columns
            col_map = {m["source"]: m["target"] for m in field_mapping if m.get("source") and m.get("target")}
            tgt_columns = [col_map.get(c, c) for c in read_columns]
        else:
            read_columns = src_columns
            tgt_columns = src_columns

        for batch_rows in src_adapter.fetch_all_streaming(
            src_conn, table_name, read_columns, chunk_size,
        ):
            insert_batch = []
            for row in batch_rows:
                insert_batch.append(tuple(row[c] for c in read_columns))

                if len(insert_batch) >= batch_insert_size:
                    tgt_adapter.batch_insert(tgt_conn, effective_target, tgt_columns, insert_batch)
                    synced_rows += len(insert_batch)
                    insert_batch = []

            if insert_batch:
                tgt_adapter.batch_insert(tgt_conn, effective_target, tgt_columns, insert_batch)
                synced_rows += len(insert_batch)
                insert_batch = []

            now = time.time()
            if now - start_time >= 5.0:
                pct = (synced_rows / total_rows * 100) if total_rows > 0 else 0
                logger.info(
                    "表 %s → %s (流式) 进度: %d / ~%d 行 (%.1f%%)",
                    table_name, effective_target, synced_rows, total_rows, pct,
                )

    except Exception as e:
        logger.error("流式同步表 %s → %s 出错: %s", table_name, effective_target, e)
        return {"table": f"{table_name}→{effective_target}", "rows": synced_rows, "time": time.time() - start_time, "error": str(e)}
    finally:
        src_adapter.close_connection(src_conn)
        tgt_adapter.close_connection(tgt_conn)

    elapsed = time.time() - start_time
    speed = synced_rows / elapsed if elapsed > 0 else 0
    logger.info(
        "完成流式同步表 %s → %s: %d 行, 耗时 %.1fs (%.0f 行/s)",
        table_name, effective_target, synced_rows, elapsed, speed,
    )
    return {"table": f"{table_name}→{effective_target}", "rows": synced_rows, "time": elapsed, "error": None}


def run_sync(config, resume_mode=False, restart=False, task_id=None, stop_flag=None):
    """
    执行完整的数据库同步任务（Web 版本）。

    流程：
    1. 读取配置，确定源/目标数据库类型
    2. 如果指定了 restart 模式，重置所有进度
    3. 发现需要同步的表列表
    4. 确保目标表存在（DDL 同步）
    5. 并行执行各表的同步
    6. 更新任务状态到内存存储
    7. 汇总结果

    支持多源同步：当 sync.multiSource 存在时，依次从每个源同步表到目标。

    Args:
        config: 同步配置字典，包含 source/target/sync 节
        resume_mode: 是否断点续传模式
        restart: 是否重新开始模式
        task_id: 任务 ID
        stop_flag: 停止标志字典

    Returns:
        同步结果列表
    """
    tgt_cfg = config["target"]
    sync_cfg = config["sync"]
    multi_source = sync_cfg.get("multiSource")

    if multi_source:
        return _run_sync_multi_source(
            multi_source, tgt_cfg, sync_cfg,
            resume_mode, restart, task_id, stop_flag
        )

    return _run_sync_single_source(
        config["source"], tgt_cfg, sync_cfg,
        resume_mode, restart, task_id, stop_flag
    )


def _run_sync_multi_source(source_cfgs, tgt_cfg, sync_cfg,
                           resume_mode, restart, task_id, stop_flag):
    """多源同步：依次从每个源数据库同步表到同一目标。"""
    all_results = []
    all_tables = sync_cfg.get("tables", [])

    for src_idx, src_cfg in enumerate(source_cfgs):
        src_adapter = get_adapter(src_cfg)
        logger.info("=" * 60)
        logger.info("多源同步 [%d/%d]: %s → 目标",
                     src_idx + 1, len(source_cfgs), src_adapter.db_type)
        logger.info("源: %s:%d/%s", src_cfg.get("host"),
                     int(src_cfg.get("port", 0)), src_cfg.get("database"))
        logger.info("目标: %s:%d/%s", tgt_cfg.get("host"),
                     int(tgt_cfg.get("port", 0)), tgt_cfg.get("database"))
        logger.info("=" * 60)

        src_tables = _resolve_tables(src_cfg, all_tables)

        single_config = {
            "source": src_cfg,
            "target": tgt_cfg,
            "sync": {**sync_cfg, "tables": src_tables},
        }
        if "multiSource" in single_config["sync"]:
            del single_config["sync"]["multiSource"]

        results = _run_sync_single_source(
            src_cfg, tgt_cfg, single_config["sync"],
            resume_mode, restart, task_id, stop_flag
        )
        all_results.extend(results)

        if stop_flag and stop_flag.get("stop"):
            logger.info("多源同步被用户停止，已处理 %d/%d 个源",
                         src_idx + 1, len(source_cfgs))
            break

    return all_results


def _resolve_tables(src_cfg, requested_tables):
    """解析要同步的表列表，为空时从源库自动发现。"""
    if requested_tables:
        return requested_tables

    src_adapter = get_adapter(src_cfg)
    conn = src_adapter.create_connection()
    try:
        tables = src_adapter.list_tables(conn)
    finally:
        src_adapter.close_connection(conn)

    logger.info("未指定同步表，从源库发现 %d 张表", len(tables))
    return tables


def _run_sync_single_source(src_cfg, tgt_cfg, sync_cfg,
                            resume_mode, restart, task_id, stop_flag):
    """单源同步：从一个源数据库同步表到目标，支持表名映射和字段映射。"""
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
    field_mappings_cfg = sync_cfg.get("fieldMappings", {})

    if restart:
        db2_store.reset_progress()
        resume_mode = False
        logger.info("重新开始模式: 已重置所有进度，将清空并重新同步")

    if not tables:
        tables = _resolve_tables(src_cfg, [])

    logger.info("待同步表: %s", tables)
    if field_mappings_cfg:
        logger.info("字段映射配置: %d 张表有自定义映射", len(field_mappings_cfg))

    for tbl in tables:
        fm_entry = field_mappings_cfg.get(tbl)
        target_tbl = fm_entry.get("targetTable") if fm_entry else None
        ensure_target_table(src_cfg, tgt_cfg, tbl, resume_mode=resume_mode,
                            target_table_name=target_tbl)

    total_start = time.time()
    results = []

    db2_store.update_task(
        task_id, status="running",
        started_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        message="同步任务正在执行...",
    )

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for tbl in tables:
            fm_entry = field_mappings_cfg.get(tbl)
            target_tbl = fm_entry.get("targetTable") if fm_entry else None
            field_map = fm_entry.get("mappings") if fm_entry else None
            future = executor.submit(
                sync_table, src_cfg, tgt_cfg, tbl, chunk_size,
                batch_insert_size, resume_mode, task_id, stop_flag,
                target_table_name=target_tbl, field_mapping=field_map,
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

    final_status = "completed"
    final_message = f"同步完成！共同步 {total_rows} 行，耗时 {total_elapsed:.1f} 秒"
    if errors:
        has_stop = any("stopped by user" in (e.get("error") or "") for e in errors)
        if has_stop:
            final_status = "stopped"
            final_message = "同步已被用户停止"
        else:
            final_status = "failed"
            final_message = f"同步完成，但有 {len(errors)} 个表出错"

    db2_store.update_task(
        task_id,
        status=final_status,
        finished_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        message=final_message,
        progress=100,
    )

    return results


def reset_progress_db2(table_name=None):
    """
    重置内存中的同步进度数据。

    Args:
        table_name: 指定表名则只重置该表，None 则重置所有
    """
    if table_name:
        db2_store.reset_progress(table_name)
        logger.info("重置表进度: %s", table_name)
    else:
        db2_store.reset_progress()
        logger.info("重置所有进度")
