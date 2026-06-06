"""
realtime_sync_engine_optimized.py — 优化版实时数据同步引擎

在原有实时同步引擎基础上增加复杂场景优化：
1. 背压控制 - 队列过载时暂停 CDC 读取，防止内存溢出
2. 大事务处理 - 大事务事件合并，减少重复操作
3. 批量写入优化 - 按表批量合并 upsert，提升写入性能
4. 内存监控 - 实时监控内存使用，超过阈值触发流控
5. 死信队列 - 处理失败的事件持久化，支持重试
"""

import time
import logging
import threading
import queue
import psutil
import os
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict

from db_adapter import get_adapter, BaseDBAdapter
from db2_memory import db2_store
from sync_optimizers import (
    BackpressureController,
    RetryManager,
    PerformanceMetrics,
    DeadLetterRecord,
)

logger = logging.getLogger(__name__)


class CDCAction:
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class CDCEvent:
    """CDC 事件封装。"""

    def __init__(self, event_type: str, table_name: str, data: Dict[str, Any],
                 binlog_file: str = "", binlog_pos: int = 0, timestamp: float = None):
        self.event_type = event_type
        self.table_name = table_name
        self.data = data
        self.binlog_file = binlog_file
        self.binlog_pos = binlog_pos
        self.timestamp = timestamp or time.time()

    def __repr__(self):
        return f"CDCEvent({self.event_type}, {self.table_name}, pos={self.binlog_pos})"


class RealtimeSyncEngineOptimized:
    """
    优化版实时数据同步引擎。

    相比原引擎增加：
    - 背压控制（Backpressure）
    - 大事务事件合并
    - 内存使用监控
    - 死信队列
    - 批量 upsert 优化
    """

    def __init__(self, config: Dict[str, Any], task_id: str = None):
        self.src_cfg = config["source"]
        self.tgt_cfg = config["target"]
        self.sync_cfg = config.get("sync", {})
        self.task_id = task_id

        self.src_adapter: BaseDBAdapter = get_adapter(self.src_cfg)
        self.tgt_adapter: BaseDBAdapter = get_adapter(self.tgt_cfg)

        self.tables = self.sync_cfg.get("tables", [])
        self.batch_size = self.sync_cfg.get("realtime_batch_size", 100)
        self.batch_timeout = self.sync_cfg.get("realtime_batch_timeout", 1.0)
        self.server_id = self.sync_cfg.get("cdc_server_id", 101)

        queue_size = self.sync_cfg.get("event_queue_size", 10000)
        self.event_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.stop_flag = {"stop": False}

        self.cdc_stream = None
        self.cdc_thread = None
        self.worker_thread = None

        self.stats = {
            "total_events": 0,
            "insert_events": 0,
            "update_events": 0,
            "delete_events": 0,
            "synced_events": 0,
            "failed_events": 0,
            "merged_events": 0,
            "backpressure_pauses": 0,
            "last_sync_time": 0,
            "start_time": 0,
            "current_binlog_file": "",
            "current_binlog_pos": 0,
            "memory_usage_mb": 0,
        }

        self.table_primary_keys: Dict[str, str] = {}
        self._init_primary_keys()

        enable_backpressure = self.sync_cfg.get("enable_backpressure", True)
        queue_high = self.sync_cfg.get("queue_high_watermark", 8000)
        queue_low = self.sync_cfg.get("queue_low_watermark", 3000)
        max_memory_mb = self.sync_cfg.get("max_memory_mb", 512)

        self.backpressure = BackpressureController(
            queue_high_watermark=queue_high,
            queue_low_watermark=queue_low,
            max_memory_mb=max_memory_mb,
        ) if enable_backpressure else None

        max_retries = self.sync_cfg.get("max_retries", 3)
        self.retry_mgr = RetryManager(max_retries=max_retries)

        self.enable_event_merge = self.sync_cfg.get("enable_event_merge", True)
        self._event_merge_buffer: Dict[Tuple[str, Any], CDCEvent] = {}
        self._merge_lock = threading.Lock()

        enable_batch_upsert = self.sync_cfg.get("enable_batch_upsert", True)
        self.enable_batch_upsert = enable_batch_upsert and self.tgt_adapter.db_type == "mysql"

        logger.info(
            "[优化版] 实时同步引擎初始化完成: %s → %s, 表数: %d, 背压=%s, 事件合并=%s, 批量upsert=%s",
            self.src_adapter.db_type, self.tgt_adapter.db_type, len(self.tables),
            enable_backpressure, self.enable_event_merge, self.enable_batch_upsert,
        )

    def _init_primary_keys(self):
        if not self.tables:
            return
        conn = self.src_adapter.create_connection()
        try:
            for table in self.tables:
                pk = self.src_adapter.get_primary_key(conn, table)
                if pk:
                    self.table_primary_keys[table] = pk
                    logger.info("表 %s 主键: %s", table, pk)
                else:
                    logger.warning("表 %s 未找到主键，实时同步可能不支持 DELETE 事件", table)
        finally:
            self.src_adapter.close_connection(conn)

    def _get_memory_usage_mb(self) -> float:
        """获取当前进程内存使用（MB）。"""
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def _on_cdc_event(self, event_type: str, table_name: str, data: Dict[str, Any]):
        """
        CDC 事件回调函数（优化版）。

        增加背压控制检查，队列满时阻塞等待。
        """
        if self.stop_flag["stop"]:
            return

        if self.tables and table_name not in self.tables:
            return

        memory_mb = self._get_memory_usage_mb()
        self.stats["memory_usage_mb"] = memory_mb

        if self.backpressure:
            queue_size = self.event_queue.qsize()
            if self.backpressure.check_should_pause(queue_size, memory_mb):
                pause_start = time.time()
                while not self.stop_flag["stop"]:
                    time.sleep(0.1)
                    qs = self.event_queue.qsize()
                    mem = self._get_memory_usage_mb()
                    if self.backpressure.check_should_resume(qs, mem):
                        break
                    if time.time() - pause_start > 30:
                        logger.warning("背压等待超时，丢弃事件")
                        self.stats["failed_events"] += 1
                        return
                self.stats["backpressure_pauses"] = self.backpressure.get_stats()["pause_count"]

        event = CDCEvent(
            event_type=event_type,
            table_name=table_name,
            data=data,
        )

        if self.enable_event_merge:
            with self._merge_lock:
                pk_col = self.table_primary_keys.get(table_name)
                if pk_col and pk_col in data:
                    pk_value = data[pk_col]
                    key = (table_name, pk_value)
                    if key in self._event_merge_buffer:
                        existing = self._event_merge_buffer[key]
                        if event_type == CDCAction.DELETE:
                            self._event_merge_buffer[key] = event
                        elif event_type == CDCAction.UPDATE and existing.event_type == CDCAction.INSERT:
                            new_event = CDCEvent(
                                event_type=CDCAction.INSERT,
                                table_name=table_name,
                                data=data,
                                binlog_file=event.binlog_file,
                                binlog_pos=event.binlog_pos,
                            )
                            self._event_merge_buffer[key] = new_event
                        else:
                            self._event_merge_buffer[key] = event
                        self.stats["merged_events"] += 1
                        return
                    else:
                        self._event_merge_buffer[key] = event

        try:
            self.event_queue.put(event, block=True, timeout=5.0)
            self.stats["total_events"] += 1
            if event_type == CDCAction.INSERT:
                self.stats["insert_events"] += 1
            elif event_type == CDCAction.UPDATE:
                self.stats["update_events"] += 1
            elif event_type == CDCAction.DELETE:
                self.stats["delete_events"] += 1
        except queue.Full:
            logger.warning("事件队列已满（等待超时），丢弃事件: %s", event)
            self.stats["failed_events"] += 1

    def _flush_merge_buffer(self) -> List[CDCEvent]:
        """将合并缓冲区的事件刷新到列表。"""
        with self._merge_lock:
            events = list(self._event_merge_buffer.values())
            self._event_merge_buffer.clear()
            return events

    def _process_batch(self, events: List[CDCEvent]):
        """
        批量处理 CDC 事件（优化版）。

        支持按表批量 upsert，提升写入性能。
        """
        if not events:
            return

        tgt_conn = self.tgt_adapter.create_connection()
        try:
            by_table: Dict[str, List[CDCEvent]] = defaultdict(list)
            for event in events:
                by_table[event.table_name].append(event)

            for table_name, table_events in by_table.items():
                self._sync_table_events_optimized(tgt_conn, table_name, table_events)

            self.stats["synced_events"] += len(events)
            self.stats["last_sync_time"] = time.time()

            last_event = events[-1]
            if hasattr(last_event, 'binlog_file') and last_event.binlog_file:
                self.stats["current_binlog_file"] = last_event.binlog_file
                self.stats["current_binlog_pos"] = last_event.binlog_pos

            if self.task_id:
                db2_store.update_realtime_task_stats(self.task_id, self.stats)

        except Exception as e:
            logger.error("批量处理事件失败: %s", e)
            self.stats["failed_events"] += len(events)
            raise
        finally:
            self.tgt_adapter.close_connection(tgt_conn)

    def _sync_table_events_optimized(self, conn, table_name: str, events: List[CDCEvent]):
        """
        优化版单表 CDC 事件同步。

        支持：
        - MySQL 批量 upsert
        - 失败重试
        - 死信队列
        """
        pk_col = self.table_primary_keys.get(table_name)
        if not pk_col:
            logger.warning("表 %s 无主键，跳过同步", table_name)
            return

        insert_events = []
        update_events = []
        delete_events = []

        for event in events:
            if event.event_type == CDCAction.INSERT:
                insert_events.append(event)
            elif event.event_type == CDCAction.UPDATE:
                update_events.append(event)
            elif event.event_type == CDCAction.DELETE:
                delete_events.append(event)

        if self.enable_batch_upsert and (insert_events or update_events):
            all_upsert = insert_events + update_events
            success, error = self.retry_mgr.execute_with_retry(
                self._batch_upsert_mysql,
                conn, table_name, all_upsert, pk_col,
            )
            if not success:
                logger.warning("批量 upsert 失败，降级为逐行处理: %s", error)
                for event in all_upsert:
                    self._upsert_with_retry(conn, table_name, event, pk_col)
        else:
            for event in insert_events + update_events:
                self._upsert_with_retry(conn, table_name, event, pk_col)

        for event in delete_events:
            self._delete_with_retry(conn, table_name, event, pk_col)

    def _batch_upsert_mysql(self, conn, table_name: str, events: List[CDCEvent], pk_col: str):
        """MySQL 批量 upsert 优化。"""
        if not events:
            return

        qt = self.tgt_adapter.quote_identifier(table_name)
        columns = list(events[0].data.keys())
        col_list = ", ".join(self.tgt_adapter.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_list = ", ".join(
            f"{self.tgt_adapter.quote_identifier(c)} = VALUES({self.tgt_adapter.quote_identifier(c)})"
            for c in columns
        )

        sql = f"""
            INSERT INTO {qt} ({col_list}) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_list}
        """

        rows = []
        for event in events:
            rows.append(tuple(event.data.get(c) for c in columns))

        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        logger.debug("批量 upsert 表 %s: %d 行", table_name, len(rows))

    def _upsert_with_retry(self, conn, table_name: str, event: CDCEvent, pk_col: str):
        """带重试的 upsert。"""
        success, error = self.retry_mgr.execute_with_retry(
            lambda: self.tgt_adapter.upsert_row(conn, table_name, event.data, pk_col)
        )
        if not success:
            self.retry_mgr._add_to_dead_letter(
                table_name=table_name,
                operation=event.event_type,
                data=event.data,
                error=str(error),
            )

    def _delete_with_retry(self, conn, table_name: str, event: CDCEvent, pk_col: str):
        """带重试的 delete。"""
        pk_value = event.data.get(pk_col)
        if pk_value is None:
            return
        success, error = self.retry_mgr.execute_with_retry(
            lambda: self.tgt_adapter.delete_row(conn, table_name, pk_col, pk_value)
        )
        if not success:
            self.retry_mgr._add_to_dead_letter(
                table_name=table_name,
                operation="delete",
                data=event.data,
                error=str(error),
            )

    def _worker_loop(self):
        """
        工作线程主循环（优化版）。

        增加内存监控和统计上报。
        """
        logger.info("[优化版] 实时同步工作线程启动")
        batch: List[CDCEvent] = []
        last_batch_time = time.time()
        last_stats_report = time.time()

        while not self.stop_flag["stop"]:
            try:
                event = self.event_queue.get(timeout=0.1)
                batch.append(event)

                if len(batch) >= self.batch_size or (time.time() - last_batch_time >= self.batch_timeout and batch):
                    if self.enable_event_merge:
                        batch.extend(self._flush_merge_buffer())
                    self._process_batch(batch)
                    batch = []
                    last_batch_time = time.time()

                if time.time() - last_stats_report >= 10.0:
                    self.stats["memory_usage_mb"] = self._get_memory_usage_mb()
                    logger.debug(
                        "实时同步统计: 总事件=%d, 已同步=%d, 失败=%d, 合并=%d, 队列=%d, 内存=%.1fMB",
                        self.stats["total_events"], self.stats["synced_events"],
                        self.stats["failed_events"], self.stats["merged_events"],
                        self.event_queue.qsize(), self.stats["memory_usage_mb"],
                    )
                    last_stats_report = time.time()

            except queue.Empty:
                if batch and time.time() - last_batch_time >= self.batch_timeout:
                    if self.enable_event_merge:
                        batch.extend(self._flush_merge_buffer())
                    self._process_batch(batch)
                    batch = []
                    last_batch_time = time.time()
                continue
            except Exception as e:
                logger.error("工作线程异常: %s", e)
                time.sleep(1)

        if self.enable_event_merge:
            batch.extend(self._flush_merge_buffer())
        if batch:
            self._process_batch(batch)

        logger.info("[优化版] 实时同步工作线程停止")

    def check_cdc_environment(self) -> Dict[str, Any]:
        from realtime_sync_engine import RealtimeSyncEngine
        temp_engine = RealtimeSyncEngine({"source": self.src_cfg, "target": self.tgt_cfg, "sync": self.sync_cfg})
        return temp_engine.check_cdc_environment()

    def start(self, resume_from_binlog: bool = False, skip_env_check: bool = False):
        if self.cdc_stream:
            logger.warning("实时同步已在运行")
            return

        if not self.src_adapter.supports_cdc():
            raise RuntimeError(f"源数据库类型 {self.src_adapter.db_type} 不支持 CDC")

        if not skip_env_check:
            env_result = self.check_cdc_environment()
            if not env_result["passed"]:
                failed_checks = [c for c in env_result["checks"] if not c["passed"]]
                error_msg = (
                    f"CDC 环境检测未通过，有 {len(failed_checks)} 项需要配置。\n"
                    f"问题汇总：{env_result['summary']}\n"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        logger.info("[优化版] 启动实时同步...")
        self.stats["start_time"] = time.time()
        self.stop_flag["stop"] = False

        if self.src_adapter.db_type == "oracle":
            cdc_kwargs = {
                "interval": self.sync_cfg.get("logminer_interval", 2),
            }
            if resume_from_binlog and self.task_id:
                saved_pos = db2_store.get_realtime_task_binlog_position(self.task_id)
                if saved_pos and saved_pos.get("current_scn"):
                    cdc_kwargs["start_scn"] = saved_pos["current_scn"]
        else:
            cdc_kwargs = {
                "server_id": self.server_id,
                "blocking": True,
                "only_tables": self.tables,
            }
            if resume_from_binlog and self.task_id:
                saved_pos = db2_store.get_realtime_task_binlog_position(self.task_id)
                if saved_pos:
                    cdc_kwargs["resume_stream"] = True
                    cdc_kwargs["log_file"] = saved_pos.get("log_file")
                    cdc_kwargs["log_pos"] = saved_pos.get("log_pos")

        self.cdc_stream, self.cdc_thread = self.src_adapter.create_cdc_stream(
            tables=self.tables,
            callback=self._on_cdc_event,
            **cdc_kwargs,
        )

        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()

        if self.task_id:
            db2_store.update_realtime_task(
                self.task_id,
                status="running",
                message="[优化版] 实时同步已启动",
            )

        logger.info("[优化版] 实时同步启动完成")

    def stop(self):
        logger.info("[优化版] 停止实时同步...")
        self.stop_flag["stop"] = True

        if self.cdc_stream:
            try:
                if self.src_adapter.db_type == "oracle":
                    self.cdc_stream["stop"] = True
                else:
                    self.cdc_stream.close()
            except Exception as e:
                logger.error("关闭 CDC 流出错: %s", e)

        if self.cdc_thread and self.cdc_thread.is_alive():
            self.cdc_thread.join(timeout=5)

        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)

        self.cdc_stream = None
        self.cdc_thread = None
        self.worker_thread = None

        if self.task_id:
            update_kwargs = {
                "status": "stopped",
                "message": "[优化版] 实时同步已停止",
            }
            if self.src_adapter.db_type == "oracle":
                update_kwargs["binlog_file"] = ""
                update_kwargs["binlog_pos"] = 0
                update_kwargs["current_scn"] = self.stats.get("current_scn", 0)
            else:
                update_kwargs["binlog_file"] = self.stats["current_binlog_file"]
                update_kwargs["binlog_pos"] = self.stats["current_binlog_pos"]
            db2_store.update_realtime_task(self.task_id, **update_kwargs)

        dlq_stats = self.retry_mgr.get_stats()
        logger.info(
            "[优化版] 实时同步已停止，死信队列: %d 条记录",
            dlq_stats["dead_letter_count"],
        )

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self.stats)
        stats["queue_size"] = self.event_queue.qsize()
        stats["running_time"] = time.time() - stats["start_time"] if stats["start_time"] > 0 else 0
        stats["is_running"] = self.cdc_stream is not None
        stats["dead_letter_count"] = self.retry_mgr.get_stats()["dead_letter_count"]
        stats["backpressure_pause_count"] = self.backpressure.get_stats()["pause_count"] if self.backpressure else 0
        return stats

    def get_dead_letters(self, table_name: str = None) -> List[DeadLetterRecord]:
        """获取死信队列记录。"""
        return self.retry_mgr.get_dead_letters(table_name)

    def is_running(self) -> bool:
        return self.cdc_stream is not None and not self.stop_flag["stop"]


class RealtimeTaskManagerOptimized:
    """全局优化版实时任务管理器。"""

    def __init__(self):
        self._engines: Dict[str, RealtimeSyncEngineOptimized] = {}
        self._lock = threading.Lock()

    def create_engine(self, task_id: str, config: Dict[str, Any]) -> RealtimeSyncEngineOptimized:
        with self._lock:
            if task_id in self._engines:
                logger.warning("任务 %s 已存在，将重新创建", task_id)
                self._stop_engine(task_id)
            engine = RealtimeSyncEngineOptimized(config, task_id=task_id)
            self._engines[task_id] = engine
            return engine

    def start_engine(self, task_id: str, resume_from_binlog: bool = False,
                     skip_env_check: bool = False) -> bool:
        with self._lock:
            engine = self._engines.get(task_id)
            if not engine:
                logger.error("任务 %s 不存在", task_id)
                return False
            if engine.is_running():
                logger.warning("任务 %s 已在运行", task_id)
                return True
            engine.start(
                resume_from_binlog=resume_from_binlog,
                skip_env_check=skip_env_check,
            )
            return True

    def stop_engine(self, task_id: str) -> bool:
        with self._lock:
            return self._stop_engine(task_id)

    def _stop_engine(self, task_id: str) -> bool:
        engine = self._engines.get(task_id)
        if not engine:
            return False
        if engine.is_running():
            engine.stop()
        return True

    def remove_engine(self, task_id: str) -> bool:
        with self._lock:
            self._stop_engine(task_id)
            if task_id in self._engines:
                del self._engines[task_id]
                return True
            return False

    def get_engine(self, task_id: str) -> Optional[RealtimeSyncEngineOptimized]:
        return self._engines.get(task_id)

    def list_running_tasks(self) -> List[str]:
        with self._lock:
            return [
                task_id for task_id, engine in self._engines.items()
                if engine.is_running()
            ]

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                task_id: engine.get_stats()
                for task_id, engine in self._engines.items()
            }


realtime_manager_optimized = RealtimeTaskManagerOptimized()


def start_realtime_sync_optimized(config: Dict[str, Any], task_id: str,
                                  resume_from_binlog: bool = False,
                                  skip_env_check: bool = False) -> bool:
    """启动优化版实时同步的便捷函数。"""
    try:
        engine = realtime_manager_optimized.create_engine(task_id, config)
        return realtime_manager_optimized.start_engine(
            task_id,
            resume_from_binlog=resume_from_binlog,
            skip_env_check=skip_env_check,
        )
    except Exception as e:
        logger.error("启动优化版实时同步失败: %s", e)
        db2_store.update_realtime_task(
            task_id,
            status="failed",
            message=f"启动失败: {str(e)}",
        )
        return False


def stop_realtime_sync_optimized(task_id: str) -> bool:
    """停止优化版实时同步的便捷函数。"""
    try:
        return realtime_manager_optimized.stop_engine(task_id)
    except Exception as e:
        logger.error("停止优化版实时同步失败: %s", e)
        return False


def get_realtime_stats_optimized(task_id: str) -> Optional[Dict[str, Any]]:
    engine = realtime_manager_optimized.get_engine(task_id)
    if engine:
        return engine.get_stats()
    return None
