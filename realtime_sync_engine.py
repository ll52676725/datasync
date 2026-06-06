"""
realtime_sync_engine.py — 实时数据同步引擎（流处理）

基于 CDC (Change Data Capture) 实现数据库的实时增量同步，
与批量同步引擎共同构成流批一体数据同步平台。

核心特性：
1. 基于 MySQL binlog 的实时数据捕获
2. 支持 INSERT / UPDATE / DELETE 事件同步
3. 断点续传（记录 binlog 位置）
4. 事件缓冲与批量写入（提升性能）
5. 状态监控与错误重试

架构：
    源库 binlog → CDC 监听器 → 事件队列 → 批量处理器 → 目标库

使用方式：
    1. 先执行批量全量同步（确保数据基准一致）
    2. 启动实时同步，从批量完成的 binlog 位置开始监听
    3. 持续捕获并同步增量变更
"""

import time
import logging
import threading
import queue
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict

from db_adapter import get_adapter, BaseDBAdapter
from db2_memory import db2_store

logger = logging.getLogger(__name__)


# ====================================================================== #
#                        实时同步事件数据结构
# ====================================================================== #

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


# ====================================================================== #
#                        实时同步引擎核心
# ====================================================================== #

class RealtimeSyncEngine:
    """
    实时数据同步引擎。

    负责：
    1. 从源数据库 CDC 捕获数据变更事件
    2. 将事件批量应用到目标数据库
    3. 维护同步状态和断点位置
    4. 提供监控指标
    """

    def __init__(self, config: Dict[str, Any], task_id: str = None):
        """
        初始化实时同步引擎。

        Args:
            config: 同步配置（与批量同步配置格式一致）
            task_id: 实时任务 ID
        """
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

        self.event_queue: queue.Queue = queue.Queue(maxsize=10000)
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
            "last_sync_time": 0,
            "start_time": 0,
            "current_binlog_file": "",
            "current_binlog_pos": 0,
        }

        self.table_primary_keys: Dict[str, str] = {}
        self._init_primary_keys()

        logger.info(
            "实时同步引擎初始化完成: %s → %s, 表数: %d",
            self.src_adapter.db_type, self.tgt_adapter.db_type, len(self.tables),
        )

    def _init_primary_keys(self):
        """初始化各表的主键列名。"""
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

    # ------------------------------------------------------------------ #
    #                        CDC 事件回调
    # ------------------------------------------------------------------ #

    def _on_cdc_event(self, event_type: str, table_name: str, data: Dict[str, Any],
                       event_meta: Dict[str, Any] = None):
        """
        CDC 事件回调函数。

        将捕获到的事件放入处理队列。

        Args:
            event_type: 事件类型 insert/update/delete
            table_name: 表名
            data: 行数据
            event_meta: 事件元数据，包含 binlog_file/binlog_pos (MySQL) 或 scn (Oracle)
        """
        if self.stop_flag["stop"]:
            return

        if self.tables and table_name not in self.tables:
            return

        event_meta = event_meta or {}

        binlog_file = event_meta.get("binlog_file", "")
        binlog_pos = event_meta.get("binlog_pos", 0)
        scn = event_meta.get("scn")

        event = CDCEvent(
            event_type=event_type,
            table_name=table_name,
            data=data,
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
        )

        if scn is not None:
            event.scn = scn
            self.stats["current_scn"] = scn

        try:
            self.event_queue.put(event, block=False)
            self.stats["total_events"] += 1
            if event_type == CDCAction.INSERT:
                self.stats["insert_events"] += 1
            elif event_type == CDCAction.UPDATE:
                self.stats["update_events"] += 1
            elif event_type == CDCAction.DELETE:
                self.stats["delete_events"] += 1
        except queue.Full:
            logger.warning("事件队列已满，丢弃事件: %s", event)
            self.stats["failed_events"] += 1

    # ------------------------------------------------------------------ #
    #                        批量同步到目标库
    # ------------------------------------------------------------------ #

    def _update_position_from_event(self, event: CDCEvent):
        """
        从事件更新当前 binlog/SCN 位置，并持久化。

        每成功处理一个事件就调用此方法，确保异常时也能保留最后成功位置。
        """
        if hasattr(event, 'binlog_file'):
            if event.binlog_file:
                self.stats["current_binlog_file"] = event.binlog_file
            if event.binlog_pos > 0:
                self.stats["current_binlog_pos"] = event.binlog_pos
        if hasattr(event, 'scn') and event.scn is not None:
            self.stats["current_scn"] = event.scn

        if self.task_id:
            db2_store.update_realtime_task_stats(self.task_id, self.stats)

    def _process_batch(self, events: List[CDCEvent]):
        """
        批量处理 CDC 事件，同步到目标数据库。

        核心改进：
        1. 逐表处理，每表成功后立即更新位置
        2. 逐行跟踪最后成功事件，异常时也保存位置
        3. 异常分支强制持久化最后成功位置，避免重复消费
        """
        if not events:
            return

        tgt_conn = self.tgt_adapter.create_connection()
        last_successful_event: Optional[CDCEvent] = None
        processed_count = 0

        try:
            by_table: Dict[str, List[CDCEvent]] = defaultdict(list)
            for event in events:
                by_table[event.table_name].append(event)

            for table_name, table_events in by_table.items():
                table_last_success = self._sync_table_events(tgt_conn, table_name, table_events)
                if table_last_success is not None:
                    last_successful_event = table_last_success
                    processed_count += len(table_events)
                    self._update_position_from_event(last_successful_event)

            self.stats["synced_events"] += processed_count
            self.stats["last_sync_time"] = time.time()

            if processed_count > 0 and last_successful_event is not None:
                self._update_position_from_event(last_successful_event)

        except Exception as e:
            logger.error(
                "批量处理事件失败: 已成功 %d/%d 行, 错误: %s",
                processed_count, len(events), e,
            )
            self.stats["failed_events"] += (len(events) - processed_count)

            if last_successful_event is not None:
                logger.warning(
                    "异常时保存最后成功位置: binlog=%s, pos=%d, scn=%s",
                    getattr(last_successful_event, 'binlog_file', ''),
                    getattr(last_successful_event, 'binlog_pos', 0),
                    getattr(last_successful_event, 'scn', None),
                )
                self._update_position_from_event(last_successful_event)
            raise
        finally:
            self.tgt_adapter.close_connection(tgt_conn)

    def _sync_table_events(self, conn, table_name: str, events: List[CDCEvent]) -> Optional[CDCEvent]:
        """
        同步单张表的 CDC 事件。

        策略：
        - INSERT/UPDATE: 使用 upsert（幂等）
        - DELETE: 使用 delete

        Returns:
            最后一个成功处理的事件（用于位置更新），全部失败返回 None
        """
        pk_col = self.table_primary_keys.get(table_name)
        if not pk_col:
            logger.warning("表 %s 无主键，跳过同步", table_name)
            return None

        last_success = None
        for event in events:
            try:
                if event.event_type in (CDCAction.INSERT, CDCAction.UPDATE):
                    self.tgt_adapter.upsert_row(conn, table_name, event.data, pk_col)
                elif event.event_type == CDCAction.DELETE:
                    pk_value = event.data.get(pk_col)
                    if pk_value is not None:
                        self.tgt_adapter.delete_row(conn, table_name, pk_col, pk_value)
                last_success = event
            except Exception as e:
                logger.error(
                    "同步事件失败: table=%s, type=%s, pk=%s, error=%s",
                    table_name, event.event_type,
                    event.data.get(pk_col), e,
                )
                raise

        return last_success

    # ------------------------------------------------------------------ #
    #                        工作线程（队列消费）
    # ------------------------------------------------------------------ #

    def _worker_loop(self):
        """
        工作线程主循环。

        从事件队列中批量取出事件，同步到目标数据库。
        采用时间窗口 + 批量大小的双重触发策略。
        """
        logger.info("实时同步工作线程启动")
        batch: List[CDCEvent] = []
        last_batch_time = time.time()

        while not self.stop_flag["stop"]:
            try:
                event = self.event_queue.get(timeout=0.1)
                batch.append(event)

                if len(batch) >= self.batch_size or (time.time() - last_batch_time >= self.batch_timeout and batch):
                    self._process_batch(batch)
                    batch = []
                    last_batch_time = time.time()

            except queue.Empty:
                if batch and time.time() - last_batch_time >= self.batch_timeout:
                    self._process_batch(batch)
                    batch = []
                    last_batch_time = time.time()
                continue
            except Exception as e:
                logger.error("工作线程异常: %s", e)
                time.sleep(1)

        if batch:
            self._process_batch(batch)

        logger.info("实时同步工作线程停止")

    # ------------------------------------------------------------------ #
    #                        启动 / 停止控制
    # ------------------------------------------------------------------ #

    def check_cdc_environment(self) -> Dict[str, Any]:
        """
        检查 CDC 环境是否满足要求。

        Returns:
            检测结果字典，包含各项检测详情和引导建议
        """
        logger.info("检查 CDC 实时同步环境...")
        conn = self.src_adapter.create_connection()
        try:
            result = self.src_adapter.check_cdc_environment(conn)
            if result["passed"]:
                logger.info("CDC 环境检测通过")
            else:
                logger.warning("CDC 环境检测未通过: %s", result["summary"])
                for check in result["checks"]:
                    if not check["passed"]:
                        logger.warning("  - %s: %s", check["name"], check["message"])
            return result
        finally:
            self.src_adapter.close_connection(conn)

    def start(self, resume_from_binlog: bool = False, skip_env_check: bool = False):
        """
        启动实时同步。

        Args:
            resume_from_binlog: 是否从上次记录的 binlog 位置恢复
            skip_env_check: 是否跳过环境检测（不推荐，仅用于已确认环境正确的场景）

        Raises:
            RuntimeError: 如果环境检测未通过且不允许跳过
        """
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
                    f"问题汇总：{env_result['summary']}\n\n"
                    f"详细问题与解决建议：\n"
                )
                for i, check in enumerate(failed_checks, 1):
                    error_msg += f"\n{i}. {check['name']}: {check['message']}\n"
                    error_msg += f"   建议：{check['guidance']}\n"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        logger.info("启动实时同步...")
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
                    logger.info("从断点恢复同步: SCN=%d", saved_pos["current_scn"])
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
                    logger.info("从断点恢复同步: %s @ %s", saved_pos["log_file"], saved_pos["log_pos"])

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
                message="实时同步已启动",
            )

        logger.info("实时同步启动完成")

    def stop(self):
        """停止实时同步。"""
        logger.info("停止实时同步...")
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
                "message": "实时同步已停止",
            }
            if self.src_adapter.db_type == "oracle":
                update_kwargs["binlog_file"] = ""
                update_kwargs["binlog_pos"] = 0
                update_kwargs["current_scn"] = self.stats.get("current_scn", 0)
                logger.info("保存 Oracle 断点: SCN=%d", update_kwargs["current_scn"])
            else:
                update_kwargs["binlog_file"] = self.stats["current_binlog_file"]
                update_kwargs["binlog_pos"] = self.stats["current_binlog_pos"]
                logger.info("保存 MySQL 断点: %s @ %d",
                            update_kwargs["binlog_file"], update_kwargs["binlog_pos"])
            db2_store.update_realtime_task(self.task_id, **update_kwargs)

        logger.info("实时同步已停止")

    def get_stats(self) -> Dict[str, Any]:
        """获取同步统计信息。"""
        stats = dict(self.stats)
        stats["queue_size"] = self.event_queue.qsize()
        stats["running_time"] = time.time() - stats["start_time"] if stats["start_time"] > 0 else 0
        stats["is_running"] = self.cdc_stream is not None
        return stats

    def is_running(self) -> bool:
        """检查同步是否在运行。"""
        return self.cdc_stream is not None and not self.stop_flag["stop"]


# ====================================================================== #
#                        全局实时任务管理器
# ====================================================================== #

class RealtimeTaskManager:
    """
    全局实时任务管理器。

    管理多个实时同步任务的生命周期。
    """

    def __init__(self):
        self._engines: Dict[str, RealtimeSyncEngine] = {}
        self._lock = threading.Lock()

    def create_engine(self, task_id: str, config: Dict[str, Any]) -> RealtimeSyncEngine:
        """创建实时同步引擎。"""
        with self._lock:
            if task_id in self._engines:
                logger.warning("任务 %s 已存在，将重新创建", task_id)
                self._stop_engine(task_id)

            engine = RealtimeSyncEngine(config, task_id=task_id)
            self._engines[task_id] = engine
            return engine

    def start_engine(self, task_id: str, resume_from_binlog: bool = False,
                     skip_env_check: bool = False) -> bool:
        """启动指定任务的同步引擎。"""
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
        """停止指定任务的同步引擎。"""
        with self._lock:
            return self._stop_engine(task_id)

    def _stop_engine(self, task_id: str) -> bool:
        """内部停止方法（不加锁）。"""
        engine = self._engines.get(task_id)
        if not engine:
            return False
        if engine.is_running():
            engine.stop()
        return True

    def remove_engine(self, task_id: str) -> bool:
        """移除并停止指定任务的同步引擎。"""
        with self._lock:
            self._stop_engine(task_id)
            if task_id in self._engines:
                del self._engines[task_id]
                return True
            return False

    def get_engine(self, task_id: str) -> Optional[RealtimeSyncEngine]:
        """获取指定任务的同步引擎。"""
        return self._engines.get(task_id)

    def list_running_tasks(self) -> List[str]:
        """列出所有正在运行的任务 ID。"""
        with self._lock:
            return [
                task_id for task_id, engine in self._engines.items()
                if engine.is_running()
            ]

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有任务的统计信息。"""
        with self._lock:
            return {
                task_id: engine.get_stats()
                for task_id, engine in self._engines.items()
            }


realtime_manager = RealtimeTaskManager()


# ====================================================================== #
#                        便捷 API
# ====================================================================== #

def start_realtime_sync(config: Dict[str, Any], task_id: str,
                        resume_from_binlog: bool = False,
                        skip_env_check: bool = False) -> bool:
    """
    启动实时同步的便捷函数。

    Args:
        config: 同步配置
        task_id: 任务 ID
        resume_from_binlog: 是否从断点恢复
        skip_env_check: 是否跳过环境检测

    Returns:
        启动成功返回 True
    """
    try:
        engine = realtime_manager.create_engine(task_id, config)
        return realtime_manager.start_engine(
            task_id,
            resume_from_binlog=resume_from_binlog,
            skip_env_check=skip_env_check,
        )
    except Exception as e:
        logger.error("启动实时同步失败: %s", e)
        db2_store.update_realtime_task(
            task_id,
            status="failed",
            message=f"启动失败: {str(e)}",
        )
        return False


def check_realtime_environment(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    检查实时同步环境的便捷函数。

    Args:
        config: 源数据库配置

    Returns:
        检测结果字典
    """
    try:
        adapter = get_adapter(config["source"])
        if not adapter.supports_cdc():
            return {
                "supported": False,
                "passed": False,
                "checks": [],
                "summary": f"{adapter.db_type} 数据库不支持 CDC 实时同步",
            }
        conn = adapter.create_connection()
        try:
            return adapter.check_cdc_environment(conn)
        finally:
            adapter.close_connection(conn)
    except Exception as e:
        logger.error("检查实时同步环境失败: %s", e)
        return {
            "supported": True,
            "passed": False,
            "checks": [{
                "name": "环境检测异常",
                "passed": False,
                "message": f"检测失败: {e}",
                "guidance": "请检查数据库连接配置是否正确",
            }],
            "summary": f"环境检测异常: {e}",
        }


def stop_realtime_sync(task_id: str) -> bool:
    """停止实时同步的便捷函数。"""
    try:
        return realtime_manager.stop_engine(task_id)
    except Exception as e:
        logger.error("停止实时同步失败: %s", e)
        return False


def get_realtime_stats(task_id: str) -> Optional[Dict[str, Any]]:
    """获取指定实时任务的统计信息。"""
    engine = realtime_manager.get_engine(task_id)
    if engine:
        return engine.get_stats()
    return None


def list_running_realtime_tasks() -> List[str]:
    """列出所有正在运行的实时任务。"""
    return realtime_manager.list_running_tasks()
