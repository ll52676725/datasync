"""
sync_orchestrator.py — 智能同步编排引擎（三阶段同步）

实现"先增量、再存量、后实时"的高效同步策略：
  阶段一：快速增量 (Quick Incremental)
    - 记录当前 binlog 位置
    - 同步最近 N 天的热点数据（基于主键倒序或时间字段）
    - 目标：让目标库快速具备可用数据，缩短业务等待时间

  阶段二：存量补全 (Full Backfill)
    - 同步快速增量阶段未覆盖的历史数据
    - 从最小主键到快速增量起始点之间的数据
    - 目标：后台静默补全历史数据，不影响业务使用

  阶段三：实时同步 (Realtime CDC)
    - 从阶段一记录的 binlog 位置开始消费
    - 持续捕获并同步新的数据变更
    - 目标：保持数据实时一致性

架构：
    编排引擎 → 阶段一（快速增量）→ 阶段二（存量补全）→ 阶段三（实时同步）
"""

import time
import logging
import threading
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta

from db_adapter import get_adapter, BaseDBAdapter
from db2_memory import db2_store
from sync_engine_web import ensure_target_table

logger = logging.getLogger(__name__)


class SyncPhase:
    QUICK_INCREMENTAL = "quick_incremental"
    FULL_BACKFILL = "full_backfill"
    REALTIME = "realtime"


class PhaseStatus:
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class SmartSyncOrchestrator:
    """
    智能同步编排引擎。

    负责三阶段同步的流程控制、状态管理和错误处理。
    """

    def __init__(self, config: Dict[str, Any], task_id: str, quick_days: int = 7):
        """
        初始化编排引擎。

        Args:
            config: 同步配置
            task_id: 任务 ID
            quick_days: 快速增量阶段同步最近 N 天的数据
        """
        self.src_cfg = config["source"]
        self.tgt_cfg = config["target"]
        self.sync_cfg = config.get("sync", {})
        self.task_id = task_id
        self.quick_days = quick_days

        self.src_adapter: BaseDBAdapter = get_adapter(self.src_cfg)
        self.tgt_adapter: BaseDBAdapter = get_adapter(self.tgt_cfg)

        self.tables = self.sync_cfg.get("tables", [])
        self.chunk_size = self.sync_cfg.get("chunk_size", 50000)
        self.batch_insert_size = self.sync_cfg.get("batch_insert_size", 5000)

        self.stop_flag = {"stop": False}
        self._lock = threading.Lock()

        self.initial_binlog_position: Optional[Dict[str, Any]] = None
        self.quick_sync_cutoff_pk: Dict[str, int] = {}

        logger.info(
            "智能同步编排引擎初始化: %s → %s, 快速同步天数: %d, 表数: %d",
            self.src_adapter.db_type, self.tgt_adapter.db_type, quick_days, len(self.tables),
        )

    def _get_table_time_column(self, conn, table_name: str) -> Optional[str]:
        """
        尝试寻找表的时间列（用于快速增量阶段的时间范围筛选）。

        优先查找常见的时间字段名：created_at, create_time, updated_at, update_time 等。
        """
        time_column_candidates = [
            "created_at", "create_time", "created_time",
            "updated_at", "update_time", "updated_time",
            "gmt_create", "gmt_modified", "gmt_create",
            "insert_time", "modify_time",
        ]
        columns = self.src_adapter.get_table_columns(conn, table_name)
        for col in time_column_candidates:
            if col in columns:
                return col
        return None

    def _estimate_quick_cutoff_pk(self, conn, table_name: str, pk_col: str) -> int:
        """
        估算快速增量阶段的主键截止值。

        策略：
        1. 如果有时间列，根据时间计算 cutoff_pk
        2. 没有时间列时，使用主键百分比（取最近 10% 的数据作为快速增量）

        Args:
            conn: 源数据库连接
            table_name: 表名
            pk_col: 主键列名

        Returns:
            cutoff_pk: 快速增量阶段截止主键值（大于此值的为快速增量数据）
        """
        min_pk, max_pk = self.src_adapter.get_pk_range(conn, table_name, pk_col)
        if max_pk == 0:
            return 0

        time_col = self._get_table_time_column(conn, table_name)
        if time_col:
            try:
                cutoff_time = datetime.now() - timedelta(days=self.quick_days)
                qt = self.src_adapter.quote_identifier(table_name)
                qpk = self.src_adapter.quote_identifier(pk_col)
                qtcol = self.src_adapter.quote_identifier(time_col)

                cur = conn.cursor()
                if self.src_adapter.db_type == "mysql":
                    cur.execute(
                        f"SELECT MIN({qpk}) FROM {qt} WHERE {qtcol} >= %s",
                        (cutoff_time,),
                    )
                else:
                    cur.execute(
                        f"SELECT MIN({qpk}) FROM {qt} WHERE {qtcol} >= ?",
                        (cutoff_time,),
                    )
                row = cur.fetchone()
                cur.close()
                if row and row[0] is not None:
                    cutoff_pk = row[0] - 1
                    logger.info(
                        "表 %s 基于时间列 %s 计算快速增量截止主键: %d (最近 %d 天)",
                        table_name, time_col, cutoff_pk, self.quick_days,
                    )
                    return max(cutoff_pk, min_pk - 1)
            except Exception as e:
                logger.warning("表 %s 基于时间列计算截止主键失败: %s，将使用百分比策略", table_name, e)

        cutoff_pk = int(max_pk * 0.9)
        logger.info(
            "表 %s 使用百分比策略计算快速增量截止主键: %d (最近约 10%% 数据)",
            table_name, cutoff_pk,
        )
        return cutoff_pk

    def _sync_table_range(self, src_conn, tgt_conn, table_name: str,
                          start_pk: int, end_pk: int) -> int:
        """
        同步指定主键范围内的表数据。

        Args:
            src_conn: 源数据库连接
            tgt_conn: 目标数据库连接
            table_name: 表名
            start_pk: 起始主键（包含）
            end_pk: 结束主键（包含）

        Returns:
            同步的行数
        """
        if start_pk > end_pk:
            return 0

        pk_col = self.src_adapter.get_primary_key(src_conn, table_name)
        if not pk_col:
            logger.warning("表 %s 无主键，跳过范围同步", table_name)
            return 0

        columns = self.src_adapter.get_table_columns(src_conn, table_name)
        synced_rows = 0
        current_start = start_pk

        while current_start <= end_pk:
            if self.stop_flag["stop"]:
                logger.info("用户请求停止，中断表 %s 的范围同步", table_name)
                break

            chunk_end = min(current_start + self.chunk_size - 1, end_pk)

            for batch_rows in self.src_adapter.fetch_chunk(
                src_conn, table_name, columns, pk_col,
                current_start, chunk_end, self.batch_insert_size,
            ):
                insert_batch = []
                for row in batch_rows:
                    insert_batch.append(tuple(row[c] for c in columns))

                    if len(insert_batch) >= self.batch_insert_size:
                        self.tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                        synced_rows += len(insert_batch)
                        insert_batch = []

                if insert_batch:
                    self.tgt_adapter.batch_insert(tgt_conn, table_name, columns, insert_batch)
                    synced_rows += len(insert_batch)
                    insert_batch = []

            current_start = chunk_end + 1

        return synced_rows

    def _update_phase_progress(self, phase: str, progress: float, message: str = ""):
        """更新阶段进度。"""
        db2_store.update_smart_sync_phase(self.task_id, phase, PhaseStatus.RUNNING, progress, message)

    def _mark_phase_completed(self, phase: str, message: str = ""):
        """标记阶段完成。"""
        db2_store.update_smart_sync_phase(self.task_id, phase, PhaseStatus.COMPLETED, 100.0, message)

    def _mark_phase_failed(self, phase: str, error: str):
        """标记阶段失败。"""
        db2_store.update_smart_sync_phase(self.task_id, phase, PhaseStatus.FAILED, 0.0, f"失败: {error}")

    # ------------------------------------------------------------------ #
    #                           阶段一：快速增量
    # ------------------------------------------------------------------ #

    def _phase_quick_incremental(self) -> bool:
        """
        阶段一：快速增量同步。

        流程：
        1. 记录当前 binlog 位置（作为后续实时同步的起点）
        2. 确定每张表的快速增量范围（最近 N 天的数据）
        3. 同步快速增量范围内的数据

        Returns:
            True 表示成功，False 表示失败
        """
        logger.info("=" * 60)
        logger.info("阶段一：快速增量同步启动")
        logger.info("=" * 60)

        self._update_phase_progress(SyncPhase.QUICK_INCREMENTAL, 0.0, "准备快速增量同步...")

        try:
            src_conn = self.src_adapter.create_connection()
            tgt_conn = self.tgt_adapter.create_connection()

            try:
                if self.src_adapter.supports_cdc():
                    self.initial_binlog_position = self.src_adapter.get_current_binlog_position(src_conn)
                    if self.initial_binlog_position:
                        logger.info(
                            "记录初始 binlog 位置: %s @ %s",
                            self.initial_binlog_position["log_file"],
                            self.initial_binlog_position["log_pos"],
                        )
                        db2_store.update_realtime_task(
                            self.task_id,
                            binlog_file=self.initial_binlog_position["log_file"],
                            binlog_pos=self.initial_binlog_position["log_pos"],
                        )

                if not self.tables:
                    self.tables = self.src_adapter.list_tables(src_conn)
                    logger.info("自动发现源数据库表: %s", self.tables)

                total_tables = len(self.tables)
                for idx, table_name in enumerate(self.tables):
                    if self.stop_flag["stop"]:
                        logger.info("用户请求停止快速增量同步")
                        return False

                    progress = (idx / total_tables) * 100
                    self._update_phase_progress(
                        SyncPhase.QUICK_INCREMENTAL, progress,
                        f"正在同步表 {table_name} ({idx + 1}/{total_tables})",
                    )

                    ensure_target_table(self.src_cfg, self.tgt_cfg, table_name, resume_mode=True)

                    pk_col = self.src_adapter.get_primary_key(src_conn, table_name)
                    if not pk_col:
                        logger.warning("表 %s 无主键，跳过快速增量阶段", table_name)
                        continue

                    cutoff_pk = self._estimate_quick_cutoff_pk(src_conn, table_name, pk_col)
                    self.quick_sync_cutoff_pk[table_name] = cutoff_pk

                    min_pk, max_pk = self.src_adapter.get_pk_range(src_conn, table_name, pk_col)
                    if max_pk <= cutoff_pk:
                        logger.info("表 %s 数据量较小，无需分批，快速增量将同步全表", table_name)
                        cutoff_pk = min_pk - 1
                        self.quick_sync_cutoff_pk[table_name] = cutoff_pk

                    rows_synced = self._sync_table_range(
                        src_conn, tgt_conn, table_name, cutoff_pk + 1, max_pk,
                    )
                    logger.info(
                        "表 %s 快速增量完成: 同步 %d 行 (主键范围: %d → %d)",
                        table_name, rows_synced, cutoff_pk + 1, max_pk,
                    )

                self._mark_phase_completed(
                    SyncPhase.QUICK_INCREMENTAL,
                    f"快速增量同步完成，共同步 {len(self.tables)} 张表",
                )
                logger.info("阶段一：快速增量同步完成")
                return True

            finally:
                self.src_adapter.close_connection(src_conn)
                self.tgt_adapter.close_connection(tgt_conn)

        except Exception as e:
            logger.error("阶段一（快速增量）失败: %s", e)
            self._mark_phase_failed(SyncPhase.QUICK_INCREMENTAL, str(e))
            db2_store.update_realtime_task(
                self.task_id, status="failed", message=f"快速增量阶段失败: {e}",
            )
            return False

    # ------------------------------------------------------------------ #
    #                           阶段二：存量补全
    # ------------------------------------------------------------------ #

    def _phase_full_backfill(self) -> bool:
        """
        阶段二：存量补全。

        同步快速增量阶段未覆盖的历史数据（min_pk 到 cutoff_pk 之间的数据）。
        此阶段在后台静默执行，不影响目标库已有的热点数据。

        Returns:
            True 表示成功，False 表示失败
        """
        logger.info("=" * 60)
        logger.info("阶段二：存量补全启动")
        logger.info("=" * 60)

        self._update_phase_progress(SyncPhase.FULL_BACKFILL, 0.0, "准备存量补全...")

        try:
            src_conn = self.src_adapter.create_connection()
            tgt_conn = self.tgt_adapter.create_connection()

            try:
                total_tables = len(self.tables)
                for idx, table_name in enumerate(self.tables):
                    if self.stop_flag["stop"]:
                        logger.info("用户请求停止存量补全")
                        return False

                    progress = (idx / total_tables) * 100
                    self._update_phase_progress(
                        SyncPhase.FULL_BACKFILL, progress,
                        f"正在补全表 {table_name} ({idx + 1}/{total_tables})",
                    )

                    pk_col = self.src_adapter.get_primary_key(src_conn, table_name)
                    if not pk_col:
                        logger.warning("表 %s 无主键，跳过存量补全", table_name)
                        continue

                    cutoff_pk = self.quick_sync_cutoff_pk.get(table_name, 0)
                    min_pk, _ = self.src_adapter.get_pk_range(src_conn, table_name, pk_col)

                    if cutoff_pk < min_pk:
                        logger.info("表 %s 已在快速增量阶段同步全部数据，跳过存量补全", table_name)
                        continue

                    rows_synced = self._sync_table_range(
                        src_conn, tgt_conn, table_name, min_pk, cutoff_pk,
                    )
                    logger.info(
                        "表 %s 存量补全完成: 同步 %d 行 (主键范围: %d → %d)",
                        table_name, rows_synced, min_pk, cutoff_pk,
                    )

                self._mark_phase_completed(
                    SyncPhase.FULL_BACKFILL,
                    f"存量补全完成，共补全 {len(self.tables)} 张表",
                )
                logger.info("阶段二：存量补全完成")
                return True

            finally:
                self.src_adapter.close_connection(src_conn)
                self.tgt_adapter.close_connection(tgt_conn)

        except Exception as e:
            logger.error("阶段二（存量补全）失败: %s", e)
            self._mark_phase_failed(SyncPhase.FULL_BACKFILL, str(e))
            db2_store.update_realtime_task(
                self.task_id, status="failed", message=f"存量补全阶段失败: {e}",
            )
            return False

    # ------------------------------------------------------------------ #
    #                           阶段三：实时同步
    # ------------------------------------------------------------------ #

    def _phase_realtime(self) -> bool:
        """
        阶段三：启动实时同步。

        从阶段一记录的 binlog 位置开始，启动 CDC 实时监听。

        Returns:
            True 表示成功启动，False 表示失败
        """
        logger.info("=" * 60)
        logger.info("阶段三：启动实时同步")
        logger.info("=" * 60)

        self._update_phase_progress(SyncPhase.REALTIME, 0.0, "启动实时同步...")

        try:
            from realtime_sync_engine import start_realtime_sync

            resume_from_binlog = self.initial_binlog_position is not None

            success = start_realtime_sync(
                self._build_config(), self.task_id,
                resume_from_binlog=resume_from_binlog,
            )

            if success:
                self._mark_phase_completed(SyncPhase.REALTIME, "实时同步已启动")
                db2_store.update_realtime_task(
                    self.task_id,
                    status="running",
                    message="智能同步全部阶段完成，实时同步运行中",
                    started_at=datetime.now().isoformat(),
                )
                logger.info("阶段三：实时同步启动成功")
                return True
            else:
                self._mark_phase_failed(SyncPhase.REALTIME, "启动失败")
                return False

        except Exception as e:
            logger.error("阶段三（实时同步）失败: %s", e)
            self._mark_phase_failed(SyncPhase.REALTIME, str(e))
            db2_store.update_realtime_task(
                self.task_id, status="failed", message=f"实时同步阶段失败: {e}",
            )
            return False

    def _build_config(self) -> Dict[str, Any]:
        """构建完整的配置字典。"""
        return {
            "source": self.src_cfg,
            "target": self.tgt_cfg,
            "sync": self.sync_cfg,
        }

    # ------------------------------------------------------------------ #
    #                           主执行流程
    # ------------------------------------------------------------------ #

    def check_environment(self) -> Dict[str, Any]:
        """
        检查智能同步环境是否满足要求。

        检查内容：
        1. 源数据库连接是否正常
        2. 目标数据库连接是否正常
        3. 如果源库支持 CDC，检查 CDC 环境

        Returns:
            检测结果字典
        """
        logger.info("检查智能同步环境...")
        checks = []
        all_passed = True

        try:
            src_conn = self.src_adapter.create_connection()
            self.src_adapter.close_connection(src_conn)
            checks.append({
                "name": "源数据库连接",
                "passed": True,
                "message": "连接正常",
                "guidance": "",
            })
        except Exception as e:
            all_passed = False
            checks.append({
                "name": "源数据库连接",
                "passed": False,
                "message": f"连接失败: {e}",
                "guidance": "请检查源数据库的 host、port、用户名、密码配置是否正确",
            })

        try:
            tgt_conn = self.tgt_adapter.create_connection()
            self.tgt_adapter.close_connection(tgt_conn)
            checks.append({
                "name": "目标数据库连接",
                "passed": True,
                "message": "连接正常",
                "guidance": "",
            })
        except Exception as e:
            all_passed = False
            checks.append({
                "name": "目标数据库连接",
                "passed": False,
                "message": f"连接失败: {e}",
                "guidance": "请检查目标数据库的 host、port、用户名、密码配置是否正确",
            })

        cdc_result = None
        if self.src_adapter.supports_cdc():
            try:
                src_conn = self.src_adapter.create_connection()
                try:
                    cdc_result = self.src_adapter.check_cdc_environment(src_conn)
                    checks.extend(cdc_result["checks"])
                    if not cdc_result["passed"]:
                        all_passed = False
                finally:
                    self.src_adapter.close_connection(src_conn)
            except Exception as e:
                all_passed = False
                checks.append({
                    "name": "CDC 环境检测",
                    "passed": False,
                    "message": f"检测失败: {e}",
                    "guidance": "请检查源数据库配置和权限",
                })

        if all_passed:
            summary = "智能同步环境检测全部通过"
        else:
            failed_count = sum(1 for c in checks if not c["passed"])
            summary = f"环境检测未通过，有 {failed_count} 项需要配置"

        return {
            "passed": all_passed,
            "checks": checks,
            "summary": summary,
            "cdc_supported": self.src_adapter.supports_cdc(),
            "cdc_result": cdc_result,
        }

    def run(self, skip_env_check: bool = False) -> bool:
        """
        执行完整的三阶段智能同步流程。

        Args:
            skip_env_check: 是否跳过环境检测（不推荐）

        Returns:
            True 表示全部阶段成功完成（实时同步已启动），False 表示某阶段失败
        """
        logger.info("=" * 60)
        logger.info("智能同步编排引擎启动: 任务 %s", self.task_id)
        logger.info("三阶段策略: 快速增量 → 存量补全 → 实时同步")
        logger.info("=" * 60)

        if not skip_env_check:
            env_result = self.check_environment()
            if not env_result["passed"]:
                failed_checks = [c for c in env_result["checks"] if not c["passed"]]
                error_msg = (
                    f"智能同步环境检测未通过，有 {len(failed_checks)} 项需要配置。\n"
                    f"问题汇总：{env_result['summary']}\n\n"
                    f"详细问题与解决建议：\n"
                )
                for i, check in enumerate(failed_checks, 1):
                    error_msg += f"\n{i}. {check['name']}: {check['message']}\n"
                    if check.get("guidance"):
                        error_msg += f"   建议：{check['guidance']}\n"
                logger.error(error_msg)
                db2_store.update_realtime_task(
                    self.task_id,
                    status="failed",
                    message=f"环境检测未通过: {env_result['summary']}",
                )
                return False
            else:
                logger.info("智能同步环境检测通过")

        db2_store.update_realtime_task(
            self.task_id,
            status="running",
            started_at=datetime.now().isoformat(),
            message="智能同步已启动，执行阶段一（快速增量）...",
        )

        if not self._phase_quick_incremental():
            return False

        if self.stop_flag["stop"]:
            logger.info("智能同步被用户停止")
            db2_store.update_realtime_task(
                self.task_id, status="stopped", message="智能同步已被用户停止",
            )
            return False

        if not self._phase_full_backfill():
            return False

        if self.stop_flag["stop"]:
            logger.info("智能同步被用户停止")
            db2_store.update_realtime_task(
                self.task_id, status="stopped", message="智能同步已被用户停止",
            )
            return False

        if not self.src_adapter.supports_cdc():
            logger.info("源数据库不支持 CDC，跳过实时同步阶段")
            db2_store.update_realtime_task(
                self.task_id,
                status="completed",
                finished_at=datetime.now().isoformat(),
                message="快速增量和存量补全完成（源库不支持实时同步）",
            )
            self._mark_phase_completed(SyncPhase.REALTIME, "源库不支持 CDC，跳过")
            return True

        if not self._phase_realtime():
            return False

        logger.info("=" * 60)
        logger.info("智能同步全部阶段完成！任务 %s", self.task_id)
        logger.info("=" * 60)
        return True

    def stop(self):
        """停止智能同步流程。"""
        logger.info("停止智能同步编排引擎: 任务 %s", self.task_id)
        self.stop_flag["stop"] = True

        from realtime_sync_engine import stop_realtime_sync
        try:
            stop_realtime_sync(self.task_id)
        except Exception as e:
            logger.warning("停止实时同步时出错: %s", e)


# ====================================================================== #
#                           全局管理器
# ====================================================================== #

class SmartSyncManager:
    """全局智能同步任务管理器。"""

    def __init__(self):
        self._orchestrators: Dict[str, SmartSyncOrchestrator] = {}
        self._threads: Dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def start_smart_sync(self, task_id: str, config: Dict[str, Any],
                         quick_days: int = 7, skip_env_check: bool = False) -> bool:
        """
        启动智能同步任务。

        Args:
            task_id: 任务 ID
            config: 同步配置
            quick_days: 快速增量天数
            skip_env_check: 是否跳过环境检测

        Returns:
            启动成功返回 True
        """
        with self._lock:
            if task_id in self._threads and self._threads[task_id].is_alive():
                logger.warning("智能同步任务 %s 已在运行", task_id)
                return False

            orchestrator = SmartSyncOrchestrator(config, task_id, quick_days=quick_days)
            self._orchestrators[task_id] = orchestrator

            def _run():
                try:
                    orchestrator.run(skip_env_check=skip_env_check)
                except Exception as e:
                    logger.error("智能同步任务 %s 异常: %s", task_id, e)
                    db2_store.update_realtime_task(
                        task_id, status="failed", message=f"任务异常: {e}",
                    )

            thread = threading.Thread(target=_run, daemon=True)
            self._threads[task_id] = thread
            thread.start()
            logger.info("智能同步任务已启动: %s", task_id)
            return True

    def stop_smart_sync(self, task_id: str) -> bool:
        """停止智能同步任务。"""
        with self._lock:
            orchestrator = self._orchestrators.get(task_id)
            if orchestrator:
                orchestrator.stop()

            thread = self._threads.get(task_id)
            if thread and thread.is_alive():
                thread.join(timeout=10)

            if task_id in self._orchestrators:
                del self._orchestrators[task_id]
            if task_id in self._threads:
                del self._threads[task_id]

            from realtime_sync_engine import stop_realtime_sync
            try:
                stop_realtime_sync(task_id)
            except Exception:
                pass

            logger.info("智能同步任务已停止: %s", task_id)
            return True

    def get_orchestrator(self, task_id: str) -> Optional[SmartSyncOrchestrator]:
        """获取指定任务的编排引擎。"""
        return self._orchestrators.get(task_id)

    def list_running_tasks(self) -> List[str]:
        """列出正在运行的智能同步任务。"""
        with self._lock:
            return [
                task_id for task_id, thread in self._threads.items()
                if thread.is_alive()
            ]


smart_sync_manager = SmartSyncManager()


# ====================================================================== #
#                           便捷 API
# ====================================================================== #

def start_smart_sync(config: Dict[str, Any], task_id: str,
                     quick_days: int = 7, skip_env_check: bool = False) -> bool:
    """
    启动智能同步的便捷函数。

    Args:
        config: 同步配置
        task_id: 任务 ID
        quick_days: 快速增量阶段同步最近 N 天的数据
        skip_env_check: 是否跳过环境检测

    Returns:
        启动成功返回 True
    """
    try:
        db2_store.create_smart_sync_task(task_id, config.get("name", ""),
                                          "system", quick_days=quick_days)
        return smart_sync_manager.start_smart_sync(
            task_id, config,
            quick_days=quick_days,
            skip_env_check=skip_env_check,
        )
    except Exception as e:
        logger.error("启动智能同步失败: %s", e)
        db2_store.update_realtime_task(
            task_id, status="failed", message=f"启动失败: {str(e)}",
        )
        return False


def stop_smart_sync(task_id: str) -> bool:
    """停止智能同步任务的便捷函数。"""
    return smart_sync_manager.stop_smart_sync(task_id)
