"""
sync_optimizers.py — 同步引擎优化组件

针对复杂场景的优化组件集合：
1. 大事务拆分器 (TransactionSplitter) - 控制单事务大小，避免锁表和回滚段溢出
2. 智能分页器 (SmartPaginator) - 基于数据分布动态调整分页策略
3. 自适应调优器 (AdaptiveTuner) - 根据数据库负载动态调整同步参数
4. 错误重试管理器 (RetryManager) - 失败重试和死信队列
5. 一致性校验器 (ConsistencyChecker) - 数据完整性校验
6. 背压控制器 (BackpressureController) - 实时同步流控
"""

import time
import logging
import threading
from typing import List, Dict, Any, Optional, Tuple, Callable, Generator
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


# ====================================================================== #
#                        数据结构定义
# ====================================================================== #

class RetryStrategy(Enum):
    """重试策略枚举。"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    IMMEDIATE = "immediate"


@dataclass
class BatchResult:
    """批量处理结果。"""
    success: bool
    rows_processed: int = 0
    rows_failed: int = 0
    failed_rows: List[Tuple] = field(default_factory=list)
    error: Optional[str] = None
    execution_time: float = 0.0


@dataclass
class DeadLetterRecord:
    """死信队列记录。"""
    id: str
    table_name: str
    operation: str  # insert/update/delete
    data: Dict[str, Any]
    error: str
    retry_count: int = 0
    first_failed_at: float = field(default_factory=time.time)
    last_failed_at: float = field(default_factory=time.time)


@dataclass
class PerformanceMetrics:
    """性能指标收集。"""
    source_response_time: float = 0.0
    target_response_time: float = 0.0
    rows_per_second: float = 0.0
    queue_size: int = 0
    memory_usage_mb: float = 0.0
    timestamp: float = field(default_factory=time.time)


# ====================================================================== #
#                        1. 大事务拆分器
# ====================================================================== #

@dataclass
class TransactionalRow:
    """
    带事务上下文的数据行。

    用于在拆分时保持事务边界感知。
    """
    row: Tuple
    transaction_id: Optional[str] = None  # 源事务ID（MySQL: XID, Oracle: XID）
    table_name: Optional[str] = None
    operation: Optional[str] = None  # insert/update/delete
    safe_point: Optional[Any] = None  # 安全点（如PK值、binlog位置、SCN）
    timestamp: float = field(default_factory=time.time)


@dataclass
class SplitBoundary:
    """
    拆分边界定义。

    描述一个拆分批次的边界信息，用于断点续传和一致性保证。
    """
    batch_id: int
    start_safe_point: Optional[Any] = None
    end_safe_point: Optional[Any] = None
    row_count: int = 0
    transaction_ids: List[str] = field(default_factory=list)
    committed: bool = False
    commit_time: Optional[float] = None


class TransactionSplitter:
    """
    增强版大事务拆分器。

    核心特性：
    1. **事务边界感知**：优先在事务边界拆分，避免同一源事务被拆分
    2. **并发事务隔离**：按事务ID分组，支持并发事务的有序提交
    3. **安全点机制**：每个拆分批次记录安全点，支持精确断点续传
    4. **双阈值触发**：行数阈值 + 时间阈值，避免长事务

    拆分边界界定规则：
    - 硬边界（必须拆分）：当前批次行数 >= max_transaction_rows
    - 软边界（优先拆分）：遇到新事务ID且当前批次已有数据
    - 时间边界：距离上次提交超过 commit_interval_ms
    """

    def __init__(self, max_transaction_rows: int = 1000,
                 commit_interval_ms: int = 0,
                 respect_transaction_boundary: bool = True,
                 allow_split_large_transaction: bool = True):
        """
        初始化事务拆分器。

        Args:
            max_transaction_rows: 单事务最大行数（硬边界）
            commit_interval_ms: 强制提交间隔（毫秒），0表示不按时间提交
            respect_transaction_boundary: 是否尊重事务边界（尽量不拆分同一源事务）
            allow_split_large_transaction: 是否允许拆分超大事务
                                           （超过 max_transaction_rows 的单个事务仍会被拆分）
        """
        self.max_transaction_rows = max_transaction_rows
        self.commit_interval_ms = commit_interval_ms
        self.respect_transaction_boundary = respect_transaction_boundary
        self.allow_split_large_transaction = allow_split_large_transaction

        self._current_batch: List[TransactionalRow] = []
        self._current_txn_ids: set = set()
        self._last_commit_time = time.time()
        self._batch_id_counter = 0
        self._boundary_history: List[SplitBoundary] = []
        self._current_start_safe_point: Optional[Any] = None
        self._lock = threading.Lock()

    def add_row(self, row: Tuple,
                transaction_id: Optional[str] = None,
                table_name: Optional[str] = None,
                operation: Optional[str] = None,
                safe_point: Optional[Any] = None) -> bool:
        """
        添加一行到当前批次。

        Args:
            row: 数据行
            transaction_id: 源事务ID（用于事务边界识别）
            table_name: 表名
            operation: 操作类型
            safe_point: 安全点（用于断点续传的精确位置）

        Returns:
            True 表示需要立即提交（达到拆分边界）
        """
        with self._lock:
            tx_row = TransactionalRow(
                row=row,
                transaction_id=transaction_id,
                table_name=table_name,
                operation=operation,
                safe_point=safe_point,
            )

            if len(self._current_batch) == 0:
                self._current_start_safe_point = safe_point

            self._current_batch.append(tx_row)
            if transaction_id:
                self._current_txn_ids.add(transaction_id)

            return self._check_should_commit(transaction_id)

    def _check_should_commit(self, new_transaction_id: Optional[str]) -> bool:
        """
        内部方法：检查是否应该提交当前批次。

        判定优先级：
        1. 硬边界（行数超限）> 2. 事务边界 > 3. 时间边界
        """
        current_size = len(self._current_batch)

        if current_size >= self.max_transaction_rows:
            if not self.allow_split_large_transaction and len(self._current_txn_ids) == 1:
                return False
            return True

        if (self.respect_transaction_boundary and
                new_transaction_id and
                current_size > 0 and
                self._current_txn_ids and
                new_transaction_id not in self._current_txn_ids):
            return True

        if self.commit_interval_ms > 0:
            elapsed_ms = (time.time() - self._last_commit_time) * 1000
            if elapsed_ms >= self.commit_interval_ms and current_size > 0:
                return True

        return False

    def get_batch_and_reset(self) -> Tuple[List[Tuple], SplitBoundary]:
        """
        获取当前批次数据并重置，同时返回拆分边界信息。

        Returns:
            (rows_list, split_boundary) 元组
        """
        with self._lock:
            rows = [tx_row.row for tx_row in self._current_batch]

            end_safe_point = None
            if self._current_batch:
                end_safe_point = self._current_batch[-1].safe_point

            boundary = SplitBoundary(
                batch_id=self._batch_id_counter,
                start_safe_point=self._current_start_safe_point,
                end_safe_point=end_safe_point,
                row_count=len(self._current_batch),
                transaction_ids=list(self._current_txn_ids),
            )

            self._batch_id_counter += 1
            self._boundary_history.append(boundary)

            self._current_batch.clear()
            self._current_txn_ids.clear()
            self._current_start_safe_point = None
            self._last_commit_time = time.time()

            return rows, boundary

    def mark_batch_committed(self, batch_id: int):
        """标记批次已提交，更新边界记录。"""
        with self._lock:
            for boundary in self._boundary_history:
                if boundary.batch_id == batch_id:
                    boundary.committed = True
                    boundary.commit_time = time.time()
                    break

    def get_last_committed_boundary(self) -> Optional[SplitBoundary]:
        """获取最后一个成功提交的批次边界。"""
        with self._lock:
            for boundary in reversed(self._boundary_history):
                if boundary.committed:
                    return boundary
            return None

    def get_uncommitted_boundaries(self) -> List[SplitBoundary]:
        """获取所有未提交的批次边界（异常恢复时使用）。"""
        with self._lock:
            return [b for b in self._boundary_history if not b.committed]

    def has_pending(self) -> bool:
        """检查是否有待提交数据。"""
        with self._lock:
            return len(self._current_batch) > 0

    def pending_count(self) -> int:
        """获取待提交行数。"""
        with self._lock:
            return len(self._current_batch)

    def split_rows(self, rows: List[Tuple],
                   safe_points: Optional[List[Any]] = None,
                   transaction_ids: Optional[List[str]] = None) -> Generator[Tuple[List[Tuple], SplitBoundary], None, None]:
        """
        将大数据集拆分为多个小批次（带边界信息）。

        Args:
            rows: 原始数据行列表
            safe_points: 每行对应的安全点列表（用于断点续传）
            transaction_ids: 每行对应的事务ID列表

        Yields:
            (rows_batch, split_boundary) 元组
        """
        current_batch: List[TransactionalRow] = []
        current_txn_ids: set = set()
        batch_start_safe = None
        batch_id = 0

        for i, row in enumerate(rows):
            sp = safe_points[i] if safe_points and i < len(safe_points) else None
            tx_id = transaction_ids[i] if transaction_ids and i < len(transaction_ids) else None

            tx_row = TransactionalRow(
                row=row,
                transaction_id=tx_id,
                safe_point=sp,
            )

            if len(current_batch) == 0:
                batch_start_safe = sp

            current_batch.append(tx_row)
            if tx_id:
                current_txn_ids.add(tx_id)

            need_split = len(current_batch) >= self.max_transaction_rows

            if need_split:
                end_sp = current_batch[-1].safe_point
                boundary = SplitBoundary(
                    batch_id=batch_id,
                    start_safe_point=batch_start_safe,
                    end_safe_point=end_sp,
                    row_count=len(current_batch),
                    transaction_ids=list(current_txn_ids),
                )
                yield [r.row for r in current_batch], boundary

                batch_id += 1
                current_batch = []
                current_txn_ids = set()
                batch_start_safe = None

        if current_batch:
            end_sp = current_batch[-1].safe_point
            boundary = SplitBoundary(
                batch_id=batch_id,
                start_safe_point=batch_start_safe,
                end_safe_point=end_sp,
                row_count=len(current_batch),
                transaction_ids=list(current_txn_ids),
            )
            yield [r.row for r in current_batch], boundary

    def get_stats(self) -> Dict[str, Any]:
        """获取拆分器统计信息。"""
        with self._lock:
            committed = sum(1 for b in self._boundary_history if b.committed)
            return {
                "total_batches": len(self._boundary_history),
                "committed_batches": committed,
                "pending_batches": len(self._boundary_history) - committed,
                "current_pending_rows": len(self._current_batch),
                "max_transaction_rows": self.max_transaction_rows,
                "respect_transaction_boundary": self.respect_transaction_boundary,
            }


# ====================================================================== #
#                        2. 智能分页器
# ====================================================================== #

class SmartPaginator:
    """
    智能分页器。

    功能：
    - 基于主键分布优化分页查询
    - 数据稀疏区域跳块优化
    - 避免深分页性能问题
    - 动态调整 chunk 大小
    """

    def __init__(self, min_chunk_size: int = 1000,
                 max_chunk_size: int = 100000,
                 target_fetch_time_ms: float = 1000.0):
        """
        初始化智能分页器。

        Args:
            min_chunk_size: 最小 chunk 大小
            max_chunk_size: 最大 chunk 大小
            target_fetch_time_ms: 目标单次查询耗时（毫秒）
        """
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.target_fetch_time_ms = target_fetch_time_ms
        self.current_chunk_size = min_chunk_size * 10
        self._fetch_history: deque = deque(maxlen=20)
        self._pk_density_cache: Dict[str, float] = {}

    def adjust_chunk_size(self, actual_fetch_time_ms: float, rows_fetched: int):
        """
        根据实际查询耗时调整 chunk 大小。

        Args:
            actual_fetch_time_ms: 实际查询耗时（毫秒）
            rows_fetched: 实际返回行数
        """
        if rows_fetched == 0:
            return

        self._fetch_history.append((actual_fetch_time_ms, rows_fetched))

        if actual_fetch_time_ms > self.target_fetch_time_ms * 1.5:
            self.current_chunk_size = max(
                self.min_chunk_size,
                int(self.current_chunk_size * 0.7)
            )
            logger.debug(
                "查询较慢 (%.1fms)，减小 chunk 到 %d",
                actual_fetch_time_ms, self.current_chunk_size,
            )
        elif actual_fetch_time_ms < self.target_fetch_time_ms * 0.5:
            self.current_chunk_size = min(
                self.max_chunk_size,
                int(self.current_chunk_size * 1.3)
            )
            logger.debug(
                "查询较快 (%.1fms)，增大 chunk 到 %d",
                actual_fetch_time_ms, self.current_chunk_size,
            )

    def estimate_optimal_chunk(self, pk_range_size: int, total_rows: int) -> int:
        """
        根据主键范围和行数估算最优 chunk 大小。

        Args:
            pk_range_size: 主键范围大小 (max_pk - min_pk)
            total_rows: 总行数

        Returns:
            推荐的 chunk 大小
        """
        if pk_range_size == 0 or total_rows == 0:
            return self.current_chunk_size

        density = total_rows / pk_range_size
        self._pk_density_cache["default"] = density

        if density < 0.1:
            recommended = min(self.max_chunk_size, int(pk_range_size * 0.05))
            logger.debug("数据稀疏 (密度=%.3f)，推荐大 chunk: %d", density, recommended)
        elif density > 10:
            recommended = max(self.min_chunk_size, int(pk_range_size * 0.001))
            logger.debug("数据密集 (密度=%.3f)，推荐小 chunk: %d", density, recommended)
        else:
            recommended = self.current_chunk_size

        return max(self.min_chunk_size, min(self.max_chunk_size, recommended))

    @staticmethod
    def build_keyset_pagination_query(
        table_name: str, columns: List[str], pk_col: str,
        last_pk: int, batch_size: int,
        quote_identifier: Callable[[str], str]
    ) -> Tuple[str, Tuple]:
        """
        构建基于 keyset 的分页查询（避免深分页）。

        使用 WHERE pk > last_pk ORDER BY pk LIMIT N
        替代 OFFSET M LIMIT N

        Args:
            table_name: 表名
            columns: 列名列表
            pk_col: 主键列名
            last_pk: 上一次的最后主键值
            batch_size: 批次大小
            quote_identifier: 标识符引用函数

        Returns:
            (SQL语句, 参数元组)
        """
        col_list = ", ".join(quote_identifier(c) for c in columns)
        qpk = quote_identifier(pk_col)
        qt = quote_identifier(table_name)

        sql = (
            f"SELECT {col_list} FROM {qt} "
            f"WHERE {qpk} > %s "
            f"ORDER BY {qpk} "
            f"LIMIT %s"
        )
        return sql, (last_pk, batch_size)

    def calculate_skip_points(self, min_pk: int, max_pk: int,
                              sample_density: float = 0.01) -> List[int]:
        """
        计算跳块采样点（用于快速估算数据分布）。

        Args:
            min_pk: 最小主键值
            max_pk: 最大主键值
            sample_density: 采样密度

        Returns:
            采样点列表
        """
        range_size = max_pk - min_pk
        if range_size <= 0:
            return []

        num_samples = max(2, int(range_size * sample_density))
        step = range_size // num_samples
        return [min_pk + i * step for i in range(num_samples + 1)]


# ====================================================================== #
#                        3. 自适应调优器
# ====================================================================== #

class AdaptiveTuner:
    """
    自适应性能调优器。

    功能：
    - 监控源/目标数据库响应时间
    - 动态调整 batch_size 和并发数
    - 高负载时自动限流
    - 低负载时最大化吞吐量
    """

    def __init__(self, initial_batch_size: int = 5000,
                 min_batch_size: int = 100,
                 max_batch_size: int = 50000,
                 initial_parallelism: int = 4,
                 min_parallelism: int = 1,
                 max_parallelism: int = 16,
                 latency_threshold_ms: float = 5000.0):
        """
        初始化自适应调优器。

        Args:
            initial_batch_size: 初始批量大小
            min_batch_size: 最小批量大小
            max_batch_size: 最大批量大小
            initial_parallelism: 初始并行数
            min_parallelism: 最小并行数
            max_parallelism: 最大并行数
            latency_threshold_ms: 延迟阈值（毫秒），超过则开始降载
        """
        self.current_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.current_parallelism = initial_parallelism
        self.min_parallelism = min_parallelism
        self.max_parallelism = max_parallelism
        self.latency_threshold_ms = latency_threshold_ms

        self._metrics_history: deque = deque(maxlen=50)
        self._lock = threading.Lock()

        logger.info(
            "自适应调优器初始化: batch_size=%d, parallelism=%d",
            initial_batch_size, initial_parallelism,
        )

    def record_metrics(self, metrics: PerformanceMetrics):
        """记录性能指标。"""
        with self._lock:
            self._metrics_history.append(metrics)
            self._adjust_parameters()

    def _adjust_parameters(self):
        """根据历史指标调整参数（内部调用，不加锁）。"""
        if len(self._metrics_history) < 5:
            return

        recent = list(self._metrics_history)[-10:]
        avg_target_latency = sum(m.target_response_time for m in recent) / len(recent)
        avg_rows_per_sec = sum(m.rows_per_second for m in recent) / len(recent)

        if avg_target_latency > self.latency_threshold_ms:
            self.current_batch_size = max(
                self.min_batch_size,
                int(self.current_batch_size * 0.8)
            )
            self.current_parallelism = max(
                self.min_parallelism,
                self.current_parallelism - 1
            )
            logger.warning(
                "目标库延迟过高 (%.1fms)，降载: batch_size=%d, parallelism=%d",
                avg_target_latency, self.current_batch_size, self.current_parallelism,
            )
        elif (avg_target_latency < self.latency_threshold_ms * 0.3 and
              avg_rows_per_sec > 0):
            self.current_batch_size = min(
                self.max_batch_size,
                int(self.current_batch_size * 1.2)
            )
            if self.current_parallelism < self.max_parallelism:
                self.current_parallelism += 1
                logger.info(
                    "目标库负载较低，提升: batch_size=%d, parallelism=%d",
                    self.current_batch_size, self.current_parallelism,
                )

    def get_batch_size(self) -> int:
        """获取当前推荐的批量大小。"""
        with self._lock:
            return self.current_batch_size

    def get_parallelism(self) -> int:
        """获取当前推荐的并行数。"""
        with self._lock:
            return self.current_parallelism

    def should_throttle(self) -> bool:
        """判断是否需要限流。"""
        if len(self._metrics_history) < 3:
            return False
        recent = list(self._metrics_history)[-3:]
        return all(m.target_response_time > self.latency_threshold_ms * 1.5 for m in recent)


# ====================================================================== #
#                        4. 错误重试管理器
# ====================================================================== #

class RetryManager:
    """
    错误重试管理器。

    功能：
    - 可配置的重试策略（指数退避、线性退避等）
    - 批量失败时降级为逐行重试
    - 死信队列持久化
    - 失败数据追溯
    """

    def __init__(self, max_retries: int = 3,
                 retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                 initial_delay_ms: int = 100,
                 max_delay_ms: int = 10000):
        """
        初始化重试管理器。

        Args:
            max_retries: 最大重试次数
            retry_strategy: 重试策略
            initial_delay_ms: 初始延迟（毫秒）
            max_delay_ms: 最大延迟（毫秒）
        """
        self.max_retries = max_retries
        self.retry_strategy = retry_strategy
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms
        self.dead_letter_queue: List[DeadLetterRecord] = []
        self._dlq_lock = threading.Lock()
        self._stats = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
            "dead_letter_count": 0,
        }

    def _calculate_delay(self, attempt: int) -> float:
        """计算重试延迟（秒）。"""
        if self.retry_strategy == RetryStrategy.IMMEDIATE:
            return 0
        elif self.retry_strategy == RetryStrategy.LINEAR_BACKOFF:
            delay_ms = self.initial_delay_ms * attempt
        else:
            delay_ms = self.initial_delay_ms * (2 ** (attempt - 1))

        delay_ms = min(delay_ms, self.max_delay_ms)
        return delay_ms / 1000.0

    def execute_with_retry(self, operation: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """
        带重试的执行操作。

        Args:
            operation: 要执行的操作函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            (是否成功, 结果/错误信息)
        """
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                result = operation(*args, **kwargs)
                if attempt > 1:
                    self._stats["successful_retries"] += 1
                return True, result
            except Exception as e:
                last_error = str(e)
                self._stats["total_retries"] += 1

                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        "操作失败 (尝试 %d/%d)，%.1f秒后重试: %s",
                        attempt, self.max_retries, delay, last_error,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "操作重试 %d 次后仍失败: %s",
                        self.max_retries, last_error,
                    )
                    self._stats["failed_retries"] += 1

        return False, last_error

    def execute_batch_with_fallback(self, batch_operation: Callable,
                                     rows: List[Tuple],
                                     table_name: str = "") -> BatchResult:
        """
        批量执行，失败时降级为逐行执行。

        Args:
            batch_operation: 批量操作函数，接收 rows 参数
            rows: 数据行列表
            table_name: 表名（用于死信记录）

        Returns:
            批量处理结果
        """
        start_time = time.time()
        result = BatchResult(success=False)

        success, _ = self.execute_with_retry(batch_operation, rows)
        if success:
            result.success = True
            result.rows_processed = len(rows)
            result.execution_time = time.time() - start_time
            return result

        logger.warning("批量操作失败，降级为逐行处理 (%d 行)", len(rows))

        failed_rows = []
        processed = 0
        for i, row in enumerate(rows):
            success, _ = self.execute_with_retry(batch_operation, [row])
            if success:
                processed += 1
            else:
                failed_rows.append(row)
                self._add_to_dead_letter(
                    table_name=table_name,
                    operation="insert",
                    data={"row_index": i, "row_values": list(row)},
                    error=f"逐行插入失败",
                )

        result.success = len(failed_rows) == 0
        result.rows_processed = processed
        result.rows_failed = len(failed_rows)
        result.failed_rows = failed_rows
        result.execution_time = time.time() - start_time
        result.error = f"{len(failed_rows)} 行失败" if failed_rows else None

        return result

    def _add_to_dead_letter(self, table_name: str, operation: str,
                             data: Dict[str, Any], error: str):
        """添加到死信队列。"""
        import uuid
        record = DeadLetterRecord(
            id=str(uuid.uuid4()),
            table_name=table_name,
            operation=operation,
            data=data,
            error=error,
        )
        with self._dlq_lock:
            self.dead_letter_queue.append(record)
            self._stats["dead_letter_count"] += 1
        logger.warning("加入死信队列: table=%s, op=%s, id=%s", table_name, operation, record.id)

    def get_dead_letters(self, table_name: str = None) -> List[DeadLetterRecord]:
        """获取死信记录。"""
        with self._dlq_lock:
            if table_name:
                return [r for r in self.dead_letter_queue if r.table_name == table_name]
            return self.dead_letter_queue.copy()

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息。"""
        return dict(self._stats)

    def clear_dead_letters(self):
        """清空死信队列。"""
        with self._dlq_lock:
            self.dead_letter_queue.clear()
            self._stats["dead_letter_count"] = 0


# ====================================================================== #
#                        5. 一致性校验器
# ====================================================================== #

class ConsistencyChecker:
    """
    数据一致性校验器。

    功能：
    - 基于 CRC32 校验和的快速比对
    - 支持抽样校验和全量校验
    - 差异数据检测和报告
    """

    def __init__(self, sample_ratio: float = 0.1,
                 max_sample_rows: int = 10000):
        """
        初始化一致性校验器。

        Args:
            sample_ratio: 抽样比例 (0-1)
            max_sample_rows: 最大抽样行数
        """
        self.sample_ratio = sample_ratio
        self.max_sample_rows = max_sample_rows
        self._cache = {}

    @staticmethod
    def _row_checksum(row: Tuple) -> int:
        """计算一行数据的校验和。"""
        import zlib
        row_str = "|".join(str(v) if v is not None else "" for v in row)
        return zlib.crc32(row_str.encode("utf-8"))

    def calculate_table_checksum(self, conn, adapter, table_name: str,
                                  columns: List[str], pk_col: str,
                                  sample_mode: bool = True) -> Tuple[int, int]:
        """
        计算表的校验和。

        Args:
            conn: 数据库连接
            adapter: 数据库适配器
            table_name: 表名
            columns: 列名列表
            pk_col: 主键列名
            sample_mode: 是否抽样模式

        Returns:
            (校验和, 行数)
        """
        qt = adapter.quote_identifier(table_name)
        qpk = adapter.quote_identifier(pk_col)
        col_list = ", ".join(adapter.quote_identifier(c) for c in columns)

        if sample_mode:
            total_rows = adapter.get_table_row_count(conn, table_name)
            sample_size = min(
                self.max_sample_rows,
                max(1, int(total_rows * self.sample_ratio))
            )
            sql = (
                f"SELECT {col_list} FROM {qt} "
                f"ORDER BY {qpk} "
                f"LIMIT %s"
            ) if adapter.db_type == "mysql" else (
                f"SELECT {col_list} FROM {qt} "
                f"ORDER BY {qpk} "
                f"FETCH FIRST %s ROWS ONLY"
            )
            params = (sample_size,)
        else:
            sql = f"SELECT {col_list} FROM {qt} ORDER BY {qpk}"
            params = ()

        cur = conn.cursor()
        cur.execute(sql, params)

        checksum = 0
        row_count = 0
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                checksum ^= self._row_checksum(tuple(row[c] for c in columns) if hasattr(row, 'keys') else row)
                row_count += 1

        cur.close()
        return checksum, row_count

    def compare_tables(self, src_conn, tgt_conn, src_adapter, tgt_adapter,
                       table_name: str, columns: List[str], pk_col: str,
                       sample_mode: bool = True) -> Dict[str, Any]:
        """
        比较源表和目标表的一致性。

        Returns:
            比较结果字典
        """
        logger.info("开始校验表 %s 一致性 (抽样=%s)", table_name, sample_mode)

        src_checksum, src_count = self.calculate_table_checksum(
            src_conn, src_adapter, table_name, columns, pk_col, sample_mode,
        )
        tgt_checksum, tgt_count = self.calculate_table_checksum(
            tgt_conn, tgt_adapter, table_name, columns, pk_col, sample_mode,
        )

        result = {
            "table": table_name,
            "sample_mode": sample_mode,
            "source_rows": src_count,
            "target_rows": tgt_count,
            "source_checksum": src_checksum,
            "target_checksum": tgt_checksum,
            "row_count_match": src_count == tgt_count,
            "checksum_match": src_checksum == tgt_checksum,
            "consistent": src_count == tgt_count and src_checksum == tgt_checksum,
        }

        if result["consistent"]:
            logger.info("表 %s 一致性校验通过", table_name)
        else:
            logger.warning(
                "表 %s 一致性校验不通过: 行数(%d vs %d), 校验和(0x%x vs 0x%x)",
                table_name, src_count, tgt_count, src_checksum, tgt_checksum,
            )

        return result

    def find_differences(self, src_conn, tgt_conn, src_adapter, tgt_adapter,
                         table_name: str, columns: List[str], pk_col: str,
                         max_diff: int = 100) -> List[Dict[str, Any]]:
        """
        查找源表和目标表的差异数据。

        Args:
            max_diff: 最大返回差异数

        Returns:
            差异列表
        """
        differences = []
        qt_src = src_adapter.quote_identifier(table_name)
        qt_tgt = tgt_adapter.quote_identifier(table_name)
        qpk = src_adapter.quote_identifier(pk_col)
        col_list = ", ".join(src_adapter.quote_identifier(c) for c in columns)

        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        try:
            src_cur.execute(f"SELECT {col_list} FROM {qt_src} ORDER BY {qpk}")
            tgt_cur.execute(f"SELECT {col_list} FROM {qt_tgt} ORDER BY {qpk}")

            src_rows = src_cur.fetchall()
            tgt_rows = tgt_cur.fetchall()

            src_dict = {row[pk_col] if hasattr(row, 'keys') else row[0]: row for row in src_rows}
            tgt_dict = {row[pk_col] if hasattr(row, 'keys') else row[0]: row for row in tgt_rows}

            all_pks = set(src_dict.keys()) | set(tgt_dict.keys())
            for pk in sorted(all_pks)[:max_diff]:
                src_row = src_dict.get(pk)
                tgt_row = tgt_dict.get(pk)

                if src_row is None:
                    differences.append({
                        "type": "missing_in_source",
                        "pk": pk,
                        "target_row": list(tgt_row),
                    })
                elif tgt_row is None:
                    differences.append({
                        "type": "missing_in_target",
                        "pk": pk,
                        "source_row": list(src_row),
                    })
                else:
                    src_tuple = tuple(src_row[c] for c in columns) if hasattr(src_row, 'keys') else src_row
                    tgt_tuple = tuple(tgt_row[c] for c in columns) if hasattr(tgt_row, 'keys') else tgt_row
                    if src_tuple != tgt_tuple:
                        differences.append({
                            "type": "value_mismatch",
                            "pk": pk,
                            "source_row": list(src_tuple),
                            "target_row": list(tgt_tuple),
                        })

        finally:
            src_cur.close()
            tgt_cur.close()

        return differences


# ====================================================================== #
#                        6. 背压控制器
# ====================================================================== #

class BackpressureController:
    """
    背压控制器。

    功能：
    - 监控事件队列长度
    - 队列达到阈值时暂停 CDC 读取
    - 内存使用监控
    - 大事务事件合并
    """

    def __init__(self, queue_high_watermark: int = 8000,
                 queue_low_watermark: int = 3000,
                 max_memory_mb: int = 512):
        """
        初始化背压控制器。

        Args:
            queue_high_watermark: 队列高水位线，超过则暂停生产
            queue_low_watermark: 队列低水位线，低于则恢复生产
            max_memory_mb: 最大内存使用（MB）
        """
        self.queue_high_watermark = queue_high_watermark
        self.queue_low_watermark = queue_low_watermark
        self.max_memory_mb = max_memory_mb
        self._is_paused = False
        self._pause_count = 0
        self._lock = threading.Lock()
        self._stats = {
            "pause_count": 0,
            "total_pause_time_ms": 0,
            "last_pause_at": 0,
        }

    def check_should_pause(self, queue_size: int,
                            memory_usage_mb: float = 0) -> bool:
        """
        检查是否应该暂停生产。

        Args:
            queue_size: 当前队列大小
            memory_usage_mb: 当前内存使用（MB）

        Returns:
            True 表示应该暂停
        """
        with self._lock:
            if self._is_paused:
                return queue_size > self.queue_low_watermark
            else:
                should_pause = (
                    queue_size >= self.queue_high_watermark or
                    memory_usage_mb >= self.max_memory_mb
                )
                if should_pause:
                    self._is_paused = True
                    self._stats["pause_count"] += 1
                    self._stats["last_pause_at"] = time.time()
                    logger.warning(
                        "触发背压控制，暂停生产: queue_size=%d, memory=%.1fMB",
                        queue_size, memory_usage_mb,
                    )
                return should_pause

    def check_should_resume(self, queue_size: int,
                             memory_usage_mb: float = 0) -> bool:
        """
        检查是否应该恢复生产。

        Returns:
            True 表示可以恢复
        """
        with self._lock:
            if not self._is_paused:
                return False

            should_resume = (
                queue_size <= self.queue_low_watermark and
                memory_usage_mb < self.max_memory_mb * 0.7
            )
            if should_resume:
                self._is_paused = False
                pause_duration = (time.time() - self._stats["last_pause_at"]) * 1000
                self._stats["total_pause_time_ms"] += pause_duration
                logger.info(
                    "背压缓解，恢复生产: queue_size=%d, 暂停时长=%.1fms",
                    queue_size, pause_duration,
                )
            return should_resume

    def is_paused(self) -> bool:
        """检查是否处于暂停状态。"""
        with self._lock:
            return self._is_paused

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息。"""
        return dict(self._stats)

    @staticmethod
    def merge_transaction_events(events: List[Any]) -> List[Any]:
        """
        合并同一事务中的同类事件（简单实现）。

        策略：
        - 同一行的多次 UPDATE 合并为最后一次
        - INSERT + DELETE 互相抵消
        - UPDATE + DELETE 保留 DELETE
        """
        if len(events) <= 1:
            return events

        pk_events: Dict[Tuple[str, Any], Any] = {}
        for event in events:
            if not hasattr(event, 'table_name') or not hasattr(event, 'data'):
                continue
            pk_value = event.data.get('id', str(event.data))
            key = (event.table_name, pk_value)

            if key in pk_events:
                existing = pk_events[key]
                if event.event_type == "delete":
                    pk_events[key] = event
                elif event.event_type == "update" and existing.event_type == "insert":
                    new_event = event
                    new_event.event_type = "insert"
                    pk_events[key] = new_event
                else:
                    pk_events[key] = event
            else:
                pk_events[key] = event

        merged = list(pk_events.values())
        if len(merged) < len(events):
            logger.debug("事件合并: %d -> %d", len(events), len(merged))
        return merged


# ====================================================================== #
#                        全局便捷函数
# ====================================================================== #

def create_optimizer_config(sync_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    从同步配置创建优化器配置。

    Args:
        sync_cfg: 同步配置字典

    Returns:
        优化器配置字典
    """
    return {
        "max_transaction_rows": sync_cfg.get("max_transaction_rows", 1000),
        "min_chunk_size": sync_cfg.get("min_chunk_size", 1000),
        "max_chunk_size": sync_cfg.get("max_chunk_size", 100000),
        "initial_batch_size": sync_cfg.get("batch_insert_size", 5000),
        "max_retries": sync_cfg.get("max_retries", 3),
        "enable_backpressure": sync_cfg.get("enable_backpressure", True),
        "queue_high_watermark": sync_cfg.get("queue_high_watermark", 8000),
        "queue_low_watermark": sync_cfg.get("queue_low_watermark", 3000),
        "enable_adaptive_tuning": sync_cfg.get("enable_adaptive_tuning", True),
        "sample_ratio": sync_cfg.get("consistency_sample_ratio", 0.1),
    }
