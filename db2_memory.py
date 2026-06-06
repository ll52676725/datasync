import threading
import time
import logging
import json
import os
from datetime import datetime
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

PERSISTENCE_FILE = os.path.join(os.path.dirname(__file__), "sync_state.json")


class DB2MemoryStore:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_store()
        return cls._instance

    def _init_store(self):
        self._users: Dict[str, Dict] = {}
        self._configs: Dict[str, Dict] = {}
        self._sync_progress: Dict[str, Dict] = {}
        self._sync_tasks: Dict[str, Dict] = {}
        self._sessions: Dict[str, Dict] = {}
        self._realtime_tasks: Dict[str, Dict] = {}
        self._persistence_lock = threading.Lock()
        self._last_persist_time = 0
        self._persist_interval = 1.0
        self._load_from_persistence()
        self._init_default_data()

    def _init_default_data(self):
        if not self._users:
            self._users = {
                "admin": {
                    "username": "admin",
                    "password": "admin123",
                    "created_at": datetime.now().isoformat(),
                    "role": "admin"
                }
            }

    def _load_from_persistence(self):
        """从磁盘 JSON 文件加载持久化状态。"""
        try:
            if os.path.exists(PERSISTENCE_FILE):
                with open(PERSISTENCE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._configs = data.get("configs", {})
                self._sync_progress = data.get("sync_progress", {})
                self._sync_tasks = data.get("sync_tasks", {})
                self._realtime_tasks = data.get("realtime_tasks", {})
                self._users = data.get("users", self._users)
                logger.info("从持久化文件加载状态成功: %s", PERSISTENCE_FILE)
                logger.info("  - 配置: %d 个", len(self._configs))
                logger.info("  - 实时任务: %d 个", len(self._realtime_tasks))
        except Exception as e:
            logger.error("加载持久化状态失败: %s", e)

    def _persist_to_disk(self):
        """将状态持久化到磁盘 JSON 文件。"""
        try:
            data = {
                "configs": self._configs,
                "sync_progress": self._sync_progress,
                "sync_tasks": self._sync_tasks,
                "realtime_tasks": self._realtime_tasks,
                "users": self._users,
                "persisted_at": datetime.now().isoformat(),
            }
            tmp_file = PERSISTENCE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, PERSISTENCE_FILE)
            logger.debug("状态持久化成功: %s", PERSISTENCE_FILE)
        except Exception as e:
            logger.error("持久化状态失败: %s", e)

    def _mark_dirty(self, immediate: bool = False):
        """标记状态已变更，触发异步持久化。

        Args:
            immediate: 是否立即持久化（用于关键状态更新，如断点位置）
        """
        now = time.time()
        if immediate or (now - self._last_persist_time >= self._persist_interval):
            with self._persistence_lock:
                now = time.time()
                if immediate or (now - self._last_persist_time >= self._persist_interval):
                    self._persist_to_disk()
                    self._last_persist_time = now

    def create_session(self, username: str) -> str:
        import uuid
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = {
            "username": username,
            "created_at": datetime.now().isoformat(),
            "expires_at": time.time() + 86400
        }
        return session_id

    def validate_session(self, session_id: str) -> Optional[str]:
        session = self._sessions.get(session_id)
        if session and session["expires_at"] > time.time():
            return session["username"]
        if session_id in self._sessions:
            del self._sessions[session_id]
        return None

    def delete_session(self, session_id: str):
        if session_id in self._sessions:
            del self._sessions[session_id]

    def get_user(self, username: str) -> Optional[Dict]:
        return self._users.get(username)

    def verify_user(self, username: str, password: str) -> bool:
        user = self.get_user(username)
        return user is not None and user["password"] == password

    def save_config(self, config_name: str, config: Dict, username: str) -> Dict:
        self._configs[config_name] = {
            "name": config_name,
            "config": config,
            "created_by": username,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        self._mark_dirty()
        return self._configs[config_name]

    def get_config(self, config_name: str) -> Optional[Dict]:
        cfg = self._configs.get(config_name)
        return cfg["config"] if cfg else None

    def list_configs(self) -> List[Dict]:
        return list(self._configs.values())

    def delete_config(self, config_name: str) -> bool:
        if config_name in self._configs:
            del self._configs[config_name]
            self._mark_dirty()
            return True
        return False

    def init_progress(self, table_name: str, total_rows: int, primary_key: str, task_id: str):
        self._sync_progress[table_name] = {
            "id": len(self._sync_progress) + 1,
            "table_name": table_name,
            "total_rows": total_rows,
            "synced_rows": 0,
            "primary_key": primary_key,
            "last_pk_value": 0,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "updated_at": datetime.now().isoformat(),
            "task_id": task_id
        }

    def get_progress(self, table_name: str) -> Optional[Dict]:
        return self._sync_progress.get(table_name)

    def update_progress(self, table_name: str, synced_rows: int, last_pk_value: int):
        if table_name in self._sync_progress:
            self._sync_progress[table_name].update({
                "synced_rows": synced_rows,
                "last_pk_value": last_pk_value,
                "updated_at": datetime.now().isoformat()
            })

    def mark_progress_done(self, table_name: str, total_synced: int):
        if table_name in self._sync_progress:
            self._sync_progress[table_name].update({
                "synced_rows": total_synced,
                "status": "done",
                "finished_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            })

    def mark_progress_failed(self, table_name: str, error: str):
        if table_name in self._sync_progress:
            self._sync_progress[table_name].update({
                "status": "failed",
                "error": error,
                "updated_at": datetime.now().isoformat()
            })

    def reset_progress(self, table_name: Optional[str] = None):
        if table_name:
            if table_name in self._sync_progress:
                del self._sync_progress[table_name]
        else:
            self._sync_progress.clear()

    def list_progress(self, task_id: Optional[str] = None) -> List[Dict]:
        if task_id:
            return [
                p for p in self._sync_progress.values()
                if p.get("task_id") == task_id
            ]
        return list(self._sync_progress.values())

    def create_task(self, task_id: str, config_name: str, username: str, mode: str) -> Dict:
        self._sync_tasks[task_id] = {
            "task_id": task_id,
            "config_name": config_name,
            "username": username,
            "mode": mode,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "progress": 0,
            "message": "任务已创建，等待启动..."
        }
        return self._sync_tasks[task_id]

    def update_task(self, task_id: str, **kwargs):
        if task_id in self._sync_tasks:
            self._sync_tasks[task_id].update(kwargs)
            self._sync_tasks[task_id]["updated_at"] = datetime.now().isoformat()
            self._mark_dirty()

    def get_task(self, task_id: str) -> Optional[Dict]:
        return self._sync_tasks.get(task_id)

    def list_tasks(self, username: Optional[str] = None) -> List[Dict]:
        tasks = list(self._sync_tasks.values())
        if username:
            tasks = [t for t in tasks if t["username"] == username]
        return sorted(tasks, key=lambda x: x["created_at"], reverse=True)

    def get_task_progress(self, task_id: str) -> Dict:
        task = self.get_task(task_id)
        if not task:
            return {}
        progress_list = self.list_progress(task_id)
        if not progress_list:
            return {
                "task": task,
                "tables": [],
                "overall": {
                    "total_tables": 0,
                    "completed_tables": 0,
                    "total_rows": 0,
                    "synced_rows": 0,
                    "percentage": 0
                }
            }
        total_tables = len(progress_list)
        completed_tables = sum(1 for p in progress_list if p["status"] == "done")
        total_rows = sum(p["total_rows"] for p in progress_list)
        synced_rows = sum(p["synced_rows"] for p in progress_list)
        percentage = (synced_rows / total_rows * 100) if total_rows > 0 else 0
        if completed_tables == total_tables and total_tables > 0:
            percentage = 100
        return {
            "task": task,
            "tables": progress_list,
            "overall": {
                "total_tables": total_tables,
                "completed_tables": completed_tables,
                "total_rows": total_rows,
                "synced_rows": synced_rows,
                "percentage": round(percentage, 2)
            }
        }

    # ------------------------------------------------------------------ #
    #                        实时同步任务管理
    # ------------------------------------------------------------------ #

    def create_realtime_task(self, task_id: str, config_name: str, username: str) -> Dict:
        """
        创建实时同步任务。

        Args:
            task_id: 任务 ID
            config_name: 配置名称
            username: 创建者用户名

        Returns:
            任务信息字典
        """
        self._realtime_tasks[task_id] = {
            "task_id": task_id,
            "config_name": config_name,
            "username": username,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "stopped_at": None,
            "message": "实时任务已创建",
            "binlog_file": "",
            "binlog_pos": 0,
            "stats": {
                "total_events": 0,
                "insert_events": 0,
                "update_events": 0,
                "delete_events": 0,
                "synced_events": 0,
                "failed_events": 0,
            },
        }
        logger.info("创建实时任务: %s (配置: %s, 用户: %s)", task_id, config_name, username)
        self._mark_dirty()
        return self._realtime_tasks[task_id]

    def get_realtime_task(self, task_id: str) -> Optional[Dict]:
        """获取指定实时任务。"""
        return self._realtime_tasks.get(task_id)

    def list_realtime_tasks(self, username: Optional[str] = None) -> List[Dict]:
        """列出所有实时任务。"""
        tasks = list(self._realtime_tasks.values())
        if username:
            tasks = [t for t in tasks if t["username"] == username]
        return sorted(tasks, key=lambda x: x["created_at"], reverse=True)

    def update_realtime_task(self, task_id: str, **kwargs):
        """更新实时任务信息。"""
        if task_id in self._realtime_tasks:
            self._realtime_tasks[task_id].update(kwargs)
            self._realtime_tasks[task_id]["updated_at"] = datetime.now().isoformat()
            logger.debug("更新实时任务: %s, 字段: %s", task_id, list(kwargs.keys()))
            self._mark_dirty(immediate=True)

    def update_realtime_task_stats(self, task_id: str, stats: Dict[str, Any]):
        """更新实时任务统计信息（包含断点位置，立即持久化）。"""
        if task_id in self._realtime_tasks:
            self._realtime_tasks[task_id]["stats"].update({
                "total_events": stats.get("total_events", 0),
                "insert_events": stats.get("insert_events", 0),
                "update_events": stats.get("update_events", 0),
                "delete_events": stats.get("delete_events", 0),
                "synced_events": stats.get("synced_events", 0),
                "failed_events": stats.get("failed_events", 0),
            })
            if "current_binlog_file" in stats and stats["current_binlog_file"] is not None:
                self._realtime_tasks[task_id]["binlog_file"] = stats["current_binlog_file"]
            if "current_binlog_pos" in stats and stats["current_binlog_pos"] is not None:
                self._realtime_tasks[task_id]["binlog_pos"] = stats["current_binlog_pos"]
            if "current_scn" in stats and stats["current_scn"] is not None:
                self._realtime_tasks[task_id]["current_scn"] = stats["current_scn"]
            self._mark_dirty(immediate=True)

    def get_realtime_task_binlog_position(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取实时任务的断点位置（支持 MySQL binlog 和 Oracle SCN）。"""
        task = self.get_realtime_task(task_id)
        if not task:
            return None
        if task.get("current_scn") is not None and task["current_scn"] > 0:
            return {
                "current_scn": task["current_scn"],
            }
        binlog_file = task.get("binlog_file", "")
        binlog_pos = task.get("binlog_pos", 0)
        if binlog_file and binlog_pos > 0:
            return {
                "log_file": binlog_file,
                "log_pos": binlog_pos,
            }
        return None

    def delete_realtime_task(self, task_id: str) -> bool:
        """删除实时任务。"""
        if task_id in self._realtime_tasks:
            del self._realtime_tasks[task_id]
            logger.info("删除实时任务: %s", task_id)
            self._mark_dirty()
            return True
        return False

    # ------------------------------------------------------------------ #
    #                     智能同步（三阶段编排）任务管理
    # ------------------------------------------------------------------ #

    def create_smart_sync_task(self, task_id: str, config_name: str, username: str,
                               quick_days: int = 7) -> Dict:
        """
        创建智能同步任务（三阶段编排）。

        Args:
            task_id: 任务 ID
            config_name: 配置名称
            username: 创建者用户名
            quick_days: 快速增量阶段同步最近 N 天的数据

        Returns:
            任务信息字典
        """
        self._realtime_tasks[task_id] = {
            "task_id": task_id,
            "config_name": config_name,
            "username": username,
            "type": "smart_sync",
            "status": "pending",
            "current_phase": "pending",
            "quick_days": quick_days,
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "message": "智能同步任务已创建",
            "binlog_file": "",
            "binlog_pos": 0,
            "phase_progress": {
                "quick_incremental": {"status": "pending", "progress": 0, "message": ""},
                "full_backfill": {"status": "pending", "progress": 0, "message": ""},
                "realtime": {"status": "pending", "progress": 0, "message": ""},
            },
            "stats": {
                "total_events": 0,
                "insert_events": 0,
                "update_events": 0,
                "delete_events": 0,
                "synced_events": 0,
                "failed_events": 0,
            },
        }
        logger.info("创建智能同步任务: %s (配置: %s, 用户: %s, 快速同步天数: %d)",
                    task_id, config_name, username, quick_days)
        self._mark_dirty()
        return self._realtime_tasks[task_id]

    def update_smart_sync_phase(self, task_id: str, phase: str, status: str,
                                progress: float = 0, message: str = ""):
        """
        更新智能同步任务的阶段状态。

        Args:
            task_id: 任务 ID
            phase: 阶段名称 (quick_incremental/full_backfill/realtime)
            status: 状态 (pending/running/completed/failed)
            progress: 进度百分比 0-100
            message: 阶段消息
        """
        if task_id in self._realtime_tasks:
            task = self._realtime_tasks[task_id]
            task["phase_progress"][phase] = {
                "status": status,
                "progress": progress,
                "message": message,
                "updated_at": datetime.now().isoformat(),
            }
            task["current_phase"] = phase
            task["updated_at"] = datetime.now().isoformat()
            if status == "running":
                task["status"] = "running"
            logger.debug("更新智能同步任务阶段: %s, 阶段=%s, 状态=%s, 进度=%.1f%%",
                         task_id, phase, status, progress)
            self._mark_dirty()

    def get_smart_sync_task(self, task_id: str) -> Optional[Dict]:
        """获取智能同步任务（与实时任务共享存储）。"""
        task = self.get_realtime_task(task_id)
        if task and task.get("type") == "smart_sync":
            return task
        return None

    def list_smart_sync_tasks(self, username: Optional[str] = None) -> List[Dict]:
        """列出所有智能同步任务。"""
        tasks = self.list_realtime_tasks(username)
        return [t for t in tasks if t.get("type") == "smart_sync"]


db2_store = DB2MemoryStore()
