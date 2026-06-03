import threading
import time
from datetime import datetime
from typing import Optional, Dict, List, Any


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
        self._init_default_data()

    def _init_default_data(self):
        self._users = {
            "admin": {
                "username": "admin",
                "password": "admin123",
                "created_at": datetime.now().isoformat(),
                "role": "admin"
            }
        }

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
        return self._configs[config_name]

    def get_config(self, config_name: str) -> Optional[Dict]:
        cfg = self._configs.get(config_name)
        return cfg["config"] if cfg else None

    def list_configs(self) -> List[Dict]:
        return [
            {k: v for k, v in cfg.items() if k != "config"}
            for cfg in self._configs.values()
        ]

    def delete_config(self, config_name: str) -> bool:
        if config_name in self._configs:
            del self._configs[config_name]
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


db2_store = DB2MemoryStore()
