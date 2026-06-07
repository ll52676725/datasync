"""
app.py — 多数据库同步系统 Web 服务

Flask 应用，提供 Web UI 和 REST API。
支持 MySQL 和 DB2 两种数据库类型的配置、测试和同步。

API 列表：
  - GET  /api/configs              — 列出所有配置
  - GET  /api/configs/<name>       — 获取指定配置
  - POST /api/configs              — 保存配置
  - DELETE /api/configs/<name>     — 删除配置
  - POST /api/configs/<name>/test  — 测试配置中的数据库连接
  - GET  /api/configs/<name>/tables — 获取源数据库表列表
  - GET  /api/tasks                — 列出任务
  - POST /api/tasks                — 创建任务
  - POST /api/tasks/<id>/start     — 启动任务
  - POST /api/tasks/<id>/stop      — 停止任务
  - GET  /api/tasks/<id>/progress  — 获取任务进度
  - POST /api/progress/reset      — 重置进度
  - GET  /api/dashboard            — 仪表盘数据
  - GET  /api/supported-types      — 获取支持的数据库类型
"""

import os
import uuid
import threading
import logging
from functools import wraps
from datetime import datetime

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory

from db_adapter import get_adapter, get_supported_types
from db2_memory import db2_store
from sync_engine_web import run_sync, reset_progress_db2
from realtime_sync_engine import (
    start_realtime_sync,
    stop_realtime_sync,
    get_realtime_stats,
    list_running_realtime_tasks,
    check_realtime_environment,
)
from sync_orchestrator import (
    smart_sync_manager,
    start_smart_sync,
    stop_smart_sync,
)
from logger_utils import setup_logger

setup_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = "sync-system-secret-key-2024"
app.config["SESSION_COOKIE_NAME"] = "sync_session"
app.config["TEMPLATES_AUTO_RELOAD"] = True


@app.after_request
def add_no_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

running_tasks = {}
task_stop_flags = {}


def login_required(f):
    """登录验证装饰器，检查 session 有效性。"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        session_id = session.get("session_id")
        if not session_id:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"success": False, "message": "未登录"}), 401
            return redirect(url_for("login"))
        username = db2_store.validate_session(session_id)
        if not username:
            session.pop("session_id", None)
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"success": False, "message": "登录已过期"}), 401
            return redirect(url_for("login"))
        request.username = username
        return f(*args, **kwargs)
    return decorated_function


@app.route("/")
@login_required
def index():
    """首页，渲染主界面。"""
    return render_template("index.html", username=request.username)


@app.route("/login", methods=["GET", "POST"])
def login():
    """登录页面和登录处理。"""
    if request.method == "GET":
        return render_template("login.html")

    data = request.get_json() if request.is_json else request.form
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"success": False, "message": "用户名和密码不能为空"}), 400

    if db2_store.verify_user(username, password):
        session_id = db2_store.create_session(username)
        session["session_id"] = session_id
        logger.info("用户登录: %s", username)
        return jsonify({"success": True, "message": "登录成功", "redirect": url_for("index")})

    return jsonify({"success": False, "message": "用户名或密码错误"}), 401


@app.route("/logout")
@login_required
def logout():
    """退出登录。"""
    session_id = session.get("session_id")
    if session_id:
        db2_store.delete_session(session_id)
    session.pop("session_id", None)
    return redirect(url_for("login"))


@app.route("/api/supported-types", methods=["GET"])
@login_required
def get_supported_db_types():
    """获取当前支持的数据库类型列表。"""
    logger.info("获取支持的数据库类型")
    return jsonify({"success": True, "types": get_supported_types()})


@app.route("/api/configs", methods=["GET"])
@login_required
def list_configs():
    """列出所有同步配置。"""
    configs = db2_store.list_configs()
    return jsonify({"success": True, "configs": configs})


@app.route("/api/configs/<name>", methods=["GET"])
@login_required
def get_config(name):
    """获取指定名称的配置。"""
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404
    return jsonify({"success": True, "config": config})


@app.route("/api/configs", methods=["POST"])
@login_required
def save_config():
    """
    保存同步配置。

    配置格式示例：
    {
        "name": "my_config",
        "config": {
            "source": {"type": "mysql", "host": "...", "port": 3306, ...},
            "target": {"type": "db2", "host": "...", "port": 50000, ...},
            "sync": {"parallel_tables": 4, ...}
        }
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    name = data.get("name", "").strip()
    config = data.get("config")

    if not name or not config:
        return jsonify({"success": False, "message": "配置名称和内容不能为空"}), 400

    required_sections = ["source", "target", "sync"]
    for section in required_sections:
        if section not in config:
            return jsonify({"success": False, "message": f"配置缺少 {section} 节"}), 400

    # 验证数据库连接字段（兼容无 type 字段的旧配置，默认 mysql）
    required_conn_fields = ["host", "port", "user", "password", "database"]
    for section in ["source", "target"]:
        db_type = config[section].get("type", "mysql").lower()
        for field in required_conn_fields:
            if field not in config[section]:
                return jsonify({"success": False, "message": f"{section} 缺少 {field} 字段"}), 400
        # 记录配置中的数据库类型
        logger.debug("配置 %s 的 %s 类型: %s", name, section, db_type)

    saved = db2_store.save_config(name, config, request.username)
    logger.info("配置已保存: %s (用户: %s, 源: %s, 目标: %s)",
                name, request.username,
                config["source"].get("type", "mysql"),
                config["target"].get("type", "mysql"))
    return jsonify({"success": True, "message": "配置保存成功", "config": saved})


@app.route("/api/configs/<name>", methods=["DELETE"])
@login_required
def delete_config(name):
    """删除指定配置。"""
    if db2_store.delete_config(name):
        logger.info("配置已删除: %s (用户: %s)", name, request.username)
        return jsonify({"success": True, "message": "配置删除成功"})
    return jsonify({"success": False, "message": "配置不存在"}), 404


@app.route("/api/configs/<name>/test", methods=["POST"])
@login_required
def test_config(name):
    """
    测试配置中的数据库连接。

    根据配置中的 type 字段选择对应的适配器进行连接测试，
    支持 mysql 和 db2 两种数据库类型。
    """
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    results = {}
    for section in ["source", "target"]:
        try:
            adapter = get_adapter(config[section])
            success, msg = adapter.test_connection()
            results[section] = {
                "success": success,
                "message": msg,
                "type": config[section].get("type", "mysql"),
            }
        except ValueError as e:
            results[section] = {"success": False, "message": str(e), "type": config[section].get("type", "mysql")}
        except Exception as e:
            results[section] = {
                "success": False,
                "message": str(e),
                "type": config[section].get("type", "mysql"),
            }

    all_success = all(r["success"] for r in results.values())
    logger.info("配置 %s 连接测试: %s", name,
                "全部成功" if all_success else "部分失败")
    return jsonify({
        "success": all_success,
        "message": "所有连接测试通过" if all_success else "部分连接测试失败",
        "results": results,
    })


@app.route("/api/configs/<name>/tables", methods=["GET"])
@login_required
def get_tables(name):
    """
    获取源数据库中的表列表。

    根据配置中的 type 字段选择对应的适配器查询表列表，
    支持 mysql 和 db2 两种数据库类型。
    """
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    try:
        adapter = get_adapter(config["source"])
        conn = adapter.create_connection()
        try:
            tables = adapter.list_tables(conn)
        finally:
            adapter.close_connection(conn)
        logger.info("获取源数据库表列表: %d 个表 (类型: %s)",
                     len(tables), adapter.db_type)
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        logger.error("获取表列表失败: %s", e)
        return jsonify({"success": False, "message": f"获取表列表失败: {str(e)}"}), 500


@app.route("/api/tasks", methods=["GET"])
@login_required
def list_tasks():
    """列出当前用户的所有任务。"""
    tasks = db2_store.list_tasks(request.username)
    return jsonify({"success": True, "tasks": tasks})


@app.route("/api/tasks", methods=["POST"])
@login_required
def create_task():
    """创建新的同步任务。"""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    config_name = data.get("config_name", "").strip()
    mode = data.get("mode", "resume")

    if not config_name:
        return jsonify({"success": False, "message": "请选择配置"}), 400

    config = db2_store.get_config(config_name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    task_id = str(uuid.uuid4())
    task = db2_store.create_task(task_id, config_name, request.username, mode)
    logger.info("任务已创建: %s (配置: %s, 模式: %s, 用户: %s)",
                task_id, config_name, mode, request.username)

    return jsonify({"success": True, "message": "任务创建成功", "task": task})


@app.route("/api/tasks/<task_id>/start", methods=["POST"])
@login_required
def start_task(task_id):
    """启动同步任务。"""
    task = db2_store.get_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    if task_id in running_tasks and running_tasks[task_id].is_alive():
        return jsonify({"success": False, "message": "任务已在运行中"}), 400

    config = db2_store.get_config(task["config_name"])
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    mode = task["mode"]
    resume_mode = mode == "resume"
    restart = mode == "restart"

    stop_flag = {"stop": False}
    task_stop_flags[task_id] = stop_flag

    def sync_worker():
        """同步工作线程，执行同步任务并处理异常。"""
        try:
            run_sync(config, resume_mode=resume_mode, restart=restart,
                     task_id=task_id, stop_flag=stop_flag)
        except Exception as e:
            logger.error("任务 %s 失败: %s", task_id, e)
            db2_store.update_task(task_id, status="failed", message=f"任务失败: {str(e)}")
        finally:
            if task_id in running_tasks:
                del running_tasks[task_id]
            if task_id in task_stop_flags:
                del task_stop_flags[task_id]

    thread = threading.Thread(target=sync_worker, daemon=True)
    running_tasks[task_id] = thread
    thread.start()

    logger.info("任务已启动: %s (用户: %s)", task_id, request.username)
    return jsonify({"success": True, "message": "任务已启动"})


@app.route("/api/tasks/<task_id>/stop", methods=["POST"])
@login_required
def stop_task(task_id):
    """停止运行中的同步任务。"""
    task = db2_store.get_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    if task_id in task_stop_flags:
        task_stop_flags[task_id]["stop"] = True

    db2_store.update_task(task_id, status="stopping", message="正在停止任务...")
    logger.info("任务停止中: %s (用户: %s)", task_id, request.username)
    return jsonify({"success": True, "message": "正在停止任务..."})


@app.route("/api/tasks/<task_id>/progress", methods=["GET"])
@login_required
def get_task_progress(task_id):
    """获取任务的同步进度。"""
    task = db2_store.get_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权查看此任务"}), 403

    progress = db2_store.get_task_progress(task_id)
    return jsonify({"success": True, "progress": progress})


@app.route("/api/progress/reset", methods=["POST"])
@login_required
def reset_progress():
    """重置同步进度。"""
    data = request.get_json() or {}
    table_name = data.get("table_name")

    reset_progress_db2(table_name)
    logger.info("进度重置%s (用户: %s)",
                f" 表: {table_name}" if table_name else "", request.username)
    return jsonify({"success": True, "message": "进度已重置"})


# ====================================================================== #
#                        实时同步 API
# ====================================================================== #

@app.route("/api/realtime/tasks", methods=["GET"])
@login_required
def list_realtime_tasks():
    """列出当前用户的所有实时同步任务。"""
    tasks = db2_store.list_realtime_tasks(request.username)
    return jsonify({"success": True, "tasks": tasks})


@app.route("/api/realtime/tasks", methods=["POST"])
@login_required
def create_realtime_task():
    """
    创建新的实时同步任务。

    请求体:
    {
        "config_name": "配置名称",
        "resume_from_binlog": true/false  # 是否从断点恢复
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    config_name = data.get("config_name", "").strip()
    if not config_name:
        return jsonify({"success": False, "message": "请选择配置"}), 400

    config = db2_store.get_config(config_name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    src_type = config["source"].get("type", "mysql").lower()
    src_adapter = get_adapter(config["source"])
    if not src_adapter.supports_cdc():
        return jsonify({
            "success": False,
            "message": f"源数据库类型 '{src_type}' 不支持实时同步（仅 MySQL 支持）",
        }), 400

    task_id = str(uuid.uuid4())
    task = db2_store.create_realtime_task(task_id, config_name, request.username)
    logger.info("实时任务已创建: %s (配置: %s, 用户: %s)", task_id, config_name, request.username)

    return jsonify({"success": True, "message": "实时任务创建成功", "task": task})


@app.route("/api/realtime/tasks/<task_id>/start", methods=["POST"])
@login_required
def start_realtime_task(task_id):
    """
    启动实时同步任务。

    请求体可选参数:
    {
        "resume_from_binlog": false,  # 是否从断点恢复
        "skip_env_check": false       # 是否跳过环境检测（不推荐）
    }
    """
    task = db2_store.get_realtime_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    config = db2_store.get_config(task["config_name"])
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    data = request.get_json() or {}
    resume_from_binlog = data.get("resume_from_binlog", False)
    skip_env_check = data.get("skip_env_check", False)

    db2_store.update_realtime_task(task_id, status="starting", message="正在启动实时同步...")

    try:
        success = start_realtime_sync(
            config, task_id,
            resume_from_binlog=resume_from_binlog,
            skip_env_check=skip_env_check,
        )
    except RuntimeError as e:
        db2_store.update_realtime_task(task_id, status="failed", message=str(e))
        return jsonify({"success": False, "message": str(e)}), 400

    if success:
        db2_store.update_realtime_task(
            task_id,
            status="running",
            started_at=datetime.now().isoformat(),
            message="实时同步已启动",
        )
        logger.info("实时任务已启动: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "实时同步已启动"})
    else:
        db2_store.update_realtime_task(task_id, status="failed", message="启动失败")
        return jsonify({"success": False, "message": "实时同步启动失败"}), 500


@app.route("/api/realtime/tasks/<task_id>/stop", methods=["POST"])
@login_required
def stop_realtime_task(task_id):
    """停止实时同步任务。"""
    task = db2_store.get_realtime_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    success = stop_realtime_sync(task_id)
    if success:
        db2_store.update_realtime_task(
            task_id,
            status="stopped",
            stopped_at=datetime.now().isoformat(),
            message="实时同步已停止",
        )
        logger.info("实时任务已停止: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "实时同步已停止"})
    else:
        return jsonify({"success": False, "message": "停止失败"}), 500


@app.route("/api/realtime/tasks/<task_id>/stats", methods=["GET"])
@login_required
def get_realtime_task_stats(task_id):
    """获取实时同步任务的统计信息。"""
    task = db2_store.get_realtime_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权查看此任务"}), 403

    runtime_stats = get_realtime_stats(task_id) or {}
    stored_stats = task.get("stats", {})

    stats = {
        **stored_stats,
        **runtime_stats,
        "task_id": task_id,
        "status": task.get("status"),
        "message": task.get("message"),
        "binlog_file": task.get("binlog_file"),
        "binlog_pos": task.get("binlog_pos"),
    }

    return jsonify({"success": True, "stats": stats})


@app.route("/api/realtime/tasks/<task_id>", methods=["DELETE"])
@login_required
def delete_realtime_task(task_id):
    """删除实时同步任务。"""
    task = db2_store.get_realtime_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    stop_realtime_sync(task_id)

    if db2_store.delete_realtime_task(task_id):
        logger.info("实时任务已删除: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "实时任务已删除"})
    return jsonify({"success": False, "message": "删除失败"}), 500


@app.route("/api/realtime/check-env/<config_name>", methods=["GET"])
@login_required
def check_realtime_env(config_name):
    """
    检查指定配置的实时同步（CDC）环境。

    返回 CDC 环境检测结果，包含各项检测详情和配置引导建议。
    """
    config = db2_store.get_config(config_name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    result = check_realtime_environment(config)
    logger.info(
        "CDC 环境检测: %s, 结果: %s",
        config_name, "通过" if result["passed"] else "未通过",
    )

    return jsonify({
        "success": True,
        "result": result,
    })


@app.route("/api/realtime/dashboard", methods=["GET"])
@login_required
def get_realtime_dashboard():
    """获取实时同步仪表盘数据。"""
    tasks = db2_store.list_realtime_tasks(request.username)
    running_count = sum(1 for t in tasks if t.get("status") == "running")
    total_events = sum(t.get("stats", {}).get("total_events", 0) for t in tasks)
    synced_events = sum(t.get("stats", {}).get("synced_events", 0) for t in tasks)

    stats = {
        "total_tasks": len(tasks),
        "running_tasks": running_count,
        "stopped_tasks": len(tasks) - running_count,
        "total_events": total_events,
        "synced_events": synced_events,
        "failed_events": total_events - synced_events,
    }

    return jsonify({
        "success": True,
        "stats": stats,
        "recent_tasks": tasks[:5],
    })


# ====================================================================== #
#                        智能同步（三阶段编排）API
# ====================================================================== #

@app.route("/api/smart-sync/tasks", methods=["GET"])
@login_required
def list_smart_sync_tasks():
    """列出当前用户的所有智能同步任务。"""
    tasks = db2_store.list_smart_sync_tasks(request.username)
    return jsonify({"success": True, "tasks": tasks})


@app.route("/api/smart-sync/tasks", methods=["POST"])
@login_required
def create_smart_sync_task():
    """
    创建新的智能同步任务（三阶段编排）。

    请求体:
    {
        "config_name": "配置名称",
        "quick_days": 7  # 可选，快速增量同步天数，默认 7
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    config_name = data.get("config_name", "").strip()
    quick_days = int(data.get("quick_days", 7))

    if not config_name:
        return jsonify({"success": False, "message": "请选择配置"}), 400

    config = db2_store.get_config(config_name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    src_type = config["source"].get("type", "mysql").lower()
    src_adapter = get_adapter(config["source"])
    if not src_adapter.supports_cdc():
        logger.warning(
            "源数据库类型 '%s' 不支持实时同步，智能同步将仅执行前两阶段",
            src_type,
        )

    task_id = str(uuid.uuid4())
    task = db2_store.create_smart_sync_task(
        task_id, config_name, request.username, quick_days=quick_days,
    )
    logger.info(
        "智能同步任务已创建: %s (配置: %s, 快速天数: %d, 用户: %s)",
        task_id, config_name, quick_days, request.username,
    )

    return jsonify({"success": True, "message": "智能同步任务创建成功", "task": task})


@app.route("/api/smart-sync/check-env/<config_name>", methods=["GET"])
@login_required
def check_smart_sync_env(config_name):
    """
    检查指定配置的智能同步环境。

    返回完整的环境检测结果，包括数据库连接和 CDC 环境。
    """
    config = db2_store.get_config(config_name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    from sync_orchestrator import SmartSyncOrchestrator
    orchestrator = SmartSyncOrchestrator(config, "check_env_task", quick_days=7)
    result = orchestrator.check_environment()

    logger.info(
        "智能同步环境检测: %s, 结果: %s",
        config_name, "通过" if result["passed"] else "未通过",
    )

    return jsonify({
        "success": True,
        "result": result,
    })


@app.route("/api/smart-sync/tasks/<task_id>/start", methods=["POST"])
@login_required
def start_smart_sync_task(task_id):
    """
    启动智能同步任务。

    请求体可选参数:
    {
        "skip_env_check": false  # 是否跳过环境检测（不推荐）
    }
    """
    task = db2_store.get_smart_sync_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    running_tasks = smart_sync_manager.list_running_tasks()
    if task_id in running_tasks:
        return jsonify({"success": False, "message": "任务已在运行中"}), 400

    config = db2_store.get_config(task["config_name"])
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404

    data = request.get_json() or {}
    skip_env_check = data.get("skip_env_check", False)

    quick_days = task.get("quick_days", 7)
    db2_store.update_realtime_task(
        task_id, status="starting", message="正在启动智能同步...",
    )

    success = smart_sync_manager.start_smart_sync(
        task_id, config,
        quick_days=quick_days,
        skip_env_check=skip_env_check,
    )
    if success:
        logger.info("智能同步任务已启动: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "智能同步已启动"})
    else:
        db2_store.update_realtime_task(task_id, status="failed", message="启动失败")
        return jsonify({"success": False, "message": "智能同步启动失败"}), 500


@app.route("/api/smart-sync/tasks/<task_id>/stop", methods=["POST"])
@login_required
def stop_smart_sync_task(task_id):
    """停止智能同步任务。"""
    task = db2_store.get_smart_sync_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    success = stop_smart_sync(task_id)
    if success:
        db2_store.update_realtime_task(
            task_id,
            status="stopped",
            stopped_at=datetime.now().isoformat(),
            message="智能同步已停止",
        )
        logger.info("智能同步任务已停止: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "智能同步已停止"})
    else:
        return jsonify({"success": False, "message": "停止失败"}), 500


@app.route("/api/smart-sync/tasks/<task_id>/stats", methods=["GET"])
@login_required
def get_smart_sync_task_stats(task_id):
    """获取智能同步任务的统计信息和阶段进度。"""
    task = db2_store.get_smart_sync_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权查看此任务"}), 403

    runtime_stats = get_realtime_stats(task_id) or {}
    stored_stats = task.get("stats", {})

    phase_progress = task.get("phase_progress", {})
    current_phase = task.get("current_phase", "pending")

    phase_names = {
        "quick_incremental": "阶段一：快速增量",
        "full_backfill": "阶段二：存量补全",
        "realtime": "阶段三：实时同步",
    }

    stats = {
        **stored_stats,
        **runtime_stats,
        "task_id": task_id,
        "status": task.get("status"),
        "message": task.get("message"),
        "current_phase": current_phase,
        "current_phase_name": phase_names.get(current_phase, current_phase),
        "quick_days": task.get("quick_days", 7),
        "binlog_file": task.get("binlog_file"),
        "binlog_pos": task.get("binlog_pos"),
        "phase_progress": phase_progress,
        "phase_names": phase_names,
    }

    return jsonify({"success": True, "stats": stats})


@app.route("/api/smart-sync/tasks/<task_id>", methods=["DELETE"])
@login_required
def delete_smart_sync_task(task_id):
    """删除智能同步任务。"""
    task = db2_store.get_smart_sync_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404

    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403

    stop_smart_sync(task_id)

    if db2_store.delete_realtime_task(task_id):
        logger.info("智能同步任务已删除: %s (用户: %s)", task_id, request.username)
        return jsonify({"success": True, "message": "智能同步任务已删除"})
    return jsonify({"success": False, "message": "删除失败"}), 500


@app.route("/api/smart-sync/dashboard", methods=["GET"])
@login_required
def get_smart_sync_dashboard():
    """获取智能同步仪表盘数据。"""
    tasks = db2_store.list_smart_sync_tasks(request.username)
    running_count = sum(1 for t in tasks if t.get("status") == "running")
    completed_count = sum(1 for t in tasks if t.get("status") == "completed")

    phase_counts = {
        "quick_incremental": 0,
        "full_backfill": 0,
        "realtime": 0,
    }
    for t in tasks:
        phase = t.get("current_phase")
        if phase in phase_counts:
            phase_counts[phase] += 1

    stats = {
        "total_tasks": len(tasks),
        "running_tasks": running_count,
        "completed_tasks": completed_count,
        "failed_tasks": sum(1 for t in tasks if t.get("status") == "failed"),
        "phase_counts": phase_counts,
    }

    return jsonify({
        "success": True,
        "stats": stats,
        "recent_tasks": tasks[:5],
    })


# ====================================================================== #
#                           资源管理 API
# ====================================================================== #

@app.route("/api/resources", methods=["GET"])
@login_required
def list_resources():
    """列出所有数据库资源。"""
    resources = db2_store.list_resources(request.username)
    return jsonify({"success": True, "resources": resources})

@app.route("/api/resources/<resource_id>", methods=["GET"])
@login_required
def get_resource(resource_id):
    """获取指定资源详情。"""
    resource = db2_store.get_resource(resource_id)
    if not resource:
        return jsonify({"success": False, "message": "资源不存在"}), 404
    return jsonify({"success": True, "resource": resource})

@app.route("/api/resources", methods=["POST"])
@login_required
def create_resource():
    """创建新的数据库资源。"""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    resource_id = str(uuid.uuid4())
    resource = db2_store.save_resource(resource_id, data, request.username)
    logger.info("资源已创建: %s (类型: %s, 用户: %s)", resource_id, data.get("type"), request.username)
    return jsonify({"success": True, "message": "资源创建成功", "resource": resource})

@app.route("/api/resources/<resource_id>", methods=["PUT"])
@login_required
def update_resource(resource_id):
    """更新数据库资源。"""
    resource = db2_store.get_resource(resource_id)
    if not resource:
        return jsonify({"success": False, "message": "资源不存在"}), 404

    data = request.get_json() or {}
    updated = db2_store.save_resource(resource_id, {**resource, **data}, request.username)
    logger.info("资源已更新: %s (用户: %s)", resource_id, request.username)
    return jsonify({"success": True, "message": "资源更新成功", "resource": updated})

@app.route("/api/resources/<resource_id>", methods=["DELETE"])
@login_required
def delete_resource(resource_id):
    """删除数据库资源。"""
    if db2_store.delete_resource(resource_id):
        logger.info("资源已删除: %s (用户: %s)", resource_id, request.username)
        return jsonify({"success": True, "message": "资源删除成功"})
    return jsonify({"success": False, "message": "资源不存在"}), 404

@app.route("/api/resources/<resource_id>/test", methods=["POST"])
@login_required
def test_resource(resource_id):
    """测试资源连接。"""
    resource = db2_store.get_resource(resource_id)
    if not resource:
        return jsonify({"success": False, "message": "资源不存在"}), 404

    try:
        adapter = get_adapter(resource)
        success, msg = adapter.test_connection()
        return jsonify({
            "success": success,
            "message": msg,
            "type": resource.get("type", "mysql"),
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route("/api/resources/<resource_id>/tables", methods=["GET"])
@login_required
def get_resource_tables(resource_id):
    """获取资源数据库中的表列表。"""
    resource = db2_store.get_resource(resource_id)
    if not resource:
        return jsonify({"success": False, "message": "资源不存在"}), 404

    try:
        adapter = get_adapter(resource)
        conn = adapter.create_connection()
        try:
            tables = adapter.list_tables(conn)
        finally:
            adapter.close_connection(conn)
        logger.info("获取资源表列表: %s, %d 个表", resource_id, len(tables))
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        logger.error("获取表列表失败: %s", e)
        return jsonify({"success": False, "message": f"获取表列表失败: {str(e)}"}), 500

# ====================================================================== #
#                           流水线管理 API
# ====================================================================== #

@app.route("/api/pipelines", methods=["GET"])
@login_required
def list_pipelines():
    """列出所有同步流水线。"""
    pipelines = db2_store.list_pipelines(request.username)
    return jsonify({"success": True, "pipelines": pipelines})

@app.route("/api/pipelines/<pipeline_id>", methods=["GET"])
@login_required
def get_pipeline(pipeline_id):
    """获取指定流水线详情。"""
    pipeline = db2_store.get_pipeline(pipeline_id)
    if not pipeline:
        return jsonify({"success": False, "message": "流水线不存在"}), 404
    return jsonify({"success": True, "pipeline": pipeline})

@app.route("/api/pipelines", methods=["POST"])
@login_required
def create_pipeline():
    """创建新的同步流水线。"""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "请求数据为空"}), 400

    pipeline_id = str(uuid.uuid4())
    pipeline = db2_store.save_pipeline(pipeline_id, data, request.username)
    logger.info("流水线已创建: %s (用户: %s)", pipeline_id, request.username)
    return jsonify({"success": True, "message": "流水线创建成功", "pipeline": pipeline})

@app.route("/api/pipelines/<pipeline_id>", methods=["PUT"])
@login_required
def update_pipeline(pipeline_id):
    """更新同步流水线。"""
    pipeline = db2_store.get_pipeline(pipeline_id)
    if not pipeline:
        return jsonify({"success": False, "message": "流水线不存在"}), 404

    data = request.get_json() or {}
    updated = db2_store.save_pipeline(pipeline_id, {**pipeline, **data}, request.username)
    logger.info("流水线已更新: %s (用户: %s)", pipeline_id, request.username)
    return jsonify({"success": True, "message": "流水线更新成功", "pipeline": updated})

@app.route("/api/pipelines/<pipeline_id>", methods=["DELETE"])
@login_required
def delete_pipeline(pipeline_id):
    """删除同步流水线。"""
    if db2_store.delete_pipeline(pipeline_id):
        logger.info("流水线已删除: %s (用户: %s)", pipeline_id, request.username)
        return jsonify({"success": True, "message": "流水线删除成功"})
    return jsonify({"success": False, "message": "流水线不存在"}), 404

@app.route("/api/pipelines/<pipeline_id>/start", methods=["POST"])
@login_required
def start_pipeline(pipeline_id):
    """从流水线启动同步任务。"""
    pipeline = db2_store.get_pipeline(pipeline_id)
    if not pipeline:
        return jsonify({"success": False, "message": "流水线不存在"}), 404

    config = db2_store.convert_pipeline_to_config(pipeline_id)
    if not config:
        return jsonify({"success": False, "message": "流水线配置不完整，无法启动"}), 400

    config_name = f"pipeline_{pipeline['name']}_{pipeline_id[:8]}"
    db2_store.save_config(config_name, config, request.username)

    data = request.get_json() or {}
    mode = data.get("mode", "resume")

    task_id = str(uuid.uuid4())
    task = db2_store.create_task(task_id, config_name, request.username, mode)
    db2_store.update_pipeline_status(pipeline_id, "running", "同步任务已启动")

    stop_flag = {"stop": False}
    task_stop_flags[task_id] = stop_flag

    def sync_worker():
        try:
            run_sync(config, resume_mode=(mode == "resume"), restart=(mode == "restart"),
                     task_id=task_id, stop_flag=stop_flag)
            db2_store.update_pipeline_status(pipeline_id, "completed", "同步完成")
        except Exception as e:
            logger.error("流水线任务 %s 失败: %s", task_id, e)
            db2_store.update_task(task_id, status="failed", message=f"任务失败: {str(e)}")
            db2_store.update_pipeline_status(pipeline_id, "failed", f"同步失败: {str(e)}")
        finally:
            if task_id in running_tasks:
                del running_tasks[task_id]
            if task_id in task_stop_flags:
                del task_stop_flags[task_id]

    thread = threading.Thread(target=sync_worker, daemon=True)
    running_tasks[task_id] = thread
    thread.start()

    logger.info("流水线任务已启动: %s (流水线: %s, 用户: %s)", task_id, pipeline_id, request.username)
    return jsonify({"success": True, "message": "同步任务已启动", "task_id": task_id})

# ====================================================================== #
#                        更新仪表盘 API 支持新数据模型
# ====================================================================== #

@app.route("/api/dashboard", methods=["GET"])
@login_required
def get_dashboard():
    """获取仪表盘数据。"""
    tasks = db2_store.list_tasks(request.username)
    progress_list = db2_store.list_progress()
    resources = db2_store.list_resources(request.username)
    pipelines = db2_store.list_pipelines(request.username)

    stats = {
        "total_tasks": len(tasks),
        "running_tasks": sum(1 for t in tasks if t["status"] == "running"),
        "completed_tasks": sum(1 for t in tasks if t["status"] == "completed"),
        "failed_tasks": sum(1 for t in tasks if t["status"] == "failed"),
        "total_tables_in_progress": len(progress_list),
        "completed_tables": sum(1 for p in progress_list if p["status"] == "done"),
        "total_rows": sum(p["total_rows"] for p in progress_list),
        "synced_rows": sum(p["synced_rows"] for p in progress_list),
        "total_resources": len(resources),
        "total_pipelines": len(pipelines),
    }

    recent_tasks = tasks[:5]
    active_progress = [p for p in progress_list if p["status"] in ["running", "failed"]][:10]

    return jsonify({
        "success": True,
        "stats": stats,
        "recent_tasks": recent_tasks,
        "active_progress": active_progress,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info("启动同步系统 Web 服务，端口: %d", port)
    logger.info("默认登录: admin / admin123")
    logger.info("支持的数据库类型: %s", ", ".join(get_supported_types()))
    app.run(host="0.0.0.0", port=port, debug=False)
