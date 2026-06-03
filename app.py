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


@app.route("/api/dashboard", methods=["GET"])
@login_required
def get_dashboard():
    """获取仪表盘数据。"""
    tasks = db2_store.list_tasks(request.username)
    progress_list = db2_store.list_progress()

    stats = {
        "total_tasks": len(tasks),
        "running_tasks": sum(1 for t in tasks if t["status"] == "running"),
        "completed_tasks": sum(1 for t in tasks if t["status"] == "completed"),
        "failed_tasks": sum(1 for t in tasks if t["status"] == "failed"),
        "total_tables_in_progress": len(progress_list),
        "completed_tables": sum(1 for p in progress_list if p["status"] == "done"),
        "total_rows": sum(p["total_rows"] for p in progress_list),
        "synced_rows": sum(p["synced_rows"] for p in progress_list),
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
