import os
import uuid
import threading
import logging
from functools import wraps
from datetime import datetime

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory

from db2_memory import db2_store
from sync_engine_web import run_sync, reset_progress_db2
from logger_utils import setup_logger

setup_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = "sync-system-secret-key-2024"
app.config["SESSION_COOKIE_NAME"] = "sync_session"

running_tasks = {}
task_stop_flags = {}


def login_required(f):
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
    return render_template("index.html", username=request.username)


@app.route("/login", methods=["GET", "POST"])
def login():
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
        logger.info("User logged in: %s", username)
        return jsonify({"success": True, "message": "登录成功", "redirect": url_for("index")})
    
    return jsonify({"success": False, "message": "用户名或密码错误"}), 401


@app.route("/logout")
@login_required
def logout():
    session_id = session.get("session_id")
    if session_id:
        db2_store.delete_session(session_id)
    session.pop("session_id", None)
    return redirect(url_for("login"))


@app.route("/api/configs", methods=["GET"])
@login_required
def list_configs():
    configs = db2_store.list_configs()
    return jsonify({"success": True, "configs": configs})


@app.route("/api/configs/<name>", methods=["GET"])
@login_required
def get_config(name):
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404
    return jsonify({"success": True, "config": config})


@app.route("/api/configs", methods=["POST"])
@login_required
def save_config():
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
    
    required_conn_fields = ["host", "port", "user", "password", "database"]
    for section in ["source", "target"]:
        for field in required_conn_fields:
            if field not in config[section]:
                return jsonify({"success": False, "message": f"{section} 缺少 {field} 字段"}), 400
    
    saved = db2_store.save_config(name, config, request.username)
    logger.info("Config saved: %s by %s", name, request.username)
    return jsonify({"success": True, "message": "配置保存成功", "config": saved})


@app.route("/api/configs/<name>", methods=["DELETE"])
@login_required
def delete_config(name):
    if db2_store.delete_config(name):
        logger.info("Config deleted: %s by %s", name, request.username)
        return jsonify({"success": True, "message": "配置删除成功"})
    return jsonify({"success": False, "message": "配置不存在"}), 404


@app.route("/api/configs/<name>/test", methods=["POST"])
@login_required
def test_config(name):
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404
    
    results = {}
    import pymysql
    
    for section in ["source", "target"]:
        try:
            conn = pymysql.connect(
                host=config[section]["host"],
                port=config[section]["port"],
                user=config[section]["user"],
                password=config[section]["password"],
                database=config[section]["database"],
                connect_timeout=5
            )
            conn.close()
            results[section] = {"success": True, "message": "连接成功"}
        except Exception as e:
            results[section] = {"success": False, "message": str(e)}
    
    all_success = all(r["success"] for r in results.values())
    return jsonify({
        "success": all_success,
        "message": "所有连接测试通过" if all_success else "部分连接测试失败",
        "results": results
    })


@app.route("/api/configs/<name>/tables", methods=["GET"])
@login_required
def get_tables(name):
    config = db2_store.get_config(name)
    if not config:
        return jsonify({"success": False, "message": "配置不存在"}), 404
    
    import pymysql
    try:
        conn = pymysql.connect(
            host=config["source"]["host"],
            port=config["source"]["port"],
            user=config["source"]["user"],
            password=config["source"]["password"],
            database=config["source"]["database"],
            connect_timeout=5
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME",
                (config["source"]["database"],)
            )
            tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        return jsonify({"success": False, "message": f"获取表列表失败: {str(e)}"}), 500


@app.route("/api/tasks", methods=["GET"])
@login_required
def list_tasks():
    tasks = db2_store.list_tasks(request.username)
    return jsonify({"success": True, "tasks": tasks})


@app.route("/api/tasks", methods=["POST"])
@login_required
def create_task():
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
    logger.info("Task created: %s (config=%s, mode=%s) by %s", task_id, config_name, mode, request.username)
    
    return jsonify({"success": True, "message": "任务创建成功", "task": task})


@app.route("/api/tasks/<task_id>/start", methods=["POST"])
@login_required
def start_task(task_id):
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
        try:
            run_sync(config, resume_mode=resume_mode, restart=restart, task_id=task_id, stop_flag=stop_flag)
        except Exception as e:
            logger.error("Task %s failed: %s", task_id, e)
            db2_store.update_task(task_id, status="failed", message=f"任务失败: {str(e)}")
        finally:
            if task_id in running_tasks:
                del running_tasks[task_id]
            if task_id in task_stop_flags:
                del task_stop_flags[task_id]
    
    thread = threading.Thread(target=sync_worker, daemon=True)
    running_tasks[task_id] = thread
    thread.start()
    
    logger.info("Task started: %s by %s", task_id, request.username)
    return jsonify({"success": True, "message": "任务已启动"})


@app.route("/api/tasks/<task_id>/stop", methods=["POST"])
@login_required
def stop_task(task_id):
    task = db2_store.get_task(task_id)
    if not task:
        return jsonify({"success": False, "message": "任务不存在"}), 404
    
    if task["username"] != request.username:
        return jsonify({"success": False, "message": "无权操作此任务"}), 403
    
    if task_id in task_stop_flags:
        task_stop_flags[task_id]["stop"] = True
    
    db2_store.update_task(task_id, status="stopping", message="正在停止任务...")
    logger.info("Task stopping: %s by %s", task_id, request.username)
    return jsonify({"success": True, "message": "正在停止任务..."})


@app.route("/api/tasks/<task_id>/progress", methods=["GET"])
@login_required
def get_task_progress(task_id):
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
    data = request.get_json() or {}
    table_name = data.get("table_name")
    
    reset_progress_db2(table_name)
    logger.info("Progress reset%s by %s", f" for table {table_name}" if table_name else "", request.username)
    return jsonify({"success": True, "message": "进度已重置"})


@app.route("/api/dashboard", methods=["GET"])
@login_required
def get_dashboard():
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
        "synced_rows": sum(p["synced_rows"] for p in progress_list)
    }
    
    recent_tasks = tasks[:5]
    active_progress = [p for p in progress_list if p["status"] in ["running", "failed"]][:10]
    
    return jsonify({
        "success": True,
        "stats": stats,
        "recent_tasks": recent_tasks,
        "active_progress": active_progress
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info("Starting Sync Web UI server on port %d", port)
    logger.info("Default login: admin / admin123")
    app.run(host="0.0.0.0", port=port, debug=False)
