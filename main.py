"""
main.py — 多数据库同步命令行工具

基于适配器模式，支持 mysql→mysql, db2→db2, mysql→db2, db2→mysql, mysql→oracle, oracle→mysql 等组合。

用法示例：
  # MySQL→MySQL 同步（默认，自动检测使用优化版）
  python main.py -c config.yaml

  # 使用优化版配置（推荐用于大事务/大表场景）
  python main.py -c config_optimized.yaml --optimized

  # 强制使用原版引擎（兼容旧版本）
  python main.py -c config.yaml --legacy

  # DB2→DB2 同步
  python main.py -c config_db2.yaml --resume

  # MySQL→DB2 同步
  python main.py -c config_mysql_to_db2.yaml

  # 重置进度
  python main.py --reset-all

  # 详细日志
  python main.py -v --log-file sync.log
"""

import argparse
import yaml
import logging
from logger_utils import setup_logger
from sync_engine import run_sync, reset_progress


def load_config(path):
    """
    加载 YAML 配置文件。

    如果 source/target 节没有 type 字段，默认为 "mysql"（兼容旧配置）。

    Args:
        path: 配置文件路径

    Returns:
        配置字典
    """
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for section in ["source", "target"]:
        if section in config and "type" not in config[section]:
            config[section]["type"] = "mysql"

    return config


def main():
    parser = argparse.ArgumentParser(
        description="多数据库同步工具 (支持 MySQL/DB2/Oracle)"
    )
    parser.add_argument("-c", "--config", default="config.yaml", help="配置文件路径 (默认: config.yaml)")
    parser.add_argument("-v", "--verbose", action="store_true", help="启用 DEBUG 级别日志")
    parser.add_argument("--log-file", default=None, help="日志输出到文件")

    engine_group = parser.add_mutually_exclusive_group()
    engine_group.add_argument(
        "--optimized", action="store_true",
        help="使用优化版引擎（支持大事务、智能分页等复杂场景）",
    )
    engine_group.add_argument(
        "--legacy", action="store_true",
        help="强制使用原版引擎（兼容旧版本）",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--resume", action="store_true",
        help="断点续传模式（保留已有数据，跳过已完成的表）",
    )
    mode_group.add_argument(
        "--restart", action="store_true",
        help="重新开始模式（清空目标表，重置所有进度）",
    )

    parser.add_argument(
        "--reset-table", default=None,
        help="重置指定表的进度（然后退出，不执行同步）",
    )
    parser.add_argument(
        "--reset-all", action="store_true",
        help="重置所有进度（然后退出，不执行同步）",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(level=level, log_file=args.log_file)

    config = load_config(args.config)

    src_type = config.get("source", {}).get("type", "mysql")
    tgt_type = config.get("target", {}).get("type", "mysql")
    logging.getLogger(__name__).info("同步方向: %s → %s", src_type, tgt_type)

    if args.reset_all:
        reset_progress(config["target"])
        return

    if args.reset_table:
        reset_progress(config["target"], table_name=args.reset_table)
        return

    sync_cfg = config.get("sync", {})
    use_optimized = args.optimized or sync_cfg.get("use_optimized_engine", False)
    if args.legacy:
        use_optimized = False

    if use_optimized:
        try:
            from sync_engine_optimized import run_sync_optimized
            logging.getLogger(__name__).info("使用优化版同步引擎")
            results = run_sync_optimized(
                config,
                resume_mode=args.resume,
                restart=args.restart,
                use_optimized=True,
            )
        except ImportError as e:
            logging.getLogger(__name__).warning(
                "优化版引擎加载失败 (%s)，回退到原版引擎", e
            )
            results = run_sync(config, resume_mode=args.resume, restart=args.restart)
    else:
        results = run_sync(config, resume_mode=args.resume, restart=args.restart)

    has_error = any(r["error"] for r in results)
    exit(1 if has_error else 0)


if __name__ == "__main__":
    main()
