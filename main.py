import argparse
import yaml
import logging
from logger_utils import setup_logger
from sync_engine import run_sync, reset_progress


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="MySQL Cloud-to-Local Full Sync Tool")
    parser.add_argument("-c", "--config", default="config.yaml", help="Path to config YAML file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("--log-file", default=None, help="Write logs to file")

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--resume", action="store_true",
        help="Resume from previous progress (keep existing data, skip completed tables)",
    )
    mode_group.add_argument(
        "--restart", action="store_true",
        help="Restart from scratch (truncate target tables, reset all progress)",
    )

    parser.add_argument(
        "--reset-table", default=None,
        help="Reset progress for a specific table only (then exit, no sync)",
    )
    parser.add_argument(
        "--reset-all", action="store_true",
        help="Reset all progress (then exit, no sync)",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    setup_logger(level=level, log_file=args.log_file)

    config = load_config(args.config)

    if args.reset_all:
        reset_progress(config["target"])
        return

    if args.reset_table:
        reset_progress(config["target"], table_name=args.reset_table)
        return

    results = run_sync(config, resume_mode=args.resume, restart=args.restart)

    has_error = any(r["error"] for r in results)
    exit(1 if has_error else 0)


if __name__ == "__main__":
    main()
