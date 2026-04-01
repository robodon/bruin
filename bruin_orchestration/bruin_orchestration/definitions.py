import logging
import glob
import os
import re
import subprocess
import yaml
from datetime import datetime
from typing import Any

from dagster import (
    Definitions,
    In,
    Nothing,
    OpExecutionContext,
    RetryPolicy,
    Backoff,
    job,
    op,
    schedule,
    multiprocess_executor,
    failure_hook,
    success_hook,
    HookContext,
)

# ============================================================
# CONFIGURATION
# ============================================================

BRUIN_PROJECT = "/Users/robin/Desktop/bruin_test/bruin/etl_test1"
BRUIN_ENV = os.getenv("BRUIN_ENV", "default")
BRUIN_PATH = os.getenv("BRUIN_PATH", "/Users/robin/.local/bin/bruin")
MAX_CONCURRENT = int(os.getenv("DAGSTER_MAX_CONCURRENT", "1"))
MAX_RETRIES = int(os.getenv("DAGSTER_MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("DAGSTER_RETRY_DELAY", "30"))

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("bruin_dagster")


# ============================================================
# HOOKS - success/failure callbacks
# ============================================================

@failure_hook
def alert_on_failure(context: HookContext):
    logger.error(
        f"❌ Op '{context.op.name}' failed in job '{context.job_name}' "
        f"at {datetime.utcnow().isoformat()}Z"
    )
    # 👉 Add Slack/email/PagerDuty notification here if needed


@success_hook
def log_on_success(context: HookContext):
    logger.info(
        f"✅ Op '{context.op.name}' succeeded in job '{context.job_name}'"
    )


# ============================================================
# ASSET DISCOVERY
# ============================================================

def parse_dependencies(path: str) -> tuple[str | None, list[str]]:
    """Extract asset name and depends list from a Bruin asset file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        logger.warning(f"Could not read file {path}: {e}")
        return None, []

    name = None
    depends = []

    try:
        if path.endswith(".sql"):
            match = re.search(r'/\*\s*@bruin(.*?)@bruin\s*\*/', content, re.DOTALL)
            if match:
                parsed = yaml.safe_load(match.group(1))
                if parsed:
                    name = parsed.get("name")
                    depends = parsed.get("depends", []) or []

        elif path.endswith(".py"):
            match = re.search(r'"""@bruin(.*?)@bruin"""', content, re.DOTALL)
            if match:
                parsed = yaml.safe_load(match.group(1))
                if parsed:
                    name = parsed.get("name")
                    depends = parsed.get("depends", []) or []

        elif path.endswith((".yml", ".yaml")):
            parsed = yaml.safe_load(content)
            if parsed:
                name = parsed.get("name")
                depends = parsed.get("depends", []) or []

    except yaml.YAMLError as e:
        logger.warning(f"Failed to parse YAML in {path}: {e}")

    return name, depends


def discover_bruin_assets() -> dict[str, dict[str, Any]]:
    """Scan the project and return {asset_name: {path, depends}}."""
    patterns = [
        f"{BRUIN_PROJECT}/assets/**/*.py",
        f"{BRUIN_PROJECT}/assets/**/*.yml",
        f"{BRUIN_PROJECT}/assets/**/*.yaml",
        f"{BRUIN_PROJECT}/assets/**/*.sql",
    ]
    assets = {}
    for pattern in patterns:
        for path in glob.glob(pattern, recursive=True):
            name, depends = parse_dependencies(path)
            if name:
                if name in assets:
                    logger.warning(f"Duplicate asset name '{name}' found at {path}, skipping.")
                    continue
                assets[name] = {"path": path, "depends": depends}
                logger.info(f"Discovered asset: {name} ({path})")

    logger.info(f"Total assets discovered: {len(assets)}")
    return assets


def sanitize(name: str) -> str:
    """Convert asset name like dataset.financial_data to a valid Dagster op name."""
    return name.replace(".", "_").replace("-", "_")


# ============================================================
# OP FACTORY
# ============================================================

def make_bruin_op(asset_name: str, asset_path: str, dep_names: list[str]):
    """Create a Dagster op for a Bruin asset with retries and error handling."""
    op_name = sanitize(asset_name)
    retry_policy = RetryPolicy(
        max_retries=MAX_RETRIES,
        delay=RETRY_DELAY,
        backoff=Backoff.EXPONENTIAL,
    )

    def run_bruin(context: OpExecutionContext):
        context.log.info(f"Starting Bruin asset: {asset_name}")
        context.log.info(f"Path: {asset_path}")
        context.log.info(f"Environment: {BRUIN_ENV}")

        if not os.path.exists(asset_path):
            raise FileNotFoundError(f"Asset file not found: {asset_path}")

        start_time = datetime.utcnow()

        try:
            result = subprocess.run(
                [BRUIN_PATH, "run", "--environment", BRUIN_ENV, asset_path],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout per asset
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"{asset_name} timed out after 1 hour")
        except FileNotFoundError:
            raise Exception(
                f"Bruin binary not found at {BRUIN_PATH}. "
                f"Set the BRUIN_PATH environment variable."
            )

        duration = (datetime.utcnow() - start_time).total_seconds()

        if result.stdout:
            context.log.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            context.log.warning(f"STDERR:\n{result.stderr}")

        context.log.info(f"Duration: {duration:.2f}s | Exit code: {result.returncode}")

        if result.returncode != 0:
            raise Exception(
                f"Asset '{asset_name}' failed with exit code {result.returncode}.\n"
                f"STDOUT: {result.stdout}\n"
                f"STDERR: {result.stderr}"
            )

        context.log.info(f"✅ Asset '{asset_name}' completed successfully in {duration:.2f}s")

    if dep_names:
        ins = {sanitize(dep): In(Nothing) for dep in dep_names}

        @op(name=op_name, ins=ins, retry_policy=retry_policy, tags={"asset": asset_name})
        def bruin_op_with_deps(context: OpExecutionContext, **kwargs):
            run_bruin(context)

        return bruin_op_with_deps
    else:
        @op(name=op_name, retry_policy=retry_policy, tags={"asset": asset_name})
        def bruin_op_no_deps(context: OpExecutionContext):
            run_bruin(context)

        return bruin_op_no_deps


# ============================================================
# ASSET DISCOVERY + OP CREATION
# ============================================================

discovered = discover_bruin_assets()
ops_map = {
    name: make_bruin_op(name, meta["path"], meta["depends"])
    for name, meta in discovered.items()
}


# ============================================================
# JOB DEFINITION
# ============================================================

@job(
    executor_def=multiprocess_executor.configured({"max_concurrent": MAX_CONCURRENT}),
    hooks={alert_on_failure, log_on_success},
    tags={"project": "bruin", "environment": BRUIN_ENV},
)
def bruin_pipeline():
    outputs = {}

    # First pass: root assets (no dependencies)
    for name, meta in discovered.items():
        if not meta["depends"]:
            outputs[sanitize(name)] = ops_map[name]()

    # Subsequent passes: resolve dependencies iteratively
    remaining = {n: m for n, m in discovered.items() if m["depends"]}
    max_iterations = len(remaining) + 1
    iteration = 0

    while remaining and iteration < max_iterations:
        iteration += 1
        for name, meta in list(remaining.items()):
            dep_keys = [sanitize(d) for d in meta["depends"]]
            if all(d in outputs for d in dep_keys):
                dep_outputs = {d: outputs[d] for d in dep_keys}
                outputs[sanitize(name)] = ops_map[name](**dep_outputs)
                del remaining[name]

    if remaining:
        logger.warning(
            f"Could not resolve dependencies for: {list(remaining.keys())}. "
            f"Check for circular dependencies or missing upstream assets."
        )


# ============================================================
# SCHEDULES
# ============================================================

@schedule(
    cron_schedule="0 0 * * *",
    job=bruin_pipeline,
    execution_timezone="UTC",
)
def daily_bruin_schedule(_context):
    return {}


@schedule(
    cron_schedule="0 6 * * 1",  # Every Monday at 6am UTC
    job=bruin_pipeline,
    execution_timezone="UTC",
)
def weekly_bruin_schedule(_context):
    return {}


# ============================================================
# DEFINITIONS
# ============================================================

defs = Definitions(
    jobs=[bruin_pipeline],
    schedules=[daily_bruin_schedule, weekly_bruin_schedule],
)
