from dagster import op, job, schedule, Definitions, In, Nothing, multiprocess_executor
import subprocess
import os
import glob
import yaml
import re

BRUIN_PROJECT = "/Users/robin/Desktop/bruin_test/bruin/etl_test1"
BRUIN_ENV = "default"
BRUIN_PATH = "/Users/robin/.local/bin/bruin"


def parse_dependencies(path: str) -> tuple[str, list[str]]:
    """Extract the asset name and depends list from a Bruin asset file."""
    content = open(path).read()
    name = None
    depends = []

    if path.endswith(".sql"):
        # Parse from SQL block comment /* @bruin ... @bruin */
        match = re.search(r'/\*\s*@bruin(.*?)@bruin\s*\*/', content, re.DOTALL)
        if match:
            block = match.group(1)
            parsed = yaml.safe_load(block)
            if parsed:
                name = parsed.get("name")
                depends = parsed.get("depends", [])

    elif path.endswith(".py"):
        # Parse from Python docstring """ @bruin ... @bruin """
        match = re.search(r'"""@bruin(.*?)@bruin"""', content, re.DOTALL)
        if match:
            block = match.group(1)
            parsed = yaml.safe_load(block)
            if parsed:
                name = parsed.get("name")
                depends = parsed.get("depends", [])

    elif path.endswith(".yml") or path.endswith(".yaml"):
        parsed = yaml.safe_load(content)
        if parsed:
            name = parsed.get("name")
            depends = parsed.get("depends", [])

    return name, depends


def discover_bruin_assets() -> dict:
    """Scan the project and return a dict of {asset_name: {path, depends}}."""
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
                assets[name] = {"path": path, "depends": depends}
    return assets


def sanitize(name: str) -> str:
    """Convert asset name like dataset.financial_data to a valid op name."""
    return name.replace(".", "_").replace("-", "_")


def make_bruin_op(asset_name: str, asset_path: str, dep_names: list[str]):
    """Create a Dagster op for a Bruin asset, with upstream dependencies."""
    op_name = sanitize(asset_name)

    if dep_names:
        ins = {sanitize(dep): In(Nothing) for dep in dep_names}

        @op(name=op_name, ins=ins)
        def bruin_op(**kwargs):
            result = subprocess.run(
                [BRUIN_PATH, "run", "--environment", BRUIN_ENV, asset_path],
                capture_output=True,
                text=True
            )
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            if result.returncode != 0:
                raise Exception(f"{asset_name} failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    else:
        @op(name=op_name)
        def bruin_op():
            result = subprocess.run(
                [BRUIN_PATH, "run", "--environment", BRUIN_ENV, asset_path],
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.returncode != 0:
                raise Exception(f"{asset_name} failed:\n{result.stderr}")

    return bruin_op


# Discover all assets and build ops
discovered = discover_bruin_assets()
ops_map = {
    name: make_bruin_op(name, meta["path"], meta["depends"])
    for name, meta in discovered.items()
}


@job
def bruin_pipeline():
    # Track op outputs keyed by sanitized asset name
    outputs = {}

    # First pass: run assets with no dependencies
    for name, meta in discovered.items():
        if not meta["depends"]:
            outputs[sanitize(name)] = ops_map[name]()

    # Second pass: run assets that depend on already-run assets
    # Repeat until all assets are scheduled
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


@schedule(cron_schedule="0 0 * * *", job=bruin_pipeline)
def daily_bruin_schedule(_context):
    return {}


defs = Definitions(
    jobs=[bruin_pipeline],
    schedules=[daily_bruin_schedule],
)
