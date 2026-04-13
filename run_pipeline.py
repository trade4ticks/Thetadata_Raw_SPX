import subprocess
import sys
from datetime import datetime

STEPS = [
    {
        "name": "fetch_intraday",
        "cmd": ["/Thetadata_Raw_SPX/.venv/bin/python", "/Thetadata_Raw_SPX/fetch_intraday.py"],
        "cwd": "/Thetadata_Raw_SPX",
        "log": "/Thetadata_Raw_SPX/logs/fetch_intraday.log",
    },
    {
        "name": "clean_intraday",
        "cmd": ["/clean_SPX/.venv/bin/python", "/clean_SPX/process_intraday.py"],
        "cwd": "/clean_SPX",
        "log": "/clean_SPX/logs/process_intraday.log",
    },
    {
        "name": "interpolate_intraday",
        "cmd": ["/interpolate_SPX/.venv/bin/python", "/interpolate_SPX/scripts/process_intraday.py"],
        "cwd": "/interpolate_SPX",
        "log": "/interpolate_SPX/logs/process_intraday.log",
    },
]

def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

for step in STEPS:
    log(f"Starting {step['name']}")
    with open(step["log"], "a") as lf:
        lf.write(f"\n=== Starting {step['name']} at {datetime.now()} ===\n")
        result = subprocess.run(
            step["cmd"],
            cwd=step["cwd"],
            stdout=lf,
            stderr=subprocess.STDOUT,
            text=True,
        )
    if result.returncode != 0:
        log(f"FAILED: {step['name']} returned {result.returncode}")
        sys.exit(result.returncode)
    log(f"Finished {step['name']}")

log("Pipeline complete")
