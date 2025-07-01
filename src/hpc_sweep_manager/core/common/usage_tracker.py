"""
Usage tracking system to monitor which code paths are executed during CLI operations.

This module provides decorators and utilities to track function calls, method usage,
and module imports to identify unused or underutilized code.
"""

import functools
import inspect
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Set, Optional
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


class UsageTracker:
    """Global usage tracker for monitoring code execution patterns."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        self.function_calls = defaultdict(int)
        self.call_stacks = defaultdict(list)
        self.timing_data = defaultdict(list)
        self.cli_commands = defaultdict(int)
        self.module_imports = set()
        self.enabled = os.getenv("HSM_TRACK_USAGE", "false").lower() == "true"
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = time.time()
        self._initialized = True

        # Create tracking directory
        self.tracking_dir = Path.cwd() / ".hsm_usage_tracking"
        self.tracking_dir.mkdir(exist_ok=True)

        logger.info(f"Usage tracker initialized (enabled={self.enabled})")

    def track_function_call(self, func_name: str, module_name: str, execution_time: float = None):
        """Track a function call with timing information."""
        if not self.enabled:
            return

        full_name = f"{module_name}.{func_name}"
        self.function_calls[full_name] += 1

        if execution_time is not None:
            self.timing_data[full_name].append(execution_time)

        # Capture call stack for context
        stack = inspect.stack()
        caller_info = []
        for frame_info in stack[2:6]:  # Skip this function and decorator
            caller_info.append(f"{frame_info.filename}:{frame_info.lineno}")
        self.call_stacks[full_name] = caller_info

    def track_cli_command(self, command: str):
        """Track CLI command usage."""
        if not self.enabled:
            return
        self.cli_commands[command] += 1

    def track_module_import(self, module_name: str):
        """Track module imports."""
        if not self.enabled:
            return
        self.module_imports.add(module_name)

    def save_session_data(self):
        """Save current session tracking data to file."""
        if not self.enabled:
            return

        session_file = self.tracking_dir / f"session_{self.session_id}.json"

        data = {
            "session_id": self.session_id,
            "start_time": self.start_time,
            "end_time": time.time(),
            "function_calls": dict(self.function_calls),
            "call_stacks": dict(self.call_stacks),
            "timing_data": {
                k: {"count": len(v), "avg_time": sum(v) / len(v) if v else 0, "total_time": sum(v)}
                for k, v in self.timing_data.items()
            },
            "cli_commands": dict(self.cli_commands),
            "module_imports": list(self.module_imports),
        }

        with open(session_file, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Usage data saved to {session_file}")

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of current usage data."""
        return {
            "total_function_calls": sum(self.function_calls.values()),
            "unique_functions_called": len(self.function_calls),
            "cli_commands_used": dict(self.cli_commands),
            "modules_imported": len(self.module_imports),
            "session_duration": time.time() - self.start_time,
        }


def track_usage(func: Callable) -> Callable:
    """Decorator to track function usage."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tracker = UsageTracker()
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            tracker.track_function_call(func.__name__, func.__module__, execution_time)
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            tracker.track_function_call(func.__name__, func.__module__, execution_time)
            raise

    return wrapper


def track_cli_command(command_name: str):
    """Decorator to track CLI command usage."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracker = UsageTracker()
            tracker.track_cli_command(command_name)
            return func(*args, **kwargs)

        return wrapper

    return decorator


class ImportTracker:
    """Context manager to track module imports during execution."""

    def __init__(self):
        self.tracker = UsageTracker()
        self.original_import = __builtins__.__import__

    def tracking_import(self, name, globals=None, locals=None, fromlist=(), level=0):
        """Custom import function that tracks imports."""
        # Only track project modules
        if name.startswith("hpc_sweep_manager"):
            self.tracker.track_module_import(name)
        return self.original_import(name, globals, locals, fromlist, level)

    def __enter__(self):
        __builtins__.__import__ = self.tracking_import
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        __builtins__.__import__ = self.original_import


# Analysis functions for the collected data
def analyze_usage_patterns(tracking_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Analyze usage patterns from collected tracking data."""
    if tracking_dir is None:
        tracking_dir = Path.cwd() / ".hsm_usage_tracking"

    if not tracking_dir.exists():
        return {"error": "No tracking data found"}

    session_files = list(tracking_dir.glob("session_*.json"))
    if not session_files:
        return {"error": "No session files found"}

    # Aggregate data from all sessions
    all_function_calls = defaultdict(int)
    all_cli_commands = defaultdict(int)
    all_modules = set()
    total_sessions = len(session_files)

    for session_file in session_files:
        with open(session_file, "r") as f:
            data = json.load(f)

        for func, count in data.get("function_calls", {}).items():
            all_function_calls[func] += count

        for cmd, count in data.get("cli_commands", {}).items():
            all_cli_commands[cmd] += count

        all_modules.update(data.get("module_imports", []))

    # Sort by usage frequency
    sorted_functions = sorted(all_function_calls.items(), key=lambda x: x[1], reverse=True)
    sorted_commands = sorted(all_cli_commands.items(), key=lambda x: x[1], reverse=True)

    return {
        "summary": {
            "total_sessions": total_sessions,
            "unique_functions_called": len(all_function_calls),
            "total_function_calls": sum(all_function_calls.values()),
            "unique_cli_commands": len(all_cli_commands),
            "modules_imported": len(all_modules),
        },
        "most_used_functions": sorted_functions[:20],
        "least_used_functions": sorted_functions[-20:] if len(sorted_functions) > 20 else [],
        "cli_command_usage": sorted_commands,
        "imported_modules": sorted(all_modules),
    }


def find_unused_functions(project_root: Path) -> Dict[str, Any]:
    """Find functions that are defined but never called according to tracking data."""
    tracking_dir = project_root / ".hsm_usage_tracking"

    # Get all tracked function calls
    usage_data = analyze_usage_patterns(tracking_dir)
    if "error" in usage_data:
        return usage_data

    tracked_functions = set(func for func, _ in usage_data.get("most_used_functions", []))
    tracked_functions.update(func for func, _ in usage_data.get("least_used_functions", []))

    # This is a simplified version - you could enhance this by parsing AST
    # to find all defined functions and compare with tracked ones
    return {
        "tracked_functions_count": len(tracked_functions),
        "note": "To find truly unused functions, combine this with AST analysis of your codebase",
    }


# Utility function to start tracking in CLI
def enable_usage_tracking():
    """Enable usage tracking for the current session."""
    os.environ["HSM_TRACK_USAGE"] = "true"
    tracker = UsageTracker()
    logger.info("Usage tracking enabled")
    return tracker


def generate_usage_report(output_file: Optional[Path] = None) -> str:
    """Generate a comprehensive usage report."""
    usage_data = analyze_usage_patterns()

    if "error" in usage_data:
        return f"Error generating report: {usage_data['error']}"

    report = []
    report.append("# HPC Sweep Manager Usage Analysis Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    # Summary
    summary = usage_data["summary"]
    report.append("## Summary")
    report.append(f"- Total tracking sessions: {summary['total_sessions']}")
    report.append(f"- Unique functions called: {summary['unique_functions_called']}")
    report.append(f"- Total function calls: {summary['total_function_calls']}")
    report.append(f"- CLI commands used: {summary['unique_cli_commands']}")
    report.append(f"- Modules imported: {summary['modules_imported']}")
    report.append("")

    # Most used functions
    report.append("## Most Used Functions")
    for func, count in usage_data["most_used_functions"]:
        report.append(f"- {func}: {count} calls")
    report.append("")

    # CLI command usage
    report.append("## CLI Command Usage")
    for cmd, count in usage_data["cli_command_usage"]:
        report.append(f"- {cmd}: {count} times")
    report.append("")

    # Recommendations
    report.append("## Recommendations")

    least_used = usage_data.get("least_used_functions", [])
    if least_used:
        report.append("### Potential Code for Review:")
        for func, count in least_used[-10:]:  # Show 10 least used
            if count == 1:
                report.append(f"- {func} (called only once)")

    report_content = "\n".join(report)

    if output_file:
        with open(output_file, "w") as f:
            f.write(report_content)
        print(f"Report saved to {output_file}")

    return report_content
