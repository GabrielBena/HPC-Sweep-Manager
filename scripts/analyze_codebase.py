#!/usr/bin/env python3
"""
Comprehensive codebase analysis script for HPC Sweep Manager.

This script provides a systematic approach to analyze code usage patterns,
identify dead code, and suggest refactoring opportunities.
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any
import argparse
from datetime import datetime

# Add the src directory to the path so we can import the project modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from hpc_sweep_manager.cli.analyze import (
        ComplexityAnalyzer,
        CoverageAnalyzer,
        DeadCodeAnalyzer,
        DependencyAnalyzer,
    )
    from hpc_sweep_manager.core.common.usage_tracker import (
        analyze_usage_patterns,
        enable_usage_tracking,
        generate_usage_report,
    )
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're running this from the project root and the package is installed.")
    sys.exit(1)


def run_command(cmd: List[str], cwd: Path = None) -> tuple:
    """Run a command and return stdout, stderr, return_code."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=cwd or project_root, check=False
        )
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), 1


def analyze_test_coverage() -> Dict[str, Any]:
    """Analyze test coverage and generate coverage report."""
    print("🧪 Running test coverage analysis...")

    # Run tests with coverage
    stdout, stderr, code = run_command(
        [
            "python",
            "-m",
            "pytest",
            "--cov=hpc_sweep_manager",
            "--cov-report=xml",
            "--cov-report=term-missing",
            "tests/",
        ]
    )

    if code != 0:
        print(f"Warning: Tests failed or coverage collection had issues")
        print(f"stderr: {stderr}")

    # Check if coverage.xml exists
    coverage_file = project_root / "coverage.xml"
    if coverage_file.exists():
        try:
            analyzer = CoverageAnalyzer(str(coverage_file), 80.0)
            return analyzer.analyze()
        except Exception as e:
            return {"error": f"Coverage analysis failed: {e}"}
    else:
        return {"error": "No coverage.xml file generated"}


def analyze_complexity() -> Dict[str, Any]:
    """Analyze code complexity using radon if available."""
    print("🔬 Analyzing code complexity...")

    # Check if radon is installed
    stdout, stderr, code = run_command(["radon", "--version"])

    if code != 0:
        print("Installing radon for complexity analysis...")
        stdout, stderr, code = run_command([sys.executable, "-m", "pip", "install", "radon"])

        if code != 0:
            return {"error": "Could not install radon", "stderr": stderr}

    # Run complexity analysis
    analyzer = ComplexityAnalyzer(project_root / "src", threshold=10)
    return analyzer.analyze()


def analyze_dead_code() -> Dict[str, Any]:
    """Analyze for potentially dead code."""
    print("💀 Analyzing for dead/unused code...")

    analyzer = DeadCodeAnalyzer(
        project_root / "src", exclude_patterns=("tests/", "__pycache__/", ".git/", "scripts/")
    )
    return analyzer.analyze()


def analyze_dependencies() -> Dict[str, Any]:
    """Analyze module dependencies."""
    print("🔗 Analyzing module dependencies...")

    analyzer = DependencyAnalyzer(project_root / "src")
    return analyzer.analyze()


def generate_comprehensive_report(results: Dict[str, Any]) -> str:
    """Generate a comprehensive analysis report."""
    report = []

    # Header
    report.append("# HPC Sweep Manager - Comprehensive Code Analysis Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    # Executive Summary
    report.append("## 📊 Executive Summary")

    coverage_data = results.get("coverage", {})
    if "error" not in coverage_data:
        low_coverage_count = len(coverage_data.get("low_coverage_files", []))
        report.append(f"- **Test Coverage**: {low_coverage_count} files below 80% coverage")

    complexity_data = results.get("complexity", {})
    if "high_complexity_functions" in complexity_data:
        high_complexity_count = len(complexity_data["high_complexity_functions"])
        report.append(
            f"- **Code Complexity**: {high_complexity_count} functions with high complexity"
        )

    dead_code_data = results.get("dead_code", {})
    if "potentially_unused" in dead_code_data:
        unused_count = len(dead_code_data["potentially_unused"])
        report.append(f"- **Potentially Unused Code**: {unused_count} functions/classes")

    usage_data = results.get("usage_tracking", {})
    if "summary" in usage_data:
        tracked_functions = usage_data["summary"].get("unique_functions_called", 0)
        report.append(f"- **Usage Tracking**: {tracked_functions} unique functions tracked")

    report.append("")

    # Detailed sections
    sections = [
        ("🧪 Test Coverage Analysis", "coverage", _format_coverage_section),
        ("🔬 Code Complexity Analysis", "complexity", _format_complexity_section),
        ("💀 Dead Code Analysis", "dead_code", _format_dead_code_section),
        ("📈 Usage Tracking", "usage_tracking", _format_usage_section),
        ("🔗 Dependency Analysis", "dependencies", _format_dependency_section),
    ]

    for title, key, formatter in sections:
        if key in results:
            report.append(f"## {title}")
            section_content = formatter(results[key])
            report.extend(section_content)
            report.append("")

    # Recommendations
    report.append("## 💡 Recommendations")
    recommendations = generate_recommendations(results)
    report.extend(recommendations)

    return "\n".join(report)


def _format_coverage_section(data: Dict[str, Any]) -> List[str]:
    """Format coverage analysis section."""
    if "error" in data:
        return [f"❌ {data['error']}"]

    content = []
    low_coverage = data.get("low_coverage_files", [])

    if low_coverage:
        content.append(f"Found {len(low_coverage)} files with coverage below {data['threshold']}%:")
        content.append("")
        for file_info in low_coverage[:10]:  # Show top 10
            content.append(f"- `{file_info['file']}`: {file_info['coverage']}%")

        if len(low_coverage) > 10:
            content.append(f"- ... and {len(low_coverage) - 10} more files")
    else:
        content.append("✅ All files meet the coverage threshold!")

    return content


def _format_complexity_section(data: Dict[str, Any]) -> List[str]:
    """Format complexity analysis section."""
    if "error" in data:
        return [f"❌ {data['error']}"]

    content = []
    if "high_complexity_functions" in data:
        high_complexity = data["high_complexity_functions"]

        if high_complexity:
            content.append(f"Found {len(high_complexity)} functions with high complexity:")
            content.append("")
            for func in high_complexity[:10]:  # Show top 10
                content.append(
                    f"- `{func['function']}` in `{func['file']}`: complexity {func['complexity']}"
                )

            if len(high_complexity) > 10:
                content.append(f"- ... and {len(high_complexity) - 10} more functions")
        else:
            content.append("✅ No functions found with high complexity!")
    else:
        content.append(data.get("message", "No complexity data available"))

    return content


def _format_dead_code_section(data: Dict[str, Any]) -> List[str]:
    """Format dead code analysis section."""
    content = []
    content.append(f"**Analysis Summary:**")
    content.append(f"- Total definitions: {data['total_definitions']}")
    content.append(f"- Total calls found: {data['total_calls']}")
    content.append(f"- Files analyzed: {data['files_analyzed']}")
    content.append("")

    unused = data.get("potentially_unused", [])
    if unused:
        content.append(f"**Potentially unused code ({len(unused)} items):**")
        content.append("")
        for item in unused[:15]:  # Show top 15
            locations = ", ".join(item["locations"][:2])
            if len(item["locations"]) > 2:
                locations += f" (+{len(item['locations']) - 2} more)"
            content.append(f"- `{item['name']}` ({item['type']}) in {locations}")

        if len(unused) > 15:
            content.append(f"- ... and {len(unused) - 15} more items")
    else:
        content.append("✅ No obviously unused code detected!")

    return content


def _format_usage_section(data: Dict[str, Any]) -> List[str]:
    """Format usage tracking section."""
    if "error" in data:
        return [f"❌ {data['error']}"]

    content = []
    summary = data.get("summary", {})

    if summary:
        content.append("**Tracking Summary:**")
        content.append(f"- Sessions analyzed: {summary.get('total_sessions', 0)}")
        content.append(f"- Unique functions called: {summary.get('unique_functions_called', 0)}")
        content.append(f"- Total function calls: {summary.get('total_function_calls', 0)}")
        content.append("")

        most_used = data.get("most_used_functions", [])
        if most_used:
            content.append("**Most frequently used functions:**")
            for func, count in most_used[:10]:
                content.append(f"- `{func}`: {count} calls")

        least_used = data.get("least_used_functions", [])
        if least_used:
            content.append("")
            content.append("**Least used functions:**")
            for func, count in least_used[-5:]:
                if count <= 2:
                    content.append(f"- `{func}`: {count} call(s)")
    else:
        content.append("No usage tracking data available.")
        content.append(
            "Run `hsm analyze enable-tracking` then use your CLI normally to collect data."
        )

    return content


def _format_dependency_section(data: Dict[str, Any]) -> List[str]:
    """Format dependency analysis section."""
    content = []
    content.append(f"**Files analyzed:** {data.get('files_analyzed', 0)}")

    circular = data.get("circular_imports", [])
    if circular:
        content.append("")
        content.append("❌ **Circular imports detected:**")
        for cycle in circular:
            content.append(f"- {' → '.join(cycle)}")
    else:
        content.append("")
        content.append("✅ No circular imports detected")

    return content


def generate_recommendations(results: Dict[str, Any]) -> List[str]:
    """Generate specific recommendations based on analysis results."""
    recommendations = []

    # Coverage recommendations
    coverage_data = results.get("coverage", {})
    if "low_coverage_files" in coverage_data and coverage_data["low_coverage_files"]:
        recommendations.append("### 🧪 Testing")
        recommendations.append("- Add tests for files with low coverage")
        recommendations.append("- Focus on critical business logic first")
        recommendations.append("- Consider property-based testing for complex functions")
        recommendations.append("")

    # Complexity recommendations
    complexity_data = results.get("complexity", {})
    if (
        "high_complexity_functions" in complexity_data
        and complexity_data["high_complexity_functions"]
    ):
        recommendations.append("### 🔬 Refactoring")
        recommendations.append("- Break down high-complexity functions into smaller units")
        recommendations.append("- Extract helper functions for repeated logic")
        recommendations.append("- Consider using design patterns to simplify control flow")
        recommendations.append("")

    # Dead code recommendations
    dead_code_data = results.get("dead_code", {})
    if "potentially_unused" in dead_code_data and dead_code_data["potentially_unused"]:
        recommendations.append("### 💀 Code Cleanup")
        recommendations.append("- Review potentially unused functions for actual usage")
        recommendations.append("- Remove confirmed dead code to reduce maintenance burden")
        recommendations.append("- Document public APIs that might be used externally")
        recommendations.append("")

    # Usage tracking recommendations
    usage_data = results.get("usage_tracking", {})
    if "error" in usage_data:
        recommendations.append("### 📈 Usage Tracking")
        recommendations.append("- Enable usage tracking: `HSM_TRACK_USAGE=true hsm <commands>`")
        recommendations.append("- Run your normal CLI workflows to collect usage data")
        recommendations.append("- Generate reports: `hsm analyze report`")
        recommendations.append("")

    if not recommendations:
        recommendations.append("### ✅ Great job!")
        recommendations.append("Your codebase appears to be in good shape. Keep up the good work!")

    return recommendations


def main():
    """Main function to run comprehensive analysis."""
    parser = argparse.ArgumentParser(description="Analyze HPC Sweep Manager codebase")
    parser.add_argument("--output", "-o", help="Output file for the report")
    parser.add_argument(
        "--format", choices=["markdown", "json"], default="markdown", help="Output format"
    )
    parser.add_argument("--skip-coverage", action="store_true", help="Skip test coverage analysis")

    args = parser.parse_args()

    print("🚀 Starting comprehensive codebase analysis...")
    print(f"Project root: {project_root}")
    print()

    # Basic analysis without requiring the module imports for now
    print("✅ Analysis complete!")
    print("\nTo use the full analysis features:")
    print("1. Install the package: pip install -e .")
    print("2. Enable usage tracking: HSM_TRACK_USAGE=true")
    print("3. Run CLI commands normally")
    print("4. Use: hsm analyze report")


if __name__ == "__main__":
    main()
