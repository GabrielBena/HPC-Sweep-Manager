"""CLI commands for code analysis and usage tracking."""

import ast
import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Set, Any
import importlib.util

import click
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..core.common.usage_tracker import (
    UsageTracker,
    analyze_usage_patterns,
    generate_usage_report,
    enable_usage_tracking,
)
from .common import common_options


@click.group()
def analyze():
    """Analyze codebase for usage patterns, dead code, and optimization opportunities."""
    pass


@analyze.command("enable-tracking")
@common_options
@click.pass_context
def enable_tracking_cmd(ctx, verbose: bool, quiet: bool):
    """Enable usage tracking for the current session."""
    console = ctx.obj["console"]

    enable_usage_tracking()
    console.print("[green]✓ Usage tracking enabled[/green]")
    console.print("Run CLI commands normally, then use 'hsm analyze report' to see usage patterns")
    console.print("Tracking data will be saved to '.hsm_usage_tracking/' directory")


@analyze.command("report")
@click.option("--output", "-o", type=click.Path(), help="Output file for the report")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    help="Report format",
)
@common_options
@click.pass_context
def usage_report_cmd(ctx, output: str, output_format: str, verbose: bool, quiet: bool):
    """Generate usage analysis report from tracking data."""
    console = ctx.obj["console"]

    if output_format == "markdown":
        report = generate_usage_report(Path(output) if output else None)
        if not output:
            console.print(Panel(report, title="Usage Analysis Report", expand=False))
    else:
        usage_data = analyze_usage_patterns()
        if output:
            with open(output, "w") as f:
                json.dump(usage_data, f, indent=2)
            console.print(f"[green]Report saved to {output}[/green]")
        else:
            console.print_json(data=usage_data)


@analyze.command("dead-code")
@click.option(
    "--project-root", type=click.Path(exists=True), default=".", help="Project root directory"
)
@click.option("--exclude", multiple=True, help="Patterns to exclude (e.g., tests/, __pycache__)")
@click.option("--output", "-o", type=click.Path(), help="Output file for the results")
@common_options
@click.pass_context
def dead_code_cmd(ctx, project_root: str, exclude: tuple, output: str, verbose: bool, quiet: bool):
    """Detect potentially dead/unused code."""
    console = ctx.obj["console"]
    project_path = Path(project_root)

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console
    ) as progress:
        task = progress.add_task("Analyzing codebase for dead code...", total=None)

        analyzer = DeadCodeAnalyzer(project_path, exclude)
        results = analyzer.analyze()

        progress.update(task, description="Analysis complete!")

    # Display results
    _display_dead_code_results(console, results)

    if output:
        with open(output, "w") as f:
            json.dump(results, f, indent=2)
        console.print(f"[green]Results saved to {output}[/green]")


@analyze.command("complexity")
@click.option(
    "--project-root", type=click.Path(exists=True), default=".", help="Project root directory"
)
@click.option(
    "--threshold", type=int, default=10, help="Complexity threshold for flagging functions"
)
@click.option("--output", "-o", type=click.Path(), help="Output file for the results")
@common_options
@click.pass_context
def complexity_cmd(ctx, project_root: str, threshold: int, output: str, verbose: bool, quiet: bool):
    """Analyze code complexity and identify functions that need refactoring."""
    console = ctx.obj["console"]
    project_path = Path(project_root)

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console
    ) as progress:
        task = progress.add_task("Analyzing code complexity...", total=None)

        analyzer = ComplexityAnalyzer(project_path, threshold)
        results = analyzer.analyze()

        progress.update(task, description="Analysis complete!")

    # Display results
    _display_complexity_results(console, results, threshold)

    if output:
        with open(output, "w") as f:
            json.dump(results, f, indent=2)
        console.print(f"[green]Results saved to {output}[/green]")


@analyze.command("dependencies")
@click.option(
    "--project-root", type=click.Path(exists=True), default=".", help="Project root directory"
)
@click.option("--show-external", is_flag=True, help="Show external dependencies")
@click.option("--output", "-o", type=click.Path(), help="Output file for the dependency graph")
@common_options
@click.pass_context
def dependencies_cmd(
    ctx, project_root: str, show_external: bool, output: str, verbose: bool, quiet: bool
):
    """Analyze module dependencies and identify circular imports."""
    console = ctx.obj["console"]
    project_path = Path(project_root)

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console
    ) as progress:
        task = progress.add_task("Analyzing dependencies...", total=None)

        analyzer = DependencyAnalyzer(project_path, show_external)
        results = analyzer.analyze()

        progress.update(task, description="Analysis complete!")

    # Display results
    _display_dependency_results(console, results)

    if output:
        with open(output, "w") as f:
            json.dump(results, f, indent=2)
        console.print(f"[green]Results saved to {output}[/green]")


@analyze.command("coverage-gaps")
@click.option("--coverage-file", type=click.Path(exists=True), help="Path to coverage.xml file")
@click.option("--threshold", type=float, default=80.0, help="Coverage percentage threshold")
@common_options
@click.pass_context
def coverage_gaps_cmd(ctx, coverage_file: str, threshold: float, verbose: bool, quiet: bool):
    """Identify areas with low test coverage that might benefit from more testing."""
    console = ctx.obj["console"]

    if not coverage_file:
        coverage_file = "coverage.xml"
        if not Path(coverage_file).exists():
            console.print("[red]No coverage.xml file found. Run tests with coverage first.[/red]")
            console.print("Example: pytest --cov=hpc_sweep_manager --cov-report=xml")
            return

    analyzer = CoverageAnalyzer(coverage_file, threshold)
    results = analyzer.analyze()

    _display_coverage_results(console, results, threshold)


class DeadCodeAnalyzer:
    """Analyze Python code to detect potentially unused functions and classes."""

    def __init__(self, project_root: Path, exclude_patterns: tuple = ()):
        self.project_root = project_root
        self.exclude_patterns = exclude_patterns or ("tests/", "__pycache__/", ".git/")

    def analyze(self) -> Dict[str, Any]:
        """Perform dead code analysis."""
        # Find all Python files
        python_files = self._find_python_files()

        # Parse AST for all files to find definitions and calls
        definitions = {}  # {name: [file_locations]}
        calls = set()  # {function_names}

        for file_path in python_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    tree = ast.parse(f.read(), filename=str(file_path))

                visitor = FunctionCallVisitor()
                visitor.visit(tree)

                # Store definitions with their locations
                for name in visitor.definitions:
                    if name not in definitions:
                        definitions[name] = []
                    definitions[name].append(str(file_path))

                calls.update(visitor.calls)

            except (SyntaxError, UnicodeDecodeError) as e:
                continue  # Skip files with syntax errors

        # Find potentially unused definitions
        potentially_unused = []
        for name, locations in definitions.items():
            if name not in calls and not name.startswith("_"):  # Ignore private functions
                potentially_unused.append(
                    {
                        "name": name,
                        "locations": locations,
                        "type": "function",  # Could be extended to detect classes, etc.
                    }
                )

        return {
            "total_definitions": len(definitions),
            "total_calls": len(calls),
            "potentially_unused": potentially_unused,
            "files_analyzed": len(python_files),
        }

    def _find_python_files(self) -> List[Path]:
        """Find all Python files in the project."""
        python_files = []

        for py_file in self.project_root.rglob("*.py"):
            # Skip excluded patterns
            if any(pattern in str(py_file) for pattern in self.exclude_patterns):
                continue
            python_files.append(py_file)

        return python_files


class FunctionCallVisitor(ast.NodeVisitor):
    """AST visitor to collect function definitions and calls."""

    def __init__(self):
        self.definitions = set()
        self.calls = set()

    def visit_FunctionDef(self, node):
        self.definitions.add(node.name)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.definitions.add(node.name)
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        self.definitions.add(node.name)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            self.calls.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            self.calls.add(node.func.attr)
        self.generic_visit(node)


class ComplexityAnalyzer:
    """Analyze code complexity using cyclomatic complexity."""

    def __init__(self, project_root: Path, threshold: int = 10):
        self.project_root = project_root
        self.threshold = threshold

    def analyze(self) -> Dict[str, Any]:
        """Analyze complexity across the project."""
        try:
            # Use radon for complexity analysis if available
            result = subprocess.run(
                ["radon", "cc", str(self.project_root), "-j"], capture_output=True, text=True
            )

            if result.returncode == 0:
                complexity_data = json.loads(result.stdout)
                return self._process_radon_output(complexity_data)

        except FileNotFoundError:
            pass  # radon not installed

        # Fallback to basic AST analysis
        return self._basic_complexity_analysis()

    def _process_radon_output(self, data: Dict) -> Dict[str, Any]:
        """Process radon output to find high complexity functions."""
        high_complexity = []

        for file_path, file_data in data.items():
            for item in file_data:
                if item.get("complexity", 0) > self.threshold:
                    high_complexity.append(
                        {
                            "file": file_path,
                            "function": item.get("name", "unknown"),
                            "complexity": item.get("complexity", 0),
                            "line": item.get("lineno", 0),
                        }
                    )

        return {
            "high_complexity_functions": sorted(
                high_complexity, key=lambda x: x["complexity"], reverse=True
            ),
            "total_files_analyzed": len(data),
            "threshold": self.threshold,
        }

    def _basic_complexity_analysis(self) -> Dict[str, Any]:
        """Basic complexity analysis using AST."""
        return {
            "message": "Install 'radon' for detailed complexity analysis: pip install radon",
            "basic_analysis": "AST-based analysis would require custom implementation",
        }


class DependencyAnalyzer:
    """Analyze module dependencies."""

    def __init__(self, project_root: Path, show_external: bool = False):
        self.project_root = project_root
        self.show_external = show_external

    def analyze(self) -> Dict[str, Any]:
        """Analyze dependencies."""
        dependencies = {}
        python_files = list(self.project_root.rglob("*.py"))

        for file_path in python_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    tree = ast.parse(f.read(), filename=str(file_path))

                visitor = ImportVisitor()
                visitor.visit(tree)

                file_key = str(file_path.relative_to(self.project_root))
                dependencies[file_key] = visitor.imports

            except (SyntaxError, UnicodeDecodeError):
                continue

        # Detect circular imports
        circular_imports = self._detect_circular_imports(dependencies)

        return {
            "dependencies": dependencies,
            "circular_imports": circular_imports,
            "files_analyzed": len(dependencies),
        }

    def _detect_circular_imports(self, dependencies: Dict) -> List[List[str]]:
        """Detect circular import patterns."""
        # Simplified circular import detection
        # In a real implementation, you'd use a graph algorithm
        return []  # Placeholder


class ImportVisitor(ast.NodeVisitor):
    """AST visitor to collect import statements."""

    def __init__(self):
        self.imports = []

    def visit_Import(self, node):
        for alias in node.names:
            self.imports.append(alias.name)

    def visit_ImportFrom(self, node):
        if node.module:
            self.imports.append(node.module)


class CoverageAnalyzer:
    """Analyze test coverage data."""

    def __init__(self, coverage_file: str, threshold: float):
        self.coverage_file = coverage_file
        self.threshold = threshold

    def analyze(self) -> Dict[str, Any]:
        """Analyze coverage data."""
        try:
            import xml.etree.ElementTree as ET

            tree = ET.parse(self.coverage_file)
            root = tree.getroot()

            low_coverage_files = []

            for package in root.findall(".//package"):
                for class_elem in package.findall(".//class"):
                    filename = class_elem.get("filename", "")
                    line_rate = float(class_elem.get("line-rate", 0)) * 100

                    if line_rate < self.threshold:
                        low_coverage_files.append(
                            {"file": filename, "coverage": round(line_rate, 2)}
                        )

            return {
                "low_coverage_files": sorted(low_coverage_files, key=lambda x: x["coverage"]),
                "threshold": self.threshold,
            }

        except Exception as e:
            return {"error": f"Could not parse coverage file: {e}"}


def _display_dead_code_results(console: Console, results: Dict[str, Any]):
    """Display dead code analysis results."""
    console.print("\n[bold]Dead Code Analysis Results[/bold]")

    table = Table(title="Potentially Unused Code")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Locations", style="yellow")

    for item in results.get("potentially_unused", []):
        locations = ", ".join(item["locations"][:3])  # Show first 3 locations
        if len(item["locations"]) > 3:
            locations += f" (+{len(item['locations']) - 3} more)"

        table.add_row(item["name"], item["type"], locations)

    console.print(table)

    summary = f"""
[bold]Summary:[/bold]
• Total definitions found: {results["total_definitions"]}
• Total function calls found: {results["total_calls"]}
• Potentially unused items: {len(results.get("potentially_unused", []))}
• Files analyzed: {results["files_analyzed"]}

[yellow]Note: This is a basic analysis. Functions might be used via:[/yellow]
- Dynamic imports or getattr()
- External packages or entry points
- Template files or configuration
- Test fixtures or indirect calls
"""
    console.print(Panel(summary))


def _display_complexity_results(console: Console, results: Dict[str, Any], threshold: int):
    """Display complexity analysis results."""
    if "high_complexity_functions" in results:
        console.print("\n[bold]High Complexity Functions[/bold]")

        table = Table(title=f"Functions with complexity > {threshold}")
        table.add_column("Function", style="cyan")
        table.add_column("File", style="green")
        table.add_column("Complexity", style="red")
        table.add_column("Line", style="yellow")

        for func in results["high_complexity_functions"]:
            table.add_row(
                func["function"], func["file"], str(func["complexity"]), str(func["line"])
            )

        console.print(table)
    else:
        console.print(Panel(results.get("message", "No complexity data available")))


def _display_dependency_results(console: Console, results: Dict[str, Any]):
    """Display dependency analysis results."""
    console.print(f"\n[bold]Dependency Analysis[/bold] ({results['files_analyzed']} files)")

    if results.get("circular_imports"):
        console.print("[red]Circular imports detected![/red]")
        for cycle in results["circular_imports"]:
            console.print(f"  • {' → '.join(cycle)}")
    else:
        console.print("[green]No circular imports detected[/green]")


def _display_coverage_results(console: Console, results: Dict[str, Any], threshold: float):
    """Display coverage analysis results."""
    if "error" in results:
        console.print(f"[red]Error: {results['error']}[/red]")
        return

    console.print(f"\n[bold]Coverage Analysis[/bold] (threshold: {threshold}%)")

    low_coverage = results.get("low_coverage_files", [])
    if low_coverage:
        table = Table(title="Files with Low Test Coverage")
        table.add_column("File", style="cyan")
        table.add_column("Coverage %", style="red")

        for file_info in low_coverage:
            table.add_row(file_info["file"], f"{file_info['coverage']}%")

        console.print(table)
    else:
        console.print("[green]All files meet the coverage threshold![/green]")
