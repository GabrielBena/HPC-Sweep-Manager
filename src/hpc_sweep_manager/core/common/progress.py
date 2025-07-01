"""A centralized progress bar manager for consistent progress reporting."""

from typing import Optional

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeElapsedColumn,
    )

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ProgressBarManager:
    """Manages a Rich progress bar for tracking sweep progress."""

    def __init__(self, console: Optional[Console] = None, show_progress: bool = True):
        if not RICH_AVAILABLE:
            self.progress = None
            self.console = None
            return

        self.console = console or Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
            disable=not show_progress,
        )
        self.task_id: Optional[TaskID] = None

    def add_task(self, description: str, total: int):
        """Add a new task to the progress bar."""
        if self.progress:
            self.task_id = self.progress.add_task(description, total=total)

    def update(self, completed: int, description: Optional[str] = None, **kwargs):
        """Update the progress of the task."""
        if self.progress and self.task_id is not None:
            update_kwargs = {"completed": completed}
            if description:
                update_kwargs["description"] = description
            update_kwargs.update(kwargs)
            self.progress.update(self.task_id, **update_kwargs)

    def start(self):
        """Start the progress bar."""
        if self.progress:
            self.progress.start()

    def stop(self):
        """Stop the progress bar."""
        if self.progress:
            self.progress.stop()

    def __enter__(self):
        if self.progress:
            return self.progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.progress:
            self.progress.__exit__(exc_type, exc_val, exc_tb)
