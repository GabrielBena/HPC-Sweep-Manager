"""Common CLI utilities shared across all command modules."""

import click


# Common CLI options as decorators
def common_options(func):
    """Add common CLI options to a command."""
    func = click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")(func)
    func = click.option("--quiet", "-q", is_flag=True, help="Suppress non-error output")(func)
    return func
