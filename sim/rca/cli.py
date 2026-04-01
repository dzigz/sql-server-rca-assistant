"""
CLI commands for the AI RCA system.

Stage 1 focuses on web-app driven analysis. This module provides
auxiliary commands for configuration inspection.
"""

import argparse
import sys
import re
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.markdown import Markdown
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment, misc]
    Panel = None  # type: ignore[assignment, misc]
    Markdown = None  # type: ignore[assignment, misc]


def _print(msg: str, **kwargs) -> None:
    """Print message, stripping rich markup if rich not available."""
    if console:
        console.print(msg, **kwargs)
    else:
        plain = re.sub(r'\[/?[a-z_ ]+\]', '', msg)
        print(plain, **kwargs)


def cmd_rca_analyze(args):
    """Run RCA analysis - redirect users to web app mode."""
    _print("\n[bold yellow]Note:[/bold yellow] Stage 1 RCA runs through the web app.\n")
    _print("Start the assistant with:")
    _print("  [cyan]python -m sim webapp start[/cyan]\n")
    _print("Then open [cyan]http://localhost:3000[/cyan].")
    sys.exit(0)


def cmd_rca_config(args):
    """Show RCA configuration and validate API keys."""
    from sim.rca import RCAConfig

    _print("\n[bold blue]RCA Configuration[/bold blue]\n")

    try:
        config = RCAConfig()
        config_dict = config.to_dict()

        if RICH_AVAILABLE and Table:
            table = Table(title="Configuration")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="white")

            for key, value in config_dict.items():
                table.add_row(key, str(value))

            console.print(table)
        else:
            for key, value in config_dict.items():
                _print(f"{key}: {value}")

        _print("")

        # Validate
        try:
            config.validate()
            _print("[green]✓ Configuration is valid[/green]")
        except ValueError as e:
            _print(f"[red]✗ Configuration invalid:[/red] {e}")
            sys.exit(1)

    except Exception as e:
        _print(f"[red]Error:[/red] {e}")
        sys.exit(1)


def add_rca_subparsers(subparsers):
    """Add RCA subcommands to the CLI parser."""
    rca_parser = subparsers.add_parser(
        "rca",
        help="AI-powered Root Cause Analysis"
    )
    rca_subparsers = rca_parser.add_subparsers(dest="rca_command")

    # rca analyze - now deprecated, redirects to workflow
    analyze_parser = rca_subparsers.add_parser(
        "analyze",
        help="Run RCA analysis (redirects to web app mode)"
    )
    analyze_parser.set_defaults(func=cmd_rca_analyze)

    # rca config
    config_parser = rca_subparsers.add_parser(
        "config",
        help="Show RCA configuration"
    )
    config_parser.set_defaults(func=cmd_rca_config)

    return rca_parser
