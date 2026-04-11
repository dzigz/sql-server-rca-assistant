"""Command-line interface for SQL Server RCA Assistant (Stage 1)."""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from sim.logging_config import configure_logging


try:
    from rich.console import Console  # type: ignore[import-not-found]

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None  # type: ignore[assignment]


def _print(msg: str, **kwargs) -> None:
    """Print message, stripping rich markup if rich is unavailable."""
    if console:
        console.print(msg, **kwargs)
    else:
        plain = re.sub(r"\[/?[a-z_ ]+\]", "", msg)
        print(plain, **kwargs)


def _apply_backend_env(overrides: dict[str, str | None], base: dict[str, str] | None = None) -> dict[str, str]:
    """Apply non-empty environment overrides for the backend process."""
    env = dict(base or os.environ)
    for key, value in overrides.items():
        if value is not None and value != "":
            env[key] = str(value)
    return env


def _wait_for_http_ok(
    url: str,
    *,
    attempts: int,
    request_timeout: float = 1.5,
    sleep_seconds: float = 1.0,
) -> tuple[bool, str]:
    """
    Poll an HTTP endpoint until it returns 200 or the retry budget is exhausted.

    Startup races can surface as connection resets, remote disconnects, or plain
    connect failures. Treat all of those as "not ready yet" and keep polling.
    """
    last_error = ""
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=request_timeout) as resp:
                if resp.status == 200:
                    return True, ""
                last_error = f"HTTP {resp.status}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(sleep_seconds)
    return False, last_error


def _ensure_frontend_dependencies(frontend_dir: Path) -> None:
    """Install frontend dependencies if Next.js packages are missing."""
    next_bin = frontend_dir / "node_modules" / ".bin" / "next"
    if next_bin.exists():
        return

    _print("[yellow]Installing frontend dependencies...[/yellow]")
    subprocess.run(["npm", "install"], cwd=frontend_dir, check=True)


def _get_preferred_backend_python(sim_dir: Path) -> Path:
    """Prefer the project-local virtualenv interpreter when it exists."""
    venv_python = sim_dir / ".venv" / "bin" / "python"
    if venv_python.exists():
        return venv_python
    return Path(sys.executable)


def _same_python_interpreter(left: Path, right: Path) -> bool:
    """Best-effort comparison for Python interpreter paths."""
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return str(left) == str(right)


def _ensure_backend_python_dependencies(python_executable: Path, sim_dir: Path) -> None:
    """Fail fast with an actionable message when backend Python deps are missing."""
    check_code = """
import importlib.util
import sys

required = {
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "multipart": "python-multipart",
}
missing = [package for module, package in required.items() if importlib.util.find_spec(module) is None]
if missing:
    print("\\n".join(missing))
    sys.exit(1)
"""
    result = subprocess.run(
        [str(python_executable), "-c", check_code],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    missing_packages = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    venv_python = sim_dir / ".venv" / "bin" / "python"
    if venv_python.exists():
        install_hint = (
            "Install the missing backend dependencies into the project environment with:\n"
            "  sim/.venv/bin/python -m pip install -r sim/requirements.txt\n"
            "Then rerun:\n"
            "  python -m sim webapp start"
        )
    else:
        install_hint = (
            "Create and install the project environment with:\n"
            "  python3.11 -m venv sim/.venv\n"
            "  sim/.venv/bin/python -m pip install -r sim/requirements.txt\n"
            "Then rerun:\n"
            "  python -m sim webapp start"
        )

    raise ValueError(
        "Backend dependencies are missing in the selected interpreter "
        f"({python_executable}). Missing packages: {', '.join(missing_packages) or 'unknown'}. "
        f"{install_hint}"
    )


def _to_bool(value: str | bool | None, default: bool = False) -> bool:
    """Parse a bool-like value."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _validate_sqlserver_env(env: dict[str, str]) -> None:
    """Validate required SQL Server env vars for stage-1 direct mode."""
    host = env.get("SQLSERVER_HOST")
    if not host:
        raise ValueError(
            "SQL Server target host is required. Set SQLSERVER_HOST or pass --sqlserver-host."
        )

    has_password = bool(
        env.get("SQLSERVER_PASSWORD")
        or env.get("SA_PASSWORD")
        or env.get("SIM_SA_PASSWORD")
    )
    if not has_password:
        raise ValueError(
            "SQL Server password is required. Set SQLSERVER_PASSWORD (or SA_PASSWORD/SIM_SA_PASSWORD) "
            "or pass --sqlserver-password."
        )


def _backend_env_from_args(args: argparse.Namespace) -> dict[str, str]:
    """Build backend environment from CLI args + existing env."""
    overrides: dict[str, str | None] = {
        "SQLSERVER_HOST": args.sqlserver_host,
        "SQLSERVER_PORT": str(args.sqlserver_port) if args.sqlserver_port is not None else None,
        "SQLSERVER_USER": args.sqlserver_user,
        "SQLSERVER_DATABASE": args.sqlserver_database,
        "SQLSERVER_PASSWORD": args.sqlserver_password,
        # Keep compatibility with existing password resolvers.
        "SA_PASSWORD": args.sqlserver_password,
        "SIM_AUTO_INSTALL_BLITZ": "1" if getattr(args, "auto_install_blitz", True) else "0",
    }

    if getattr(args, "enable_monitoring", False):
        overrides.update(
            {
                "SIM_ENABLE_MONITORING": "1",
                "CLICKHOUSE_HOST": args.clickhouse_host,
                "CLICKHOUSE_PORT": str(args.clickhouse_port),
                "CLICKHOUSE_DATABASE": args.clickhouse_database,
                "CLICKHOUSE_USER": args.clickhouse_user,
                "CLICKHOUSE_PASSWORD": args.clickhouse_password,
            }
        )
    else:
        overrides["SIM_ENABLE_MONITORING"] = "0"

    return _apply_backend_env(overrides)


def _start_monitoring_stack(args: argparse.Namespace, backend_env: dict[str, str]) -> str:
    """Start optional monitoring stack (ClickHouse + collector + Grafana)."""
    sim_dir = Path(__file__).parent
    compose_dir = sim_dir / "docker"

    grafana_password = (
        args.grafana_password
        or os.environ.get("GRAFANA_PASSWORD")
        or "admin123!"
    )

    compose_env = _apply_backend_env(
        {
            "SQLSERVER_HOST": backend_env.get("SQLSERVER_HOST"),
            "SQLSERVER_PORT": backend_env.get("SQLSERVER_PORT"),
            "SQLSERVER_USER": backend_env.get("SQLSERVER_USER"),
            "SQLSERVER_PASSWORD": backend_env.get("SQLSERVER_PASSWORD"),
            "SQLSERVER_DATABASE": backend_env.get("SQLSERVER_DATABASE"),
            "CLICKHOUSE_USER": backend_env.get("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": backend_env.get("CLICKHOUSE_PASSWORD"),
            "GRAFANA_PASSWORD": grafana_password,
        }
    )

    _print("[cyan]Starting monitoring stack (docker compose)...[/cyan]")
    cmd = [
        "docker",
        "compose",
        "up",
        "-d",
        "clickhouse",
        "clickhouse-init",
        "dmv-collector",
        "grafana",
    ]
    try:
        subprocess.run(cmd, cwd=compose_dir, env=compose_env, check=True)
    except FileNotFoundError as exc:
        raise ValueError(
            "Docker is required to start the optional monitoring stack, but the 'docker' command "
            "was not found. Install Docker Desktop/Engine, or run without monitoring using "
            "'python -m sim webapp start --no-monitoring-stack --no-monitoring'."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = ""
        if exc.stderr:
            stderr = exc.stderr.strip()
        elif exc.stdout:
            stderr = exc.stdout.strip()

        if "Cannot connect to the Docker daemon" in stderr:
            raise ValueError(
                "Docker is installed but the Docker daemon is not running. Start Docker Desktop/Engine "
                "and retry, or run without monitoring using "
                "'python -m sim webapp start --no-monitoring-stack --no-monitoring'."
            ) from exc

        raise ValueError(
            "Failed to start the optional monitoring stack via docker compose. "
            f"Details: {stderr or exc}"
        ) from exc

    # Best-effort readiness checks. The DMV collector may spend up to ~60s
    # waiting for SQL Server before its HTTP API comes up.
    clickhouse_ready, clickhouse_error = _wait_for_http_ok(
        "http://127.0.0.1:8123/ping",
        attempts=30,
    )
    if not clickhouse_ready:
        raise ValueError(
            "Monitoring stack started but ClickHouse did not become ready at "
            "http://127.0.0.1:8123/ping. "
            f"Last error: {clickhouse_error or 'unknown error'}"
        )

    collector_ready, collector_error = _wait_for_http_ok(
        "http://127.0.0.1:8080/health",
        attempts=90,
    )
    if not collector_ready:
        raise ValueError(
            "Monitoring containers started, but the DMV collector did not become ready at "
            "http://127.0.0.1:8080/health. "
            f"Last error: {collector_error or 'unknown error'}. "
            "Check collector logs with 'docker compose -f sim/docker/docker-compose.yaml logs dmv-collector'."
        )

    _print("[green]Monitoring stack is up.[/green]")
    return grafana_password


def cmd_webapp_start(args: argparse.Namespace) -> None:
    """Start backend and frontend servers."""
    backend_port = args.backend_port
    frontend_port = args.frontend_port
    repo_path = getattr(args, "repo_path", None)
    backend_host = "127.0.0.1"
    sim_dir = Path(__file__).parent
    backend_python = _get_preferred_backend_python(sim_dir)

    backend_env = _backend_env_from_args(args)
    _validate_sqlserver_env(backend_env)
    _ensure_backend_python_dependencies(backend_python, sim_dir)

    grafana_password: str | None = None
    if getattr(args, "enable_monitoring", True) and getattr(args, "start_monitoring_stack", True):
        grafana_password = _start_monitoring_stack(args, backend_env)

    frontend_url = f"http://localhost:{frontend_port}"

    _print("[cyan]Starting SQL Server RCA Assistant...[/cyan]")
    _print(f"  Backend:  http://{backend_host}:{backend_port}")
    _print(f"  Frontend: {frontend_url}")
    _print(
        f"  SQL Server target: [green]{backend_env.get('SQLSERVER_HOST')}:{backend_env.get('SQLSERVER_PORT', '1433')}[/green]"
    )
    if backend_env.get("SIM_ENABLE_MONITORING") == "1":
        _print(
            f"  Monitoring backend: [green]{backend_env.get('CLICKHOUSE_HOST')}:{backend_env.get('CLICKHOUSE_PORT')}[/green]"
        )
        _print("  Grafana: [green]http://localhost:3001[/green] (user: admin)")
        if grafana_password:
            _print(f"  Grafana password: [green]{grafana_password}[/green]")
        elif args.grafana_password or os.environ.get("GRAFANA_PASSWORD"):
            _print("  Grafana password: [green]from GRAFANA_PASSWORD / --grafana-password[/green]")
        else:
            _print("  Grafana password: [yellow]set GRAFANA_PASSWORD or use --grafana-password[/yellow]")
    else:
        _print("  Monitoring backend: [yellow]disabled (direct SQL + Blitz only)[/yellow]")
    if repo_path:
        _print(f"  Code Analysis: [green]Enabled[/green] ({repo_path})")
    if backend_env.get("SIM_AUTO_INSTALL_BLITZ") == "1":
        _print("  Blitz install: [green]auto-install enabled[/green]")
    else:
        _print("  Blitz install: [yellow]manual[/yellow]")

    _print(f"\n[green]Open {frontend_url} in your browser[/green]")
    _print("[dim]Press Ctrl+C to stop[/dim]\n")

    frontend_dir = sim_dir / "webapp" / "frontend"

    processes: list[tuple[str, subprocess.Popen]] = []

    try:
        _ensure_frontend_dependencies(frontend_dir)

        backend_proc = subprocess.Popen(
            [
                str(backend_python),
                "-m",
                "uvicorn",
                "sim.webapp.backend.main:app",
                "--host",
                backend_host,
                "--port",
                str(backend_port),
            ],
            env=backend_env,
            cwd=sim_dir.parent,
        )
        processes.append(("Backend", backend_proc))

        health_url = f"http://{backend_host}:{backend_port}/health"
        backend_ready = False
        for _ in range(40):
            if backend_proc.poll() is not None:
                raise RuntimeError(f"Backend process exited early with code {backend_proc.returncode}")
            try:
                with urllib.request.urlopen(health_url, timeout=1.0) as resp:
                    if resp.status == 200:
                        backend_ready = True
                        break
            except Exception:
                pass
            time.sleep(0.25)

        if not backend_ready:
            raise RuntimeError(f"Backend did not become ready at {health_url}")

        frontend_env = _apply_backend_env(
            {
                "NEXT_PUBLIC_API_URL": f"http://{backend_host}:{backend_port}",
                "NEXT_PUBLIC_REPO_PATH": repo_path,
                "NEXT_PUBLIC_SQLSERVER_HOST": backend_env.get("SQLSERVER_HOST"),
                "NEXT_PUBLIC_SQLSERVER_PORT": backend_env.get("SQLSERVER_PORT", "1433"),
                "NEXT_PUBLIC_SQLSERVER_USER": backend_env.get("SQLSERVER_USER", "sa"),
                "NEXT_PUBLIC_SQLSERVER_DATABASE": backend_env.get("SQLSERVER_DATABASE", "master"),
                "NEXT_PUBLIC_ENABLE_MONITORING": backend_env.get("SIM_ENABLE_MONITORING", "0"),
                "NEXT_PUBLIC_CLICKHOUSE_HOST": backend_env.get("CLICKHOUSE_HOST"),
                "NEXT_PUBLIC_CLICKHOUSE_PORT": backend_env.get("CLICKHOUSE_PORT"),
                "NEXT_PUBLIC_CLICKHOUSE_DATABASE": backend_env.get("CLICKHOUSE_DATABASE"),
                "NEXT_PUBLIC_AUTO_INSTALL_BLITZ": backend_env.get("SIM_AUTO_INSTALL_BLITZ", "1"),
            }
        )

        frontend_proc = subprocess.Popen(
            ["npm", "run", "dev", "--", "-p", str(frontend_port)],
            env=frontend_env,
            cwd=frontend_dir,
        )
        processes.append(("Frontend", frontend_proc))

        def handle_shutdown(sig, frame):  # type: ignore[no-untyped-def]
            _print("\n[yellow]Shutting down...[/yellow]")
            for _, proc in processes:
                proc.terminate()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        while True:
            for name, proc in processes:
                ret = proc.poll()
                if ret is not None:
                    _print(f"[red]{name} process exited with code {ret}[/red]")
                    for _, running in processes:
                        if running.poll() is None:
                            running.terminate()
                    sys.exit(ret)
            time.sleep(1)

    except FileNotFoundError as e:
        missing = Path(str(getattr(e, "filename", "") or "")).name or "required executable"
        _print(f"[red]Error: Missing dependency '{missing}'.[/red]")
        _print(f"[dim]Details: {e}[/dim]")
        for _, proc in processes:
            if proc.poll() is None:
                proc.terminate()
        sys.exit(1)
    except Exception as e:
        _print(f"[red]Error: {e}[/red]")
        for _, proc in processes:
            if proc.poll() is None:
                proc.terminate()
        sys.exit(1)


def cmd_webapp_backend(args: argparse.Namespace) -> None:
    """Start only backend server."""
    sim_dir = Path(__file__).parent
    backend_python = _get_preferred_backend_python(sim_dir)
    if not _same_python_interpreter(backend_python, Path(sys.executable)):
        os.execv(str(backend_python), [str(backend_python), "-m", "sim", *sys.argv[1:]])

    _print("[cyan]Starting SQL Server RCA Assistant backend...[/cyan]")

    backend_env = _backend_env_from_args(args)
    _validate_sqlserver_env(backend_env)
    _ensure_backend_python_dependencies(backend_python, sim_dir)
    os.environ.update(backend_env)

    _print(f"  Server: http://{args.host}:{args.port}")
    _print(
        f"  SQL Server target: [green]{backend_env.get('SQLSERVER_HOST')}:{backend_env.get('SQLSERVER_PORT', '1433')}[/green]"
    )
    _print("[dim]Press Ctrl+C to stop[/dim]")

    try:
        import uvicorn

        uvicorn.run(
            "sim.webapp.backend.main:app",
            host=args.host,
            port=args.port,
            reload=False,
        )
    except ImportError:
        _print("[red]Error: uvicorn not installed. Run: pip install uvicorn[/red]")
        sys.exit(1)
    except Exception as e:
        _print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def cmd_webapp_frontend(args: argparse.Namespace) -> None:
    """Start only frontend server."""
    _print("[cyan]Starting SQL Server RCA Assistant frontend...[/cyan]")
    _print(f"  Server: http://localhost:{args.port}")
    _print("[dim]Press Ctrl+C to stop[/dim]")

    sim_dir = Path(__file__).parent
    frontend_dir = sim_dir / "webapp" / "frontend"

    frontend_env = _apply_backend_env(
        {
            "NEXT_PUBLIC_API_URL": args.api_url,
            "NEXT_PUBLIC_REPO_PATH": args.repo_path,
            "NEXT_PUBLIC_SQLSERVER_HOST": args.sqlserver_host,
            "NEXT_PUBLIC_SQLSERVER_PORT": str(args.sqlserver_port) if args.sqlserver_port is not None else None,
            "NEXT_PUBLIC_SQLSERVER_USER": args.sqlserver_user,
            "NEXT_PUBLIC_SQLSERVER_DATABASE": args.sqlserver_database,
            "NEXT_PUBLIC_ENABLE_MONITORING": "1" if args.enable_monitoring else "0",
            "NEXT_PUBLIC_CLICKHOUSE_HOST": args.clickhouse_host,
            "NEXT_PUBLIC_CLICKHOUSE_PORT": str(args.clickhouse_port) if args.clickhouse_port is not None else None,
            "NEXT_PUBLIC_CLICKHOUSE_DATABASE": args.clickhouse_database,
            "NEXT_PUBLIC_AUTO_INSTALL_BLITZ": "1" if args.auto_install_blitz else "0",
        }
    )

    _ensure_frontend_dependencies(frontend_dir)

    try:
        subprocess.run(
            ["npm", "run", "dev", "--", "-p", str(args.port)],
            env=frontend_env,
            cwd=frontend_dir,
            check=True,
        )
    except FileNotFoundError:
        _print("[red]Error: npm not found. Install Node.js/npm first.[/red]")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        _print("[red]Frontend server exited with error[/red]")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        _print("\n[yellow]Shutting down...[/yellow]")
        sys.exit(0)


def main() -> None:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m sim",
        description="SQL Server RCA Assistant (Stage 1)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose/debug output",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    webapp_parser = subparsers.add_parser(
        "webapp",
        help="Start the SQL Server RCA web app",
    )
    webapp_subparsers = webapp_parser.add_subparsers(dest="webapp_command")

    def add_sqlserver_options(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--sqlserver-host",
            type=str,
            default=os.environ.get("SQLSERVER_HOST"),
            help="SQL Server host (or set SQLSERVER_HOST)",
        )
        p.add_argument(
            "--sqlserver-port",
            type=int,
            default=int(os.environ.get("SQLSERVER_PORT", "1433")),
            help="SQL Server port (default: 1433)",
        )
        p.add_argument(
            "--sqlserver-user",
            type=str,
            default=os.environ.get("SQLSERVER_USER", "sa"),
            help="SQL Server username (default: sa)",
        )
        p.add_argument(
            "--sqlserver-database",
            type=str,
            default=os.environ.get("SQLSERVER_DATABASE", "master"),
            help="SQL Server database (default: master)",
        )
        p.add_argument(
            "--sqlserver-password",
            type=str,
            default=os.environ.get("SQLSERVER_PASSWORD") or os.environ.get("SA_PASSWORD") or os.environ.get("SIM_SA_PASSWORD"),
            help="SQL Server password",
        )

    def add_monitoring_options(p: argparse.ArgumentParser, default_enabled: bool = True) -> None:
        group = p.add_mutually_exclusive_group()
        group.add_argument(
            "--enable-monitoring",
            dest="enable_monitoring",
            action="store_true",
            default=default_enabled,
            help="Enable ClickHouse monitoring tools",
        )
        group.add_argument(
            "--no-monitoring",
            dest="enable_monitoring",
            action="store_false",
            help="Disable ClickHouse monitoring tools",
        )
        p.add_argument(
            "--clickhouse-host",
            type=str,
            default=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            help="ClickHouse host",
        )
        p.add_argument(
            "--clickhouse-port",
            type=int,
            default=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            help="ClickHouse HTTP port",
        )
        p.add_argument(
            "--clickhouse-database",
            type=str,
            default=os.environ.get("CLICKHOUSE_DATABASE", "rca_metrics"),
            help="ClickHouse database",
        )
        p.add_argument(
            "--clickhouse-user",
            type=str,
            default=os.environ.get("CLICKHOUSE_USER", "rca"),
            help="ClickHouse user",
        )
        p.add_argument(
            "--clickhouse-password",
            type=str,
            default=os.environ.get("CLICKHOUSE_PASSWORD", "rca_password"),
            help="ClickHouse password",
        )

    def add_blitz_options(p: argparse.ArgumentParser, default_enabled: bool = True) -> None:
        group = p.add_mutually_exclusive_group()
        group.add_argument(
            "--auto-install-blitz",
            dest="auto_install_blitz",
            action="store_true",
            default=default_enabled,
            help="Auto-install First Responder Kit scripts when missing",
        )
        group.add_argument(
            "--no-auto-install-blitz",
            dest="auto_install_blitz",
            action="store_false",
            help="Disable FRK auto-install",
        )

    webapp_start_parser = webapp_subparsers.add_parser(
        "start",
        help="Start both backend and frontend",
    )
    webapp_start_parser.add_argument(
        "--backend-port",
        type=int,
        default=8000,
        help="Backend port (default: 8000)",
    )
    webapp_start_parser.add_argument(
        "--frontend-port",
        type=int,
        default=3000,
        help="Frontend port (default: 3000)",
    )
    webapp_start_parser.add_argument(
        "--repo-path",
        type=str,
        default=None,
        help="Optional application repository path for code analysis tools",
    )
    start_stack_group = webapp_start_parser.add_mutually_exclusive_group()
    start_stack_group.add_argument(
        "--start-monitoring-stack",
        dest="start_monitoring_stack",
        action="store_true",
        default=True,
        help="Start bundled monitoring stack with docker compose (default)",
    )
    start_stack_group.add_argument(
        "--no-monitoring-stack",
        dest="start_monitoring_stack",
        action="store_false",
        help="Skip docker compose stack startup",
    )
    webapp_start_parser.add_argument(
        "--grafana-password",
        type=str,
        default=os.environ.get("GRAFANA_PASSWORD"),
        help="Grafana admin password for monitoring stack bootstrap",
    )
    add_sqlserver_options(webapp_start_parser)
    add_monitoring_options(webapp_start_parser, default_enabled=True)
    add_blitz_options(webapp_start_parser, default_enabled=True)
    webapp_start_parser.set_defaults(func=cmd_webapp_start)

    webapp_backend_parser = webapp_subparsers.add_parser(
        "backend",
        help="Start backend only",
    )
    webapp_backend_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Backend port (default: 8000)",
    )
    webapp_backend_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Backend host (default: 127.0.0.1)",
    )
    add_sqlserver_options(webapp_backend_parser)
    add_monitoring_options(webapp_backend_parser, default_enabled=True)
    add_blitz_options(webapp_backend_parser, default_enabled=True)
    webapp_backend_parser.set_defaults(func=cmd_webapp_backend)

    webapp_frontend_parser = webapp_subparsers.add_parser(
        "frontend",
        help="Start frontend only",
    )
    webapp_frontend_parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Frontend port (default: 3000)",
    )
    webapp_frontend_parser.add_argument(
        "--api-url",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_API_URL", "http://127.0.0.1:8000"),
        help="Backend API URL exposed to frontend",
    )
    webapp_frontend_parser.add_argument(
        "--repo-path",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_REPO_PATH"),
        help="Optional repo path for code analysis",
    )
    webapp_frontend_parser.add_argument(
        "--sqlserver-host",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_SQLSERVER_HOST"),
        help="Default SQL Server host shown to frontend session requests",
    )
    webapp_frontend_parser.add_argument(
        "--sqlserver-port",
        type=int,
        default=int(os.environ.get("NEXT_PUBLIC_SQLSERVER_PORT", "1433")),
        help="Default SQL Server port shown to frontend session requests",
    )
    webapp_frontend_parser.add_argument(
        "--sqlserver-user",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_SQLSERVER_USER", "sa"),
        help="Default SQL Server user shown to frontend session requests",
    )
    webapp_frontend_parser.add_argument(
        "--sqlserver-database",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_SQLSERVER_DATABASE", "master"),
        help="Default SQL Server database shown to frontend session requests",
    )
    frontend_monitor_group = webapp_frontend_parser.add_mutually_exclusive_group()
    frontend_monitor_group.add_argument(
        "--enable-monitoring",
        dest="enable_monitoring",
        action="store_true",
        default=True,
        help="Expose monitoring defaults to frontend (default: enabled)",
    )
    frontend_monitor_group.add_argument(
        "--no-monitoring",
        dest="enable_monitoring",
        action="store_false",
        help="Disable monitoring defaults in frontend",
    )
    frontend_blitz_group = webapp_frontend_parser.add_mutually_exclusive_group()
    frontend_blitz_group.add_argument(
        "--auto-install-blitz",
        dest="auto_install_blitz",
        action="store_true",
        default=True,
        help="Default frontend session behavior: auto-install Blitz",
    )
    frontend_blitz_group.add_argument(
        "--no-auto-install-blitz",
        dest="auto_install_blitz",
        action="store_false",
        help="Disable FRK auto-install as frontend default",
    )
    webapp_frontend_parser.add_argument(
        "--clickhouse-host",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_CLICKHOUSE_HOST"),
        help="Default ClickHouse host shown to frontend session requests",
    )
    webapp_frontend_parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(os.environ.get("NEXT_PUBLIC_CLICKHOUSE_PORT", "8123")),
        help="Default ClickHouse port shown to frontend session requests",
    )
    webapp_frontend_parser.add_argument(
        "--clickhouse-database",
        type=str,
        default=os.environ.get("NEXT_PUBLIC_CLICKHOUSE_DATABASE", "rca_metrics"),
        help="Default ClickHouse database shown to frontend session requests",
    )
    webapp_frontend_parser.set_defaults(func=cmd_webapp_frontend)

    # Keep RCA subcommand for backward-compatible non-web tooling.
    from sim.rca.cli import add_rca_subparsers

    add_rca_subparsers(subparsers)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    configure_logging(verbose=args.verbose, json_format=args.json_logs)

    try:
        args.func(args)
    except ValueError as e:
        _print(f"[red]Error: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
