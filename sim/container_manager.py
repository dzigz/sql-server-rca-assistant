"""Container runtime abstraction for Podman/Docker."""

import shutil
import subprocess
from typing import Optional
from dataclasses import dataclass

from sim.exceptions import ContainerRuntimeError, ContainerCommandError
from sim.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class Mount:
    """Container mount specification."""
    host_path: str
    container_path: str
    mode: str = "rw"  # "ro" or "rw"
    
    def to_arg(self) -> str:
        """Convert to -v argument format."""
        return f"{self.host_path}:{self.container_path}:{self.mode}"


class ContainerManager:
    """
    Container runtime abstraction layer.
    
    Supports both Podman and Docker with Podman preferred.
    """
    
    def __init__(self):
        """Initialize and detect available container runtime."""
        self.runtime = self._detect_runtime()
        logger.info("Using container runtime: %s", self.runtime)
    
    def _detect_runtime(self) -> str:
        """
        Detect available container runtime.
        
        Returns:
            'podman' or 'docker'
            
        Raises:
            ContainerRuntimeError: If no runtime is available
        """
        # Prefer Docker
        if shutil.which("docker"):
            return "docker"

        # Fall back to Podman
        if shutil.which("podman"):
            return "podman"
        
        raise ContainerRuntimeError(
            "No container runtime found!\n"
            "Please install Podman or Docker:\n"
            "  macOS:  brew install podman && podman machine init && podman machine start\n"
            "  Linux:  sudo apt install podman  (or docker.io)\n"
        )
    
    def _run_cmd(
        self, 
        args: list[str], 
        check: bool = True,
        capture_output: bool = True,
        timeout: Optional[int] = None
    ) -> subprocess.CompletedProcess:
        """
        Run a container runtime command.
        
        Args:
            args: Command arguments (without runtime prefix)
            check: Raise exception on non-zero exit
            capture_output: Capture stdout/stderr
            timeout: Command timeout in seconds
            
        Returns:
            CompletedProcess result
        """
        cmd = [self.runtime] + args
        try:
            result = subprocess.run(
                cmd,
                check=check,
                capture_output=capture_output,
                text=True,
                timeout=timeout
            )
            return result
        except subprocess.CalledProcessError as e:
            raise ContainerCommandError(
                f"Command failed: {' '.join(cmd)}\n"
                f"Exit code: {e.returncode}\n"
                f"Stderr: {e.stderr}"
            ) from e
        except subprocess.TimeoutExpired as e:
            raise ContainerCommandError(f"Command timed out: {' '.join(cmd)}") from e
    
    def pull(self, image: str) -> None:
        """
        Pull a container image.
        
        Args:
            image: Image name with tag
        """
        logger.info("Pulling image: %s", image)
        # Don't capture output so user sees progress
        subprocess.run(
            [self.runtime, "pull", image],
            check=True
        )
        logger.info("Image pulled: %s", image)
    
    def run(
        self,
        name: str,
        image: str,
        ports: Optional[list[tuple[int, int]]] = None,
        env: Optional[dict[str, str]] = None,
        mounts: Optional[list[Mount]] = None,
        cpu_limit: Optional[str] = None,
        mem_limit: Optional[str] = None,
        detach: bool = True
    ) -> str:
        """
        Run a new container.
        
        Args:
            name: Container name
            image: Image to run
            ports: List of (host_port, container_port) tuples
            env: Environment variables
            mounts: Volume mounts
            cpu_limit: CPU limit (e.g., "2.0")
            mem_limit: Memory limit (e.g., "4g")
            detach: Run in background
            
        Returns:
            Container ID
        """
        args = ["run"]
        
        if detach:
            args.append("-d")
        
        args.extend(["--name", name])
        
        # Port mappings
        if ports:
            for host_port, container_port in ports:
                args.extend(["-p", f"{host_port}:{container_port}"])
        
        # Environment variables
        if env:
            for key, value in env.items():
                args.extend(["-e", f"{key}={value}"])
        
        # Volume mounts
        if mounts:
            for mount in mounts:
                args.extend(["-v", mount.to_arg()])
        
        # Resource limits (same flags for both Podman and Docker)
        if cpu_limit:
            args.extend(["--cpus", cpu_limit])
        
        if mem_limit:
            args.extend(["--memory", mem_limit])
        
        args.append(image)
        
        result = self._run_cmd(args)
        container_id: str = result.stdout.strip()

        logger.info("Container started: %s", name)
        return container_id
    
    def exec(
        self, 
        name: str, 
        cmd: list[str],
        timeout: Optional[int] = None
    ) -> tuple[int, str, str]:
        """
        Execute a command in a running container.
        
        Args:
            name: Container name
            cmd: Command to execute
            timeout: Command timeout
            
        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        args = ["exec", name] + cmd
        result = self._run_cmd(args, check=False, timeout=timeout)
        return result.returncode, result.stdout, result.stderr
    
    def logs(self, name: str, tail: Optional[int] = None) -> str:
        """
        Get container logs.
        
        Args:
            name: Container name
            tail: Number of lines from the end
            
        Returns:
            Log output
        """
        args = ["logs"]
        if tail:
            args.extend(["--tail", str(tail)])
        args.append(name)
        
        result = self._run_cmd(args, check=False)
        output: str = result.stdout + result.stderr
        return output
    
    def stop(self, name: str, timeout: int = 10) -> None:
        """
        Stop a running container.
        
        Args:
            name: Container name
            timeout: Seconds to wait before killing
        """
        args = ["stop", "-t", str(timeout), name]
        self._run_cmd(args, check=False)
        logger.info("Stopped container: %s", name)
    
    def remove(self, name: str, force: bool = False, volumes: bool = True) -> None:
        """
        Remove a container.
        
        Args:
            name: Container name
            force: Force removal even if running
            volumes: Also remove associated volumes
        """
        args = ["rm"]
        if force:
            args.append("-f")
        if volumes:
            args.append("-v")
        args.append(name)
        
        self._run_cmd(args, check=False)
        logger.info("Removed container: %s", name)
    
    def is_running(self, name: str) -> bool:
        """
        Check if a container is running.
        
        Args:
            name: Container name
            
        Returns:
            True if container is running
        """
        args = ["ps", "-q", "-f", f"name=^{name}$"]
        result = self._run_cmd(args, check=False)
        return bool(result.stdout.strip())
    
    def exists(self, name: str) -> bool:
        """
        Check if a container exists (running or stopped).
        
        Args:
            name: Container name
            
        Returns:
            True if container exists
        """
        args = ["ps", "-a", "-q", "-f", f"name=^{name}$"]
        result = self._run_cmd(args, check=False)
        return bool(result.stdout.strip())
    
    def inspect(self, name: str) -> dict:
        """
        Inspect a container.
        
        Args:
            name: Container name
            
        Returns:
            Container inspection data
        """
        import json
        args = ["inspect", name]
        result = self._run_cmd(args)
        data = json.loads(result.stdout)
        return data[0] if data else {}


# Re-export exceptions for backwards compatibility
__all__ = [
    "Mount",
    "ContainerManager",
    "ContainerRuntimeError",
    "ContainerCommandError",
]

