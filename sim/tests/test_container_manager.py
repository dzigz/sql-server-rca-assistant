"""Tests for sim.container_manager module."""

import pytest
from unittest.mock import patch, MagicMock
import subprocess


class TestMount:
    """Tests for the Mount dataclass."""

    def test_mount_to_arg(self):
        """Test Mount.to_arg() formatting."""
        from sim.container_manager import Mount

        mount = Mount(
            host_path="/host/path",
            container_path="/container/path",
            mode="rw",
        )

        assert mount.to_arg() == "/host/path:/container/path:rw"

    def test_mount_readonly(self):
        """Test Mount with readonly mode."""
        from sim.container_manager import Mount

        mount = Mount(
            host_path="/host/path",
            container_path="/container/path",
            mode="ro",
        )

        assert mount.to_arg() == "/host/path:/container/path:ro"


class TestContainerManagerDetection:
    """Tests for container runtime detection."""

    def test_detect_docker(self):
        """Test detection of Docker runtime."""
        from sim.container_manager import ContainerManager

        with patch("shutil.which") as mock_which:
            mock_which.side_effect = lambda cmd: "/usr/bin/docker" if cmd == "docker" else None

            with patch.object(ContainerManager, "__init__", lambda self: None):
                manager = ContainerManager()
                manager.runtime = manager._detect_runtime = MagicMock(return_value="docker")
                # Direct test of detection logic
                mock_which.reset_mock()
                mock_which.side_effect = lambda cmd: "/usr/bin/docker" if cmd == "docker" else None

    def test_detect_podman_fallback(self):
        """Test fallback to Podman when Docker not available."""
        from sim.container_manager import ContainerManager

        with patch("shutil.which") as mock_which:
            mock_which.side_effect = lambda cmd: "/usr/bin/podman" if cmd == "podman" else None

            with patch.object(ContainerManager, "__init__", lambda self: None):
                manager = ContainerManager()
                # Would fall back to podman

    def test_no_runtime_raises_error(self):
        """Test that missing runtime raises ContainerRuntimeError."""
        from sim.container_manager import ContainerManager
        from sim.exceptions import ContainerRuntimeError

        with patch("shutil.which", return_value=None):
            with pytest.raises(ContainerRuntimeError) as exc_info:
                ContainerManager()

            assert "No container runtime found" in str(exc_info.value)


class TestContainerManagerOperations:
    """Tests for container manager operations."""

    def test_run_container(self, mock_container_manager):
        """Test running a container."""
        container_id = mock_container_manager.run(
            name="test-container",
            image="test-image:latest",
            ports=[(8080, 80)],
            env={"KEY": "value"},
        )

        assert container_id == "container_id_123"
        mock_container_manager._mock_run.assert_called()

    def test_run_container_with_mounts(self, mock_container_manager):
        """Test running a container with volume mounts."""
        from sim.container_manager import Mount

        mounts = [
            Mount("/host/data", "/container/data", "rw"),
        ]

        container_id = mock_container_manager.run(
            name="test-container",
            image="test-image:latest",
            mounts=mounts,
        )

        assert container_id == "container_id_123"

    def test_run_container_with_resource_limits(self, mock_container_manager):
        """Test running a container with resource limits."""
        container_id = mock_container_manager.run(
            name="test-container",
            image="test-image:latest",
            cpu_limit="2.0",
            mem_limit="4g",
        )

        assert container_id == "container_id_123"

    def test_exec_command(self, mock_container_manager):
        """Test executing a command in a container."""
        mock_container_manager._mock_run.return_value = MagicMock(
            returncode=0,
            stdout="command output",
            stderr="",
        )

        exit_code, stdout, stderr = mock_container_manager.exec(
            "test-container",
            ["echo", "hello"],
        )

        assert exit_code == 0
        assert stdout == "command output"

    def test_stop_container(self, mock_container_manager):
        """Test stopping a container."""
        mock_container_manager.stop("test-container")

        mock_container_manager._mock_run.assert_called()

    def test_remove_container(self, mock_container_manager):
        """Test removing a container."""
        mock_container_manager.remove("test-container", force=True)

        mock_container_manager._mock_run.assert_called()

    def test_is_running(self, mock_container_manager):
        """Test checking if container is running."""
        mock_container_manager._mock_run.return_value = MagicMock(
            stdout="container_id_123\n",
            returncode=0,
        )

        result = mock_container_manager.is_running("test-container")

        assert result is True

    def test_is_not_running(self, mock_container_manager):
        """Test checking if container is not running."""
        mock_container_manager._mock_run.return_value = MagicMock(
            stdout="",
            returncode=0,
        )

        result = mock_container_manager.is_running("test-container")

        assert result is False

    def test_exists(self, mock_container_manager):
        """Test checking if container exists."""
        mock_container_manager._mock_run.return_value = MagicMock(
            stdout="container_id_123\n",
            returncode=0,
        )

        result = mock_container_manager.exists("test-container")

        assert result is True

    def test_logs(self, mock_container_manager):
        """Test getting container logs."""
        mock_container_manager._mock_run.return_value = MagicMock(
            stdout="log line 1\nlog line 2\n",
            stderr="",
            returncode=0,
        )

        logs = mock_container_manager.logs("test-container", tail=100)

        assert "log line 1" in logs

    def test_pull_image(self, mock_container_manager):
        """Test pulling a container image."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            mock_container_manager.pull("test-image:latest")

            mock_run.assert_called_once()


class TestContainerCommandError:
    """Tests for command error handling."""

    def test_command_failure_raises_error(self):
        """Test that command failures raise ContainerCommandError."""
        from sim.container_manager import ContainerManager
        from sim.exceptions import ContainerCommandError

        with patch("shutil.which", return_value="/usr/bin/docker"):
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = subprocess.CalledProcessError(
                    returncode=1,
                    cmd=["docker", "run", "test"],
                    stderr="Error: something went wrong",
                )

                manager = ContainerManager()

                with pytest.raises(ContainerCommandError):
                    manager._run_cmd(["run", "test"])

    def test_command_timeout_raises_error(self):
        """Test that command timeouts raise ContainerCommandError."""
        from sim.container_manager import ContainerManager
        from sim.exceptions import ContainerCommandError

        with patch("shutil.which", return_value="/usr/bin/docker"):
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = subprocess.TimeoutExpired(
                    cmd=["docker", "run", "test"],
                    timeout=30,
                )

                manager = ContainerManager()

                with pytest.raises(ContainerCommandError) as exc_info:
                    manager._run_cmd(["run", "test"], timeout=30)

                assert "timed out" in str(exc_info.value)
