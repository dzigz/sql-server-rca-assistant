"""
Session API endpoints.
"""

from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from sim.webapp.backend.session_manager import session_manager

router = APIRouter()


class CreateSessionRequest(BaseModel):
    """Request body for creating a session."""
    repo_path: Optional[str] = None  # Path to application repo for code analysis
    sqlserver_host: Optional[str] = None
    sqlserver_port: int = 1433
    sqlserver_user: str = "sa"
    sqlserver_password: Optional[str] = None
    sqlserver_database: str = "master"
    sqlserver_container: Optional[str] = None
    enable_monitoring: bool = True
    auto_install_blitz: bool = True
    clickhouse_host: Optional[str] = None
    clickhouse_port: Optional[int] = None
    clickhouse_database: Optional[str] = None
    clickhouse_user: Optional[str] = None
    clickhouse_password: Optional[str] = None


class SessionResponse(BaseModel):
    """Response for session operations."""
    session_id: str
    created_at: str
    message_count: int


class SessionSummaryResponse(BaseModel):
    """Lightweight session summary for history list views."""
    session_id: str
    created_at: str
    message_count: int
    title: str
    last_message_at: str


class BlitzInstallRequest(BaseModel):
    """Request body for explicit FRK installation confirmation."""
    confirm: bool = False


@router.post("/create", response_model=SessionResponse)
async def create_session(request: CreateSessionRequest):
    """
    Create a new chat session.

    This initializes a new AgentRCAEngine with SQL Server diagnostics.
    ClickHouse monitoring tools are optional and controlled by enable_monitoring.
    """
    try:
        session = session_manager.create_session(
            repo_path=request.repo_path,
            sqlserver_host=request.sqlserver_host,
            sqlserver_port=request.sqlserver_port,
            sqlserver_user=request.sqlserver_user,
            sqlserver_password=request.sqlserver_password,
            sqlserver_database=request.sqlserver_database,
            sqlserver_container=request.sqlserver_container,
            enable_monitoring=request.enable_monitoring,
            auto_install_blitz=request.auto_install_blitz,
            clickhouse_host=request.clickhouse_host,
            clickhouse_port=request.clickhouse_port,
            clickhouse_database=request.clickhouse_database,
            clickhouse_user=request.clickhouse_user,
            clickhouse_password=request.clickhouse_password,
        )
        return SessionResponse(
            session_id=session.session_id,
            created_at=session.created_at.isoformat(),
            message_count=len(session.message_history),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create session: {str(e)}")


@router.get("/summaries")
async def list_session_summaries():
    """List active sessions with lightweight metadata."""
    sessions = session_manager.list_session_summaries()
    return {"sessions": [SessionSummaryResponse(**s) for s in sessions]}


@router.get("/{session_id}", response_model=SessionResponse)
async def get_session(session_id: str):
    """Get session info by ID."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return SessionResponse(
        session_id=session.session_id,
        created_at=session.created_at.isoformat(),
        message_count=len(session.message_history),
    )


@router.get("/{session_id}/history")
async def get_session_history(session_id: str):
    """Get message history for a session."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"messages": session.message_history}


@router.delete("/{session_id}")
async def delete_session(session_id: str):
    """Delete a session."""
    if session_manager.delete_session(session_id):
        return {"status": "deleted", "session_id": session_id}
    raise HTTPException(status_code=404, detail="Session not found")


@router.get("/")
async def list_sessions():
    """List all active sessions."""
    sessions = session_manager.list_sessions()
    return {"sessions": sessions}


@router.post("/{session_id}/blitz/install")
async def install_blitz_scripts(session_id: str, request: BlitzInstallRequest):
    """Install First Responder Kit scripts for the session's direct SQL target."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if not request.confirm:
        raise HTTPException(
            status_code=400,
            detail="Installation requires explicit confirmation (confirm=true).",
        )

    tool = session_manager.get_tool(session_id, "run_sp_blitz")
    if not tool or not hasattr(tool, "install_first_responder_kit_direct"):
        raise HTTPException(
            status_code=400,
            detail="run_sp_blitz tool is not available for direct SQL installation.",
        )

    try:
        result = tool.install_first_responder_kit_direct(target_database="master")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to install FRK scripts: {str(e)}")

    session_manager.update_metadata(
        session_id,
        {
            "blitz_install_declined": False,
            "blitz_install_installed": True,
            "blitz_install_installed_at": datetime.now().isoformat(),
        },
    )
    if hasattr(tool, "set_install_offer_enabled"):
        tool.set_install_offer_enabled(True)

    return {
        "status": "installed",
        "message": "First Responder Kit scripts were installed successfully in master.",
        "data": result,
    }


@router.post("/{session_id}/blitz/decline")
async def decline_blitz_install(session_id: str):
    """Record user decline for FRK installation prompts in this session."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    session_manager.update_metadata(
        session_id,
        {
            "blitz_install_declined": True,
            "blitz_install_declined_at": datetime.now().isoformat(),
        },
    )

    tool = session_manager.get_tool(session_id, "run_sp_blitz")
    if tool and hasattr(tool, "set_install_offer_enabled"):
        tool.set_install_offer_enabled(False)

    return {
        "status": "declined",
        "message": "Installation prompt declined for this session.",
    }
