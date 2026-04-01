"""
FastAPI Backend for Database RCA Chat.

Provides endpoints for:
- Session management (create, get, delete)
- Chat with SSE streaming
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sim.logging_config import configure_logging
from sim.webapp.backend.api import chat, session, grafana

# Configure logging for the backend
configure_logging(verbose=True, use_color=True)

# Create FastAPI app
app = FastAPI(
    title="SQL Server DBA Assistant",
    description="AI-powered database performance analysis and health check",
    version="1.0.0",
)

# Configure CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(session.router, prefix="/api/session", tags=["session"])
app.include_router(chat.router, prefix="/api/chat", tags=["chat"])
app.include_router(grafana.router, prefix="/api/grafana", tags=["grafana"])


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "name": "SQL Server DBA Assistant API",
        "version": "1.0.0",
        "endpoints": {
            "session": "/api/session",
            "chat": "/api/chat",
            "grafana": "/api/grafana",
        },
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


def run_server(host: str = "0.0.0.0", port: int = 8000):
    """Run the FastAPI server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
