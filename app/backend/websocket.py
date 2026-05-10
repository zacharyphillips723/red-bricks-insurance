"""WebSocket manager for real-time notifications.

Broadcasts alert events (new alerts, status changes, assignments) to all
connected clients so the Population Health Command Center updates in real-time
across multiple care manager sessions.
"""

import asyncio
import json
from datetime import datetime
from typing import Any

from fastapi import WebSocket


class NotificationManager:
    """Manages WebSocket connections and broadcasts notifications."""

    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._connections.append(websocket)
        print(f"[WS] Client connected. Total: {len(self._connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)
        print(f"[WS] Client disconnected. Total: {len(self._connections)}")

    async def broadcast(self, event_type: str, data: dict[str, Any]):
        """Broadcast a notification to all connected clients."""
        message = json.dumps({
            "type": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })
        async with self._lock:
            stale = []
            for ws in self._connections:
                try:
                    await ws.send_text(message)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self._connections.remove(ws)

    @property
    def connection_count(self) -> int:
        return len(self._connections)


# Singleton instance
notifications = NotificationManager()
