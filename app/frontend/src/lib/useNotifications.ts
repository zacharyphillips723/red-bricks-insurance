/**
 * WebSocket hook for real-time alert notifications.
 *
 * Connects to /ws/notifications and dispatches events for:
 * - alert_assigned: A care manager claimed an alert
 * - alert_status_changed: An alert's status was updated
 * - new_alert: A new alert was created (from ADT feed or other source)
 */

import { useEffect, useRef, useCallback, useState } from "react";

export interface Notification {
  id: string;
  type: string;
  data: Record<string, string>;
  timestamp: string;
}

interface UseNotificationsOptions {
  onNotification?: (notification: Notification) => void;
}

export function useNotifications({ onNotification }: UseNotificationsOptions = {}) {
  const wsRef = useRef<WebSocket | null>(null);
  const [connected, setConnected] = useState(false);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/notifications`);

    ws.onopen = () => {
      setConnected(true);
      console.log("[WS] Connected to notifications");
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        const notification: Notification = {
          id: `${msg.type}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          type: msg.type,
          data: msg.data,
          timestamp: msg.timestamp,
        };
        setNotifications((prev) => [notification, ...prev].slice(0, 50));
        onNotification?.(notification);
      } catch {
        // ignore non-JSON messages
      }
    };

    ws.onclose = () => {
      setConnected(false);
      // Reconnect after 5 seconds
      reconnectTimer.current = setTimeout(connect, 5000);
    };

    ws.onerror = () => {
      ws.close();
    };

    wsRef.current = ws;
  }, [onNotification]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  const dismissNotification = useCallback((id: string) => {
    setNotifications((prev) => prev.filter((n) => n.id !== id));
  }, []);

  return { connected, notifications, dismissNotification };
}
