/**
 * Toast notification overlay for real-time alerts.
 * Renders in the top-right corner with auto-dismiss after 8 seconds.
 */

import { useEffect, useState } from "react";
import { X, Bell, UserCheck, RefreshCw, AlertTriangle } from "lucide-react";
import type { Notification } from "@/lib/useNotifications";

interface ToastNotificationsProps {
  notifications: Notification[];
  onDismiss: (id: string) => void;
  onClickAlert?: (alertId: string) => void;
}

function formatNotification(n: Notification): {
  title: string;
  body: string;
  icon: React.ReactNode;
  color: string;
} {
  switch (n.type) {
    case "alert_assigned":
      return {
        title: "Alert Assigned",
        body: `${n.data.care_manager_name || "A care manager"} claimed alert for member ${n.data.member_id}`,
        icon: <UserCheck className="w-5 h-5" />,
        color: "border-blue-400 bg-blue-50",
      };
    case "alert_status_changed":
      return {
        title: "Status Updated",
        body: `Alert ${n.data.alert_id?.slice(0, 8)}… moved from ${n.data.old_status} to ${n.data.new_status}`,
        icon: <RefreshCw className="w-5 h-5" />,
        color: "border-amber-400 bg-amber-50",
      };
    case "new_alert":
      return {
        title: `New ${n.data.risk_tier || ""} Alert`,
        body: n.data.primary_driver || `New alert for member ${n.data.member_id}`,
        icon: <AlertTriangle className="w-5 h-5" />,
        color: n.data.risk_tier === "Critical"
          ? "border-red-500 bg-red-50"
          : "border-orange-400 bg-orange-50",
      };
    default:
      return {
        title: "Notification",
        body: JSON.stringify(n.data),
        icon: <Bell className="w-5 h-5" />,
        color: "border-gray-400 bg-gray-50",
      };
  }
}

function Toast({
  notification,
  onDismiss,
  onClickAlert,
}: {
  notification: Notification;
  onDismiss: () => void;
  onClickAlert?: (alertId: string) => void;
}) {
  const [exiting, setExiting] = useState(false);
  const { title, body, icon, color } = formatNotification(notification);

  useEffect(() => {
    const timer = setTimeout(() => {
      setExiting(true);
      setTimeout(onDismiss, 300);
    }, 8000);
    return () => clearTimeout(timer);
  }, [onDismiss]);

  return (
    <div
      className={`flex items-start gap-3 p-4 rounded-lg border-l-4 shadow-lg max-w-sm cursor-pointer
        transition-all duration-300 ${color}
        ${exiting ? "opacity-0 translate-x-4" : "opacity-100 translate-x-0"}`}
      onClick={() => {
        const alertId = notification.data.alert_id;
        if (alertId && onClickAlert) onClickAlert(alertId);
      }}
    >
      <div className="text-gray-600 mt-0.5">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-semibold text-gray-900">{title}</p>
        <p className="text-xs text-gray-600 mt-0.5 truncate">{body}</p>
        <p className="text-[10px] text-gray-400 mt-1">
          {new Date(notification.timestamp).toLocaleTimeString()}
        </p>
      </div>
      <button
        onClick={(e) => {
          e.stopPropagation();
          onDismiss();
        }}
        className="text-gray-400 hover:text-gray-600"
      >
        <X className="w-4 h-4" />
      </button>
    </div>
  );
}

export function ToastNotifications({
  notifications,
  onDismiss,
  onClickAlert,
}: ToastNotificationsProps) {
  // Show only the 3 most recent
  const visible = notifications.slice(0, 3);

  if (visible.length === 0) return null;

  return (
    <div className="fixed top-4 right-4 z-50 flex flex-col gap-2">
      {visible.map((n) => (
        <Toast
          key={n.id}
          notification={n}
          onDismiss={() => onDismiss(n.id)}
          onClickAlert={onClickAlert}
        />
      ))}
    </div>
  );
}
