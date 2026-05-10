"""User identity middleware for Databricks Apps.

Databricks Apps inject the authenticated user's identity via request headers.
This module extracts it and makes it available to route handlers via a
FastAPI dependency.
"""

import os
from dataclasses import dataclass

from fastapi import Request


@dataclass
class UserIdentity:
    email: str
    display_name: str


_FALLBACK_EMAIL = "anonymous@redbricks.local"


def get_current_user(request: Request) -> UserIdentity:
    """Extract user identity from Databricks Apps request headers.

    In Databricks Apps, the platform injects:
      - X-Forwarded-Email: user's email address
      - X-Forwarded-Preferred-Username: user's display name or username

    Locally, falls back to DATABRICKS_USER env var or anonymous.
    """
    email = (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-Preferred-Username")
        or os.environ.get("DATABRICKS_USER")
        or _FALLBACK_EMAIL
    )
    # Display name: try the preferred username header, else derive from email
    display_name = (
        request.headers.get("X-Forwarded-Preferred-Username")
        or email.split("@")[0].replace(".", " ").title()
    )
    return UserIdentity(email=email, display_name=display_name)
