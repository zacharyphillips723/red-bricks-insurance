"""Context enrichment layer — optional Slack, Glean, and Salesforce integrations.

Each integration returns a dict or None. If credentials are missing or the call
fails, the function returns None and the agent works with Databricks data alone.
"""

import os
import traceback
from typing import Optional


# ---------------------------------------------------------------------------
# Slack — recent account channel messages
# ---------------------------------------------------------------------------

def get_slack_context(group_name: str) -> Optional[dict]:
    """Search Slack for recent messages in the group's account channel.

    Looks for channels matching #grp-{name} or #renewal-{name} patterns.
    Returns last 30 days of messages with threaded replies.
    """
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None

    try:
        from slack_sdk import WebClient
        from datetime import datetime, timedelta

        client = WebClient(token=token)
        # Normalize group name for channel search
        search_name = group_name.lower().replace(" ", "-").replace("_", "-")

        # Search for matching channels
        channels_resp = client.conversations_list(types="public_channel,private_channel", limit=200)
        matching_channels = []
        for ch in channels_resp.get("channels", []):
            ch_name = ch.get("name", "").lower()
            if search_name in ch_name or any(
                prefix in ch_name for prefix in [f"grp-{search_name}", f"renewal-{search_name}"]
            ):
                matching_channels.append(ch)

        if not matching_channels:
            return None

        # Get messages from the last 30 days
        oldest = str(int((datetime.now() - timedelta(days=30)).timestamp()))
        all_messages = []
        for ch in matching_channels[:3]:
            history = client.conversations_history(
                channel=ch["id"], oldest=oldest, limit=50,
            )
            for msg in history.get("messages", []):
                all_messages.append({
                    "channel": ch.get("name"),
                    "text": msg.get("text", ""),
                    "user": msg.get("user", "unknown"),
                    "ts": msg.get("ts"),
                })

        if not all_messages:
            return None

        return {
            "source": "slack",
            "channels": [ch["name"] for ch in matching_channels[:3]],
            "message_count": len(all_messages),
            "messages": all_messages[:30],
        }
    except ImportError:
        print("[Enrichment] slack_sdk not installed — Slack integration disabled")
        return None
    except Exception as e:
        print(f"[Enrichment] Slack error: {e}")
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Glean — internal knowledge search
# ---------------------------------------------------------------------------

def get_glean_context(group_name: str, industry: str = "") -> Optional[dict]:
    """Search Glean for relevant internal docs: playbooks, competitive intel, case studies.

    Runs 2-3 targeted queries and returns top document snippets with source attribution.
    """
    api_token = os.environ.get("GLEAN_API_TOKEN", "").strip()
    api_url = os.environ.get("GLEAN_API_URL", "").strip()
    if not api_token or not api_url:
        return None

    try:
        import json
        from urllib.request import Request, urlopen

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

        queries = [
            f'"{group_name}" renewal',
            f'"{industry}" competitive intel' if industry else "renewal playbook",
            "stop loss renewal playbook",
        ]

        all_results = []
        for query in queries:
            req = Request(
                f"{api_url}/api/v1/search",
                data=json.dumps({"query": query, "pageSize": 3}).encode(),
                headers=headers,
                method="POST",
            )
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                for result in data.get("results", []):
                    doc = result.get("document", {})
                    all_results.append({
                        "title": doc.get("title", "Untitled"),
                        "url": doc.get("url", ""),
                        "snippet": result.get("snippets", [{}])[0].get("snippet", "")[:500],
                        "source": doc.get("datasource", "unknown"),
                    })

        if not all_results:
            return None

        return {
            "source": "glean",
            "document_count": len(all_results),
            "documents": all_results[:5],
        }
    except ImportError:
        print("[Enrichment] urllib not available — Glean integration disabled")
        return None
    except Exception as e:
        print(f"[Enrichment] Glean error: {e}")
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Salesforce — CRM account data
# ---------------------------------------------------------------------------

def get_salesforce_context(group_name: str) -> Optional[dict]:
    """Query Salesforce for account, opportunity, and case data.

    Returns deal stage, contract end date, account health, and open issues.
    """
    instance_url = os.environ.get("SF_INSTANCE_URL", "").strip()
    client_id = os.environ.get("SF_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SF_CLIENT_SECRET", "").strip()
    username = os.environ.get("SF_USERNAME", "").strip()
    password = os.environ.get("SF_PASSWORD", "").strip()

    if not all([instance_url, client_id, client_secret, username, password]):
        return None

    try:
        from simple_salesforce import Salesforce

        sf = Salesforce(
            instance_url=instance_url,
            username=username,
            password=password,
            consumer_key=client_id,
            consumer_secret=client_secret,
        )

        # Find matching account
        safe_name = group_name.replace("'", "\\'")
        account_result = sf.query(
            f"SELECT Id, Name, Industry, AccountNumber, Rating, "
            f"LastActivityDate, Description "
            f"FROM Account "
            f"WHERE Name LIKE '%{safe_name}%' LIMIT 1"
        )

        if not account_result.get("records"):
            return None

        account = account_result["records"][0]
        account_id = account["Id"]

        # Open opportunities (renewals)
        opp_result = sf.query(
            f"SELECT Id, Name, StageName, Amount, CloseDate, Probability "
            f"FROM Opportunity "
            f"WHERE AccountId = '{account_id}' AND IsClosed = false "
            f"ORDER BY CloseDate ASC LIMIT 3"
        )

        # Open cases
        case_result = sf.query(
            f"SELECT COUNT(Id) cnt "
            f"FROM Case "
            f"WHERE AccountId = '{account_id}' AND IsClosed = false"
        )

        return {
            "source": "salesforce",
            "account": {
                "name": account.get("Name"),
                "industry": account.get("Industry"),
                "rating": account.get("Rating"),
                "last_activity": account.get("LastActivityDate"),
            },
            "open_opportunities": [
                {
                    "name": opp.get("Name"),
                    "stage": opp.get("StageName"),
                    "amount": opp.get("Amount"),
                    "close_date": opp.get("CloseDate"),
                }
                for opp in opp_result.get("records", [])
            ],
            "open_cases_count": case_result["records"][0]["cnt"] if case_result.get("records") else 0,
        }
    except ImportError:
        print("[Enrichment] simple_salesforce not installed — Salesforce integration disabled")
        return None
    except Exception as e:
        print(f"[Enrichment] Salesforce error: {e}")
        traceback.print_exc()
        return None
