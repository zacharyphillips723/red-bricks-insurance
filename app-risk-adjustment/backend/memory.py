"""Lakebase (Postgres) persistence for the Coder Workbench.

Backs the adjudication audit trail, chat history (conversations / conversation_messages),
and the agent's user-controllable memory (agent_user_prefs, agent_lessons). Every function
degrades gracefully: if Lakebase is unavailable it returns a clear, non-raising result so the
API never 500s on a missing resource.
"""
import json
import uuid

from . import config

# ---- table bootstrap (self-heal: chat tables may not pre-exist on a fresh instance) ----
_CONV_DDL = [
    """CREATE TABLE IF NOT EXISTS conversations (
         conversation_id TEXT PRIMARY KEY, member_id TEXT, user_email TEXT,
         title TEXT, model_endpoint TEXT,
         created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS conversation_messages (
         message_id TEXT PRIMARY KEY, conversation_id TEXT, role TEXT, content TEXT,
         metadata JSONB, created_at TIMESTAMPTZ DEFAULT now())""",
    "CREATE INDEX IF NOT EXISTS idx_conv_msg_cid ON conversation_messages(conversation_id)",
]
_bootstrapped = False


def _conn():
    try:
        import psycopg2
        w = config.workspace()
        inst = w.database.get_database_instance(name=config.LAKEBASE_INSTANCE)
        cred = w.database.generate_database_credential(
            instance_names=[config.LAKEBASE_INSTANCE], request_id="ra-app")
        user = w.current_user.me().user_name
        return psycopg2.connect(host=inst.read_write_dns, port=5432, dbname="databricks_postgres",
                                user=user, password=cred.token, sslmode="require")
    except Exception as e:
        print(f"[RA App] Lakebase unavailable: {e}")
        return None


def _ensure_bootstrap(conn):
    global _bootstrapped
    if _bootstrapped:
        return
    try:
        conn.autocommit = True
        cur = conn.cursor()
        for d in _CONV_DDL:
            cur.execute(d)
        cur.close()
        conn.autocommit = False
        _bootstrapped = True
    except Exception as e:
        print(f"[RA App] chat-table bootstrap skipped: {e}")


def available() -> bool:
    conn = _conn()
    if conn is None:
        return False
    conn.close()
    return True


# ---------------------------- adjudication audit trail ----------------------------
def write_adjudication(member_id, hcc, decision, reason, chunk_id, user_id, ai_suggested=True):
    conn = _conn()
    if conn is None:
        return {"written": False, "reason": "memory unavailable"}
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ra_adjudications (adjudication_id, member_id, user_id, hcc_code, ai_suggested, "
            "decision, reason, evidence_chunk_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), member_id, user_id, hcc, ai_suggested, decision, reason, chunk_id))
        conn.commit(); cur.close(); conn.close()
        return {"written": True}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"written": False, "error": str(e)}


def list_adjudications(member_id):
    conn = _conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT hcc_code, decision, reason, evidence_chunk_id, created_at "
                    "FROM ra_adjudications WHERE member_id=%s ORDER BY created_at DESC LIMIT 50", (member_id,))
        rows = [{"hcc": r[0], "decision": r[1], "reason": r[2], "chunk_id": r[3], "at": str(r[4])}
                for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        try: conn.close()
        except Exception: pass
        print(f"[RA App] list_adjudications failed: {e}")
        return []


# ------------------------------- chat / conversations ------------------------------
def create_conversation(member_id, user_email, model_endpoint, title=None):
    conn = _conn()
    if conn is None:
        return {"ok": False, "reason": "memory unavailable"}
    try:
        _ensure_bootstrap(conn)
        cid = str(uuid.uuid4())
        cur = conn.cursor()
        cur.execute("INSERT INTO conversations (conversation_id, member_id, user_email, title, model_endpoint) "
                    "VALUES (%s,%s,%s,%s,%s)", (cid, member_id, user_email, title or "New chart review", model_endpoint))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "conversation_id": cid}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"ok": False, "error": str(e)}


def save_message(conversation_id, role, content, metadata=None):
    conn = _conn()
    if conn is None:
        return {"ok": False, "reason": "memory unavailable"}
    try:
        _ensure_bootstrap(conn)
        cur = conn.cursor()
        cur.execute("INSERT INTO conversation_messages (message_id, conversation_id, role, content, metadata) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (str(uuid.uuid4()), conversation_id, role, content, json.dumps(metadata or {})))
        cur.execute("UPDATE conversations SET updated_at=now() WHERE conversation_id=%s", (conversation_id,))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"ok": False, "error": str(e)}


def load_messages(conversation_id):
    conn = _conn()
    if conn is None:
        return []
    try:
        _ensure_bootstrap(conn)
        cur = conn.cursor()
        cur.execute("SELECT role, content, metadata, created_at FROM conversation_messages "
                    "WHERE conversation_id=%s ORDER BY created_at", (conversation_id,))
        rows = [{"role": r[0], "content": r[1], "metadata": r[2] or {}, "at": str(r[3])} for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        try: conn.close()
        except Exception: pass
        print(f"[RA App] load_messages failed: {e}")
        return []


def list_conversations(user_id):
    conn = _conn()
    if conn is None:
        return []
    try:
        _ensure_bootstrap(conn)
        cur = conn.cursor()
        cur.execute("SELECT conversation_id, member_id, title, model_endpoint, updated_at "
                    "FROM conversations WHERE user_email=%s ORDER BY updated_at DESC LIMIT 50", (user_id,))
        rows = [{"conversation_id": r[0], "member_id": r[1], "title": r[2],
                 "model_endpoint": r[3], "updated_at": str(r[4])} for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        try: conn.close()
        except Exception: pass
        print(f"[RA App] list_conversations failed: {e}")
        return []


# ------------------------------------ memory CRUD ----------------------------------
def get_memory(user_id):
    conn = _conn()
    if conn is None:
        return {"prefs": [], "lessons": [], "available": False}
    try:
        cur = conn.cursor()
        cur.execute("SELECT pref_key, pref_value, source, updated_at FROM agent_user_prefs "
                    "WHERE user_id=%s ORDER BY updated_at DESC", (user_id,))
        prefs = [{"pref_key": r[0], "pref_value": r[1], "source": r[2] or "inferred", "updated_at": str(r[3])}
                 for r in cur.fetchall()]
        # Lessons are agent-wide (scoped by provider/coder/hcc). Surface recent ones so the loop is visible.
        cur.execute("SELECT lesson_id, scope, subject_key, lesson_text, confidence, status, "
                    "times_recalled, provenance, created_at FROM agent_lessons "
                    "ORDER BY created_at DESC LIMIT 100")
        lessons = []
        for r in cur.fetchall():
            prov = r[7] or {}
            src = prov.get("source", "reflection") if isinstance(prov, dict) else "reflection"
            lessons.append({"lesson_id": r[0], "scope": r[1], "subject_key": r[2], "lesson_text": r[3],
                            "confidence": float(r[4] or 0), "status": r[5], "times_recalled": r[6],
                            "source": src, "created_at": str(r[8])})
        cur.close(); conn.close()
        return {"prefs": prefs, "lessons": lessons, "available": True}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"prefs": [], "lessons": [], "available": False, "error": str(e)}


def add_pref(user_id, pref_key, pref_value):
    conn = _conn()
    if conn is None:
        return {"ok": False, "reason": "memory unavailable"}
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO agent_user_prefs (user_id, pref_key, pref_value, source, updated_at) "
            "VALUES (%s,%s,%s,'explicit', now()) "
            "ON CONFLICT (user_id, pref_key) DO UPDATE SET pref_value=EXCLUDED.pref_value, "
            "source='explicit', updated_at=now()",
            (user_id, pref_key, pref_value))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"ok": False, "error": str(e)}


def delete_memory(kind, ident, user_id=None):
    conn = _conn()
    if conn is None:
        return {"ok": False, "reason": "memory unavailable"}
    try:
        cur = conn.cursor()
        if kind == "pref":
            if user_id:
                cur.execute("DELETE FROM agent_user_prefs WHERE user_id=%s AND pref_key=%s", (user_id, ident))
            else:
                cur.execute("DELETE FROM agent_user_prefs WHERE pref_key=%s", (ident,))
        elif kind == "lesson":
            cur.execute("DELETE FROM agent_lessons WHERE lesson_id=%s", (ident,))
        else:
            cur.close(); conn.close()
            return {"ok": False, "error": f"unknown kind {kind}"}
        n = cur.rowcount
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "deleted": n}
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return {"ok": False, "error": str(e)}
