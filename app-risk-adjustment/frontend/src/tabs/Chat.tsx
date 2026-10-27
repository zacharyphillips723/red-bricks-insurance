import { useEffect, useRef, useState } from 'react';
import { api, Step } from '../api';
import { Markdown, StepList } from '../Markdown';

interface Msg { role: string; content: string; steps?: Step[]; }

export function Chat({ model, onActivity }: { model: string; onActivity: (s: string, steps: Step[]) => void }) {
  const [convs, setConvs] = useState<any[]>([]);
  const [cid, setCid] = useState<string | null>(null);
  const [member, setMember] = useState('');
  const [cases, setCases] = useState<any[]>([]);
  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);

  const loadConvs = () => api.conversations().then((d) => setConvs(d.conversations || [])).catch(() => {});
  useEffect(() => { loadConvs(); api.cases().then((d) => setCases(d.cases || [])); }, []);
  useEffect(() => { logRef.current?.scrollTo(0, logRef.current.scrollHeight); }, [msgs, busy]);

  async function openConv(id: string, mid: string) {
    setCid(id); setMember(mid || '');
    const d = await api.conversation(id);
    setMsgs((d.messages || []).map((m: any) => ({ role: m.role, content: m.content, steps: m.metadata?.steps })));
  }

  function newChat() { setCid(null); setMsgs([]); setInput(''); }

  async function send() {
    const text = input.trim();
    if (!text || busy) return;
    setInput('');
    setMsgs((p) => [...p, { role: 'user', content: text }]);
    setBusy(true);
    try {
      const d = await api.chat({ conversation_id: cid, member_id: member || null, message: text, model_endpoint: model });
      if (!cid && d.conversation_id) { setCid(d.conversation_id); loadConvs(); }
      setMsgs((p) => [...p, { role: 'assistant', content: d.reply, steps: d.steps }]);
      onActivity('Chat', d.steps || []);
    } catch (e: any) {
      setMsgs((p) => [...p, { role: 'assistant', content: `**Error:** ${e.message}` }]);
    } finally { setBusy(false); }
  }

  return (
    <div className="chat">
      <div className="chat-side">
        <div style={{ padding: 12, borderBottom: '1px solid var(--line)' }}>
          <button className="btn sm primary" style={{ width: '100%' }} onClick={newChat}>+ New chart review</button>
        </div>
        {convs.map((c) => (
          <div key={c.conversation_id} className={`conv ${cid === c.conversation_id ? 'active' : ''}`} onClick={() => openConv(c.conversation_id, c.member_id)}>
            <div className="t">{c.title || 'Conversation'}</div>
            <div className="m">{c.member_id || 'no member'} · {new Date(c.updated_at).toLocaleString()}</div>
          </div>
        ))}
        {convs.length === 0 && <div className="empty" style={{ fontSize: 12, padding: 20 }}>No saved conversations yet.</div>}
      </div>

      <div className="chat-main">
        <div style={{ padding: '10px 20px', borderBottom: '1px solid var(--line)', display: 'flex', gap: 10, alignItems: 'center', background: 'var(--panel)' }}>
          <span className="muted" style={{ fontSize: 13 }}>Ground on member</span>
          <select value={member} onChange={(e) => setMember(e.target.value)} style={{ padding: '6px 9px', borderRadius: 8, border: '1px solid var(--line-strong)' }}>
            <option value="">(none)</option>
            {cases.map((c) => <option key={c.member_id} value={c.member_id}>{c.member_id}</option>)}
          </select>
          <span className="muted" style={{ fontSize: 12 }}>model {model}</span>
        </div>

        <div className="chat-log" ref={logRef}>
          {msgs.length === 0 && <div className="empty">Ask the agent to review a member's chart, explain a suspected HCC, or check RADV defensibility. Pick a member above to ground the conversation.</div>}
          {msgs.map((m, i) => (
            <div key={i} className={`bubble ${m.role}`}>
              <div className="who">{m.role === 'user' ? 'You' : 'RA Agent'}</div>
              <div className="box">
                {m.role === 'assistant' ? <Markdown>{m.content}</Markdown> : m.content}
              </div>
              {m.role === 'assistant' && m.steps && m.steps.length > 0 && (
                <details style={{ marginTop: 6 }}>
                  <summary className="muted" style={{ cursor: 'pointer', fontSize: 12 }}>{m.steps.length} agent steps</summary>
                  <div className="card" style={{ marginTop: 6 }}><StepList steps={m.steps} /></div>
                </details>
              )}
            </div>
          ))}
          {busy && <div className="bubble assistant"><div className="who">RA Agent</div><div className="box"><span className="spinner dark" /> working the chart…</div></div>}
        </div>

        <div className="chat-input">
          <textarea value={input} placeholder="Message the RA agent…" onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); } }} />
          <button className="btn primary" onClick={send} disabled={busy}>Send</button>
        </div>
      </div>
    </div>
  );
}
