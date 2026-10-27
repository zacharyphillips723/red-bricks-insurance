import { useEffect, useState } from 'react';
import { api } from '../api';

// Curated standing-instruction keys. The agent does not dispatch on the key name
// (it reads all prefs as labeled guidance), but a controlled vocabulary keeps the
// ON CONFLICT (user_id, pref_key) upsert meaningful — same key edits, not duplicates —
// and shows which dimensions of the agent's behavior are steerable. "Custom…" allows
// a free-form key for anything not covered.
const PREF_KEYS: { key: string; label: string; hint: string }[] = [
  { key: 'evidence_bar', label: 'Evidence bar', hint: 'e.g. require a lab value or provider note before suspecting nephropathy' },
  { key: 'suspecting_aggressiveness', label: 'Suspecting aggressiveness', hint: 'e.g. conservative — only surface documented or strongly suggested HCCs' },
  { key: 'citation_strictness', label: 'Citation strictness', hint: 'e.g. never surface an HCC without a verified chart citation' },
  { key: 'radv_caution', label: 'RADV caution', hint: 'e.g. always flag weak-evidence HCCs as RADV deletion risk' },
  { key: 'focus_hccs', label: 'HCC focus', hint: 'e.g. prioritize diabetes (HCC18/19), CKD (HCC135), CHF (HCC85)' },
  { key: 'exclude_hccs', label: 'HCC exclusions', hint: 'e.g. never suspect HCC55 (substance use) for this panel' },
  { key: 'query_threshold', label: 'Provider-query threshold', hint: 'e.g. recommend a provider query instead of rejecting when evidence is borderline' },
  { key: 'specialty_lens', label: 'Specialty lens', hint: 'e.g. read charts as a nephrologist would' },
  { key: 'output_verbosity', label: 'Output verbosity', hint: 'e.g. brief — one line per HCC with the citation' },
];
const CUSTOM = '__custom__';

export function Memory() {
  const [data, setData] = useState<any>(null);
  const [key, setKey] = useState('');
  const [keyChoice, setKeyChoice] = useState('');   // dropdown selection (a PREF_KEYS key, or CUSTOM)
  const [val, setVal] = useState('');
  const [busy, setBusy] = useState(false);

  const load = () => api.memory().then(setData).catch(() => setData({ prefs: [], lessons: [], available: false }));
  useEffect(() => { load(); }, []);

  const effectiveKey = keyChoice === CUSTOM ? key.trim() : keyChoice;
  const hint = PREF_KEYS.find((p) => p.key === keyChoice)?.hint || 'value (e.g. require labs before suspecting nephropathy)';

  async function add() {
    if (!effectiveKey || !val.trim()) return;
    setBusy(true);
    await api.addPref(effectiveKey, val.trim());
    setKey(''); setKeyChoice(''); setVal(''); setBusy(false); load();
  }
  async function del(kind: string, ident: string) {
    if (!window.confirm('Delete this memory?')) return;
    await api.delMemory(kind, ident); load();
  }

  const prefs: any[] = data?.prefs || [];
  const lessons: any[] = data?.lessons || [];

  return (
    <div className="scroll">
      <div className="pad" style={{ maxWidth: 980, margin: '0 auto' }}>
        <h1>Agent Memory</h1>
        <p className="muted" style={{ maxWidth: 760 }}>
          The agent keeps governed memory in Lakebase. <b>Standing instructions</b> are preferences it applies for you;
          <b> lessons</b> are patterns it distills as it works cases (provisional until corroborated). Org coding policy
          always wins — memory never overrides it. Add your own instructions, and delete anything you disagree with.
        </p>
        {data && data.available === false && <div className="banner" style={{ marginTop: 10 }}>Lakebase memory is currently unavailable — showing what we can. {data.error ? `(${data.error})` : ''}</div>}

        <div className="card pad" style={{ marginTop: 16 }}>
          <h3>Standing instructions {prefs.length ? `(${prefs.length})` : ''}</h3>
          {prefs.length === 0 && <div className="muted">No standing instructions yet.</div>}
          {prefs.map((p) => (
            <div className="mem-row" key={p.pref_key}>
              <div>
                <span className={`badge ${p.source === 'explicit' ? 'user' : 'agent'}`}>{p.source === 'explicit' ? 'you' : 'agent-inferred'}</span>
                &nbsp;<b>{p.pref_key}</b>
                <div className="muted">{p.pref_value}</div>
              </div>
              <button className="btn sm" onClick={() => del('pref', p.pref_key)}>Delete</button>
            </div>
          ))}
          <div className="inline-form">
            <select value={keyChoice} onChange={(e) => setKeyChoice(e.target.value)}
              style={{ width: 240, font: 'inherit', border: '1px solid var(--line-strong)', borderRadius: 8, padding: '8px 10px' }}>
              <option value="">Choose instruction…</option>
              {PREF_KEYS.map((p) => <option key={p.key} value={p.key}>{p.label}</option>)}
              <option value={CUSTOM}>Custom…</option>
            </select>
            {keyChoice === CUSTOM && (
              <input placeholder="custom key (e.g. my_rule)" value={key} onChange={(e) => setKey(e.target.value)} style={{ width: 200 }} />
            )}
            <input placeholder={hint} value={val} onChange={(e) => setVal(e.target.value)} style={{ flex: 1, minWidth: 260 }} />
            <button className="btn primary" onClick={add} disabled={busy || !effectiveKey || !val.trim()}>Add instruction</button>
          </div>
        </div>

        <div className="card pad" style={{ marginTop: 16 }}>
          <h3>Distilled lessons {lessons.length ? `(${lessons.length})` : ''}</h3>
          {lessons.length === 0 && <div className="muted">No lessons yet — the agent writes these as it works cases.</div>}
          {lessons.map((l) => (
            <div className="mem-row" key={l.lesson_id}>
              <div>
                <span className={`badge ${l.status === 'confirmed' ? 'conf' : 'prov'}`}>{l.status}</span>
                <span className="badge agent" style={{ marginLeft: 6 }}>{l.source}</span>
                &nbsp;<b>{l.scope}</b> / <code>{l.subject_key}</code>
                <div style={{ marginTop: 3 }}>{l.lesson_text}</div>
                <div className="muted" style={{ fontSize: 12 }}>confidence {l.confidence?.toFixed?.(2) ?? l.confidence} · recalled {l.times_recalled}× · {new Date(l.created_at).toLocaleString()}</div>
              </div>
              <button className="btn sm" onClick={() => del('lesson', l.lesson_id)}>Delete</button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
