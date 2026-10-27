import { useEffect, useRef, useState } from 'react';
import { api, Candidate, Step } from '../api';
import { Markdown, StepList } from '../Markdown';

interface Props {
  model: string;
  onActivity: (source: string, steps: Step[]) => void;
  gotoActivity: () => void;
}

const num = (v: any) => (Number.isFinite(Number(v)) ? Number(v) : 0);
const sumRaf = (rows: any[]) => rows.reduce((a, r) => a + num(r.raf_value), 0);

export function ChartReview({ model, onActivity, gotoActivity }: Props) {
  const [cases, setCases] = useState<any[]>([]);
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<any[]>([]);
  const [searching, setSearching] = useState(false);
  const [sel, setSel] = useState<string | null>(null);
  const [evidence, setEvidence] = useState<any | null>(null);
  const [loadingRun, setLoadingRun] = useState(false);
  const [runSteps, setRunSteps] = useState<Step[]>([]);
  const [result, setResult] = useState<{ narrative: string; candidates: Candidate[]; steps: Step[]; meta: any } | null>(null);
  const [adj, setAdj] = useState<Record<string, string>>({});
  const runRef = useRef(0);   // cancels any in-flight poll loop when the member changes / re-runs

  useEffect(() => { api.cases().then((d) => setCases(d.cases || [])); }, []);

  // Debounced member search (by id or name). Empty query = default top-by-RAF worklist.
  useEffect(() => {
    const term = query.trim();
    if (!term) { setResults([]); setSearching(false); return; }
    setSearching(true);
    const h = setTimeout(() => {
      api.searchMembers(term)
        .then((d) => setResults(d.results || []))
        .catch(() => setResults([]))
        .finally(() => setSearching(false));
    }, 300);
    return () => clearTimeout(h);
  }, [query]);

  const list = query.trim() ? results : cases;

  async function select(id: string) {
    runRef.current++;                 // cancel any in-flight agent poll for the previous member
    setSel(id); setResult(null); setEvidence(null); setAdj({}); setRunSteps([]); setLoadingRun(false);
    const e = await api.evidence(id);
    setEvidence(e);
  }

  async function run() {
    if (!sel) return;
    const member = sel;
    const token = ++runRef.current;
    setLoadingRun(true); setResult(null); setRunSteps([]);
    try {
      const start = await api.suspectStart(member, model);
      if (!start.streaming) {                       // endpoint mode: no live steps, one-shot
        const d = await api.suspect(member, model);
        if (runRef.current !== token) return;
        setResult(d); setRunSteps(d.steps || []); onActivity(`Chart Review · ${member}`, d.steps || []);
        return;
      }
      // Poll the background run for live steps until done.
      while (runRef.current === token) {
        await new Promise((r) => setTimeout(r, 1000));
        if (runRef.current !== token) return;
        const p = await api.suspectPoll(start.run_id);
        if (!p.found) break;
        setRunSteps(p.steps || []);
        if (p.done) {
          const d = { narrative: p.narrative, candidates: p.candidates || [], steps: p.steps || [], meta: p.meta || {} };
          setResult(d);
          onActivity(`Chart Review · ${member}`, p.steps || []);
          break;
        }
      }
    } catch (e: any) {
      if (runRef.current === token) setResult({ narrative: `**Error:** ${e.message}`, candidates: [], steps: [], meta: {} });
    } finally {
      if (runRef.current === token) setLoadingRun(false);
    }
  }

  async function adjudicate(hcc: string, chunkId: string | null, decision: string) {
    let reason = '';
    if (decision === 'reject' || decision === 'query') reason = window.prompt(`Reason for ${decision}:`) || '';
    const r = await api.adjudicate({ member_id: sel, hcc, decision, reason, chunk_id: chunkId });
    setAdj((p) => ({ ...p, [hcc]: r.written ? decision : 'failed' }));
    if (sel) api.evidence(sel).then(setEvidence);
  }

  function ChunkCell({ id, passage }: { id?: string | null; passage?: string }) {
    if (!id) return <>—</>;
    return (
      <span className="chunk" tabIndex={0}>
        <code>{id}</code>
        <span className="chunk-tip">{passage ? passage : 'Passage text not retrievable for this chunk.'}</span>
      </span>
    );
  }

  function AdjCell({ hcc, chunkId }: { hcc: string; chunkId: string | null }) {
    if (adj[hcc]) return <span className="chip ghost">{adj[hcc]}</span>;
    return (
      <span style={{ whiteSpace: 'nowrap' }}>
        <button className="btn sm primary" onClick={() => adjudicate(hcc, chunkId, 'code')}>Code</button>{' '}
        <button className="btn sm" onClick={() => adjudicate(hcc, chunkId, 'reject')}>Reject</button>{' '}
        <button className="btn sm" onClick={() => adjudicate(hcc, chunkId, 'query')}>Query</button>
      </span>
    );
  }

  const ev: any[] = evidence?.evidence || [];
  const suspected = ev.filter((r) => r.evidence_state === 'suspected');
  const weak = ev.filter((r) => r.evidence_state === 'weak');
  const context = ev.filter((r) => r.evidence_state === 'documented' || r.evidence_state === 'non_hcc_noise');

  function PreReadTable({ rows }: { rows: any[] }) {
    return (
      <table>
        <thead><tr><th>HCC</th><th>Label</th><th>RAF</th><th>Cited chunk</th><th>Note hits</th><th>Adjudicate</th></tr></thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td><b>{r.hcc_code}</b></td>
              <td>{r.hcc_label}</td>
              <td>{r.raf_value}</td>
              <td><ChunkCell id={r.evidence_chunk_id} passage={r.passage} /></td>
              <td>{r.note_hits}</td>
              <td><AdjCell hcc={r.hcc_code} chunkId={r.evidence_chunk_id || null} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }

  return (
    <div className="split">
      <div className="queue">
        <div className="queue-head" style={{ display: 'block' }}>
          <input className="search" placeholder="Search member by ID or name…" value={query}
            onChange={(e) => setQuery(e.target.value)} />
          <div className="muted" style={{ fontWeight: 400, fontSize: 12, marginTop: 6 }}>
            {query.trim()
              ? (searching ? 'searching…' : `${results.length} match${results.length === 1 ? '' : 'es'}`)
              : <>Worklist · {cases.length} members by RAF gain</>}
          </div>
        </div>
        {query.trim() && !searching && results.length === 0 && <div className="empty" style={{ fontSize: 13, padding: 24 }}>No members found.</div>}
        {list.map((c) => (
          <div key={c.member_id} className={`case ${sel === c.member_id ? 'active' : ''}`} onClick={() => select(c.member_id)}>
            <div className="mid">{c.member_id}{c.member_name ? <span className="muted" style={{ fontWeight: 400 }}> · {c.member_name}</span> : null}</div>
            <div className="meta">potential RAF +{c.potential_raf_gain} · RADV risk {c.raf_at_radv_risk}</div>
            <div>
              {String(c.suspected_hccs || '').split(',').filter(Boolean).map((h: string) => <span key={h} className="chip sus">{h} suspect</span>)}
              {String(c.weak_hccs || '').split(',').filter(Boolean).map((h: string) => <span key={h} className="chip weak">{h} weak</span>)}
            </div>
          </div>
        ))}
      </div>

      <div className="main">
        {!sel && <div className="empty">Select a member from the worklist to review their chart.</div>}
        {sel && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: 14, flexWrap: 'wrap' }}>
              <h1 style={{ margin: 0 }}>{sel}</h1>
              <button className="btn primary" onClick={run} disabled={loadingRun}>
                {loadingRun ? <><span className="spinner" /> Agent reviewing…</> : `▶ Deep agent review (${model})`}
              </button>
              <span className="muted" style={{ fontSize: 12, maxWidth: 420 }}>
                The Overview below is computed deterministically (grouper, no LLM). The agent adds verified citations,
                passage text, a narrative, and the memory loop.
              </span>
            </div>

            {/* Deterministic pre-read — instant, straight from the evidence table */}
            {evidence && (
              <div className="card pad" style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8 }}>
                  <h3 style={{ margin: 0 }}>Overview</h3>
                  <span className="muted" style={{ fontSize: 12 }}>
                    potential RAF gain <b>+{sumRaf(suspected).toFixed(2)}</b> · RAF at RADV risk <b>{sumRaf(weak).toFixed(2)}</b>
                  </span>
                </div>

                <h3 style={{ marginTop: 16 }}>Capture opportunities — suspected ({suspected.length})</h3>
                {suspected.length === 0 ? <div className="muted">No uncoded, note-evidenced HCCs.</div> : <PreReadTable rows={suspected} />}

                <h3 style={{ marginTop: 18 }}>RADV risk — weak coded HCCs ({weak.length})</h3>
                {weak.length === 0 ? <div className="muted">No coded HCCs lacking note evidence.</div> : <PreReadTable rows={weak} />}

                {context.length > 0 && (
                  <details style={{ marginTop: 14 }}>
                    <summary className="muted" style={{ cursor: 'pointer', fontSize: 12 }}>
                      {context.length} documented / non-HCC context rows
                    </summary>
                    <table style={{ marginTop: 6 }}>
                      <thead><tr><th>HCC</th><th>Label</th><th>RAF</th><th>State</th><th>Note hits</th></tr></thead>
                      <tbody>
                        {context.map((r, i) => (
                          <tr key={i}>
                            <td><b>{r.hcc_code}</b></td><td>{r.hcc_label}</td><td>{r.raf_value}</td>
                            <td className={`state ${r.evidence_state}`}>{r.evidence_state}</td><td>{r.note_hits}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </details>
                )}

                {evidence.adjudications?.length > 0 && (
                  <div style={{ marginTop: 10, fontSize: 12 }} className="muted">
                    Prior adjudications: {evidence.adjudications.map((a: any) => `${a.hcc}→${a.decision}`).join(', ')}
                  </div>
                )}
              </div>
            )}

            {/* Live agent step feed while the opt-in deep review runs */}
            {(loadingRun || (runSteps.length > 0 && !result)) && (
              <div className="card pad" style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <h3 style={{ margin: 0 }}>{loadingRun ? <><span className="spinner dark" /> Agent working the chart…</> : 'Agent steps'}</h3>
                  <span className="muted" style={{ fontSize: 12 }}>{runSteps.length} step{runSteps.length === 1 ? '' : 's'}</span>
                </div>
                {runSteps.length === 0 ? <div className="muted" style={{ marginTop: 8 }}>Starting up…</div>
                  : <div style={{ marginTop: 8 }}><StepList steps={runSteps} /></div>}
              </div>
            )}

            {/* Agent result — verified citations + passages + narrative */}
            {result && (
              <div className="card pad" style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <h3>Agent review — verified candidates</h3>
                  <button className="btn sm" onClick={gotoActivity}>View agent activity ({result.steps?.length || 0}) →</button>
                </div>
                <table>
                  <thead><tr><th>HCC</th><th>State</th><th>RAF</th><th>Cited passage</th><th>Verified</th><th>Adjudicate</th></tr></thead>
                  <tbody>
                    {result.candidates.length === 0 && <tr><td colSpan={6} className="muted">No candidates returned.</td></tr>}
                    {result.candidates.map((c, i) => (
                      <tr key={i}>
                        <td><b>{c.hcc}</b></td>
                        <td className={`state ${c.evidence_state || ''}`}>{c.evidence_state}</td>
                        <td>{c.raf_value ?? ''}</td>
                        <td>
                          {c.chunk_id ? <code>{c.chunk_id}</code> : '—'}
                          {c.passage && <div className="passage">{c.passage.slice(0, 320)}{c.passage.length > 320 ? '…' : ''}</div>}
                        </td>
                        <td>{c.verified === true ? <span className="ok">✓</span> : c.evidence_state === 'suspected' ? <span className="no">✗</span> : '—'}</td>
                        <td><AdjCell hcc={c.hcc || ''} chunkId={c.chunk_id || null} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {result.meta && (result.meta.potential_raf_gain != null) && (
                  <div style={{ marginTop: 10 }} className="muted">
                    Agent-reported potential RAF gain <b>{result.meta.potential_raf_gain}</b>
                    {result.meta.raf_at_radv_risk != null && <> · RAF at RADV risk <b>{result.meta.raf_at_radv_risk}</b></>}
                  </div>
                )}
                <h3 style={{ marginTop: 18 }}>Agent narrative</h3>
                <Markdown>{result.narrative}</Markdown>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
