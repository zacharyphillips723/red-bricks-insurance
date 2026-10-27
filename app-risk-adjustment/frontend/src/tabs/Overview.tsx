import { useEffect, useState } from 'react';
import { api } from '../api';
import type { TabKey } from '../App';

export function Overview({ goto }: { goto: (t: TabKey) => void }) {
  const [data, setData] = useState<any>(null);
  useEffect(() => { api.overview().then(setData).catch(() => setData({ stats: {} })); }, []);
  const s = data?.stats || {};
  const states: any[] = data?.evidence_states || [];

  return (
    <div className="scroll">
      <div className="pad" style={{ maxWidth: 1100, margin: '0 auto' }}>
        <h1>Risk Adjustment & HCC Intelligence</h1>
        <p className="muted" style={{ maxWidth: 760 }}>
          A payer-side coder workbench where an AI agent reads the member's unstructured clinical chart,
          surfaces <b>suspected HCCs with verified evidence citations</b>, and flags RADV-weak coded HCCs.
          HCC grouping is deterministic (CMS-HCC V28 subset), the organization's coding policy is read-only
          and wins all conflicts, and the agent keeps governed memory in Lakebase. Every LLM, tool, retrieval,
          and memory step is MLflow-traced to Unity Catalog, and the foundation model is swappable via Unity
          Gateway with no redeploy.
        </p>

        <div className="grid cols-3" style={{ marginTop: 18 }}>
          <div className="card pad stat"><div className="n">{s.worklist ?? '—'}</div><div className="l">Members on the worklist (open RAF capture)</div></div>
          <div className="card pad stat"><div className="n">{s.total_gain ?? '—'}</div><div className="l">Total potential RAF gain (suspected, uncoded)</div></div>
          <div className="card pad stat"><div className="n">{s.total_radv_risk ?? '—'}</div><div className="l">RAF at RADV risk (weakly documented)</div></div>
        </div>

        <div className="grid cols-3" style={{ marginTop: 16 }}>
          <div className="card pad" style={{ gridColumn: 'span 2' }}>
            <h3>What this app does</h3>
            {[
              ['Chart Review', 'Pick a member, see the evidence layer, run the agent to suspect HCCs with cited passages, then Code / Reject / Query each — written to the Lakebase audit trail.'],
              ['Agent Activity', 'The live, ordered list of tools the agent ran for the current chart or chat turn — search, verify_citation, deterministic grouper, RAF math, memory reads/writes.'],
              ['Chat', 'Work a chart conversationally; turns persist to Lakebase and ground on the selected member.'],
              ['Memory', 'Inspect and control the agent\'s standing instructions and distilled lessons — add your own, delete any you disagree with.'],
            ].map(([h, b]) => (
              <div className="feat" key={h}><div className="dot" /><div><b>{h}</b><div className="muted">{b}</div></div></div>
            ))}
            <div style={{ marginTop: 10 }}>
              <button className="btn primary" onClick={() => goto('chart')}>Open Chart Review →</button>
            </div>
          </div>

          <div className="card pad">
            <h3>Data scope</h3>
            <div className="muted" style={{ fontSize: 13 }}>
              <div><b>Catalog</b> <code>{data?.catalog || 'red_bricks_insurance_catalog'}</code></div>
              <div style={{ marginTop: 8 }}><b>Evidence states</b></div>
              {states.length === 0 && <div style={{ marginTop: 4 }}>—</div>}
              {states.map((e) => (
                <div key={e.evidence_state} style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4 }}>
                  <span>{e.evidence_state}</span><b>{e.n}</b>
                </div>
              ))}
              <div style={{ marginTop: 10 }}><b>Default model</b><div><code>{data?.default_model}</code></div></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
