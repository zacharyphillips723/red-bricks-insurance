async function j(url: string, opts?: RequestInit) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}
const post = (url: string, body: any) =>
  j(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

export interface Step { tool: string; purpose: string; summary: string; }
export interface Candidate {
  hcc?: string; evidence_state?: string; chunk_id?: string; raf_value?: number;
  verified?: boolean; passage?: string;
}

export const api = {
  models: () => j('/api/models'),
  whoami: () => j('/api/whoami'),
  overview: () => j('/api/overview'),
  cases: () => j('/api/cases'),
  searchMembers: (q: string) => j(`/api/members/search?q=${encodeURIComponent(q)}&limit=25`),
  evidence: (id: string) => j(`/api/member/${encodeURIComponent(id)}/evidence`),
  suspect: (member_id: string, model_endpoint: string) => post('/api/suspect', { member_id, model_endpoint }),
  suspectStart: (member_id: string, model_endpoint: string) => post('/api/suspect/start', { member_id, model_endpoint }),
  suspectPoll: (run_id: string) => j(`/api/suspect/poll/${run_id}`),
  adjudicate: (b: any) => post('/api/adjudicate', b),
  chat: (b: any) => post('/api/chat', b),
  conversations: () => j('/api/conversations'),
  conversation: (id: string) => j(`/api/conversations/${id}`),
  memory: () => j('/api/memory'),
  addPref: (pref_key: string, pref_value: string) => post('/api/memory', { pref_key, pref_value }),
  delMemory: (kind: string, ident: string) =>
    j(`/api/memory/${kind}/${encodeURIComponent(ident)}`, { method: 'DELETE' }),
};
