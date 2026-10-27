import { useEffect, useState } from 'react';
import { api, Step } from './api';
import { Overview } from './tabs/Overview';
import { ChartReview } from './tabs/ChartReview';
import { AgentActivity } from './tabs/AgentActivity';
import { Chat } from './tabs/Chat';
import { Memory } from './tabs/Memory';

export type TabKey = 'overview' | 'chart' | 'activity' | 'chat' | 'memory';

export interface Activity {
  source: string;        // "Chart Review · MBR100445" | "Chat"
  model: string;
  steps: Step[];
  at: string;
}

const TABS: { key: TabKey; label: string }[] = [
  { key: 'overview', label: 'Overview' },
  { key: 'chart', label: 'Chart Review' },
  { key: 'activity', label: 'Agent Activity' },
  { key: 'chat', label: 'Chat' },
  { key: 'memory', label: 'Memory' },
];

export function App() {
  const [tab, setTab] = useState<TabKey>('overview');
  const [models, setModels] = useState<string[]>([]);
  const [model, setModel] = useState('');
  const [user, setUser] = useState('');
  const [activity, setActivity] = useState<Activity | null>(null);

  useEffect(() => {
    api.models().then((d) => { setModels(d.models || []); setModel(d.default || (d.models || [])[0] || ''); });
    api.whoami().then((d) => setUser(d.user)).catch(() => {});
  }, []);

  const recordActivity = (source: string, steps: Step[]) =>
    setActivity({ source, model, steps, at: new Date().toLocaleTimeString() });

  return (
    <div className="app">
      <header>
        <div className="brand">
          <span className="mark">Red&nbsp;<b>Bricks</b></span>
          <span className="sub">· Risk Adjustment — Coder Workbench</span>
        </div>
        <div className="spacer" />
        <div className="model-pick">
          <label>Unity Gateway model</label>
          <select value={model} onChange={(e) => setModel(e.target.value)}>
            {models.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>
        {user && <div className="whoami" title={user}>{user}</div>}
      </header>

      <div className="tabs">
        {TABS.map((t) => (
          <button key={t.key} className={`tab ${tab === t.key ? 'active' : ''}`} onClick={() => setTab(t.key)}>
            {t.label}{t.key === 'activity' && activity ? ` (${activity.steps.length})` : ''}
          </button>
        ))}
      </div>

      {/* Tab panels stay mounted and toggle visibility, so per-tab state (e.g. the selected
          member and suspect result on Chart Review) survives switching tabs. */}
      <div className="body">
        <div className="tabpanel" style={{ display: tab === 'overview' ? 'block' : 'none' }}>
          <Overview goto={setTab} />
        </div>
        <div className="tabpanel" style={{ display: tab === 'chart' ? 'block' : 'none' }}>
          <ChartReview model={model} onActivity={recordActivity} gotoActivity={() => setTab('activity')} />
        </div>
        <div className="tabpanel" style={{ display: tab === 'activity' ? 'block' : 'none' }}>
          <AgentActivity activity={activity} />
        </div>
        <div className="tabpanel" style={{ display: tab === 'chat' ? 'block' : 'none' }}>
          <Chat model={model} onActivity={recordActivity} />
        </div>
        <div className="tabpanel" style={{ display: tab === 'memory' ? 'block' : 'none' }}>
          <Memory />
        </div>
      </div>
    </div>
  );
}
