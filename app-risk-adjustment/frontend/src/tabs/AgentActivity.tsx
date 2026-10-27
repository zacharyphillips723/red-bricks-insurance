import { StepList } from '../Markdown';
import type { Activity } from '../App';

export function AgentActivity({ activity }: { activity: Activity | null }) {
  return (
    <div className="scroll">
      <div className="pad" style={{ maxWidth: 900, margin: '0 auto' }}>
        <h1>Agent Activity</h1>
        <p className="muted">
          The ordered list of tools the agent executed for the most recent chart review or chat turn.
          Each step shows the tool, its purpose, and a one-line result — the agent's reasoning made legible.
        </p>
        {!activity && <div className="empty">Run suspecting on a chart or send a chat message to see the agent's steps.</div>}
        {activity && (
          <div className="card" style={{ marginTop: 14 }}>
            <div className="pad" style={{ borderBottom: '1px solid var(--line)', display: 'flex', justifyContent: 'space-between' }}>
              <div><b>{activity.source}</b><div className="muted" style={{ fontSize: 12 }}>model {activity.model} · {activity.at}</div></div>
              <div className="chip ghost" style={{ alignSelf: 'center' }}>{activity.steps.length} steps</div>
            </div>
            <StepList steps={activity.steps} />
          </div>
        )}
      </div>
    </div>
  );
}
