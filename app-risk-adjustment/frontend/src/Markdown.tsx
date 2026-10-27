import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

export function Markdown({ children }: { children: string }) {
  return (
    <div className="md">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{children || ''}</ReactMarkdown>
    </div>
  );
}

export function StepList({ steps }: { steps: { tool: string; purpose: string; summary: string }[] }) {
  if (!steps || steps.length === 0)
    return <div className="empty">No agent steps captured for this turn.</div>;
  return (
    <div className="steps">
      {steps.map((s, i) => (
        <div className="step" key={i}>
          <div className="num">{i + 1}</div>
          <div style={{ flex: 1 }}>
            <div>
              <span className="tool">{s.tool}</span>&nbsp;&middot;&nbsp;
              <span className="purpose">{s.purpose}</span>
            </div>
            <div className="summary">
              → <b>{s.summary}</b>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
