import { useState, useEffect, useRef } from "react";
import {
  ClipboardEdit, Loader2, Plus, X, Play, Search, FileText, CheckCircle2,
} from "lucide-react";
import { api } from "@/lib/api";
import type {
  DraftClaim, ClaimLine, MemberSearchItem, SampleDraft, ScrubResult, ScrubStreamEvent,
} from "@/lib/api";

interface Props {
  onScrubComplete: (result: ScrubResult, draft: DraftClaim) => void;
  initialDraft?: DraftClaim | null;
}

const EMPTY_DRAFT: DraftClaim = {
  member_id: "",
  provider_npi: "",
  date_of_service: new Date().toISOString().slice(0, 10),
  request_type: "claim",
  lines: [{ cpt: "", units: 1, pos: "11" }],
  dx_codes: [],
  clinical_notes: "",
  billed_amount: null,
};

const STAGES = [
  { key: "eligibility", label: "Eligibility" },
  { key: "rules", label: "Coding · Auth · Limits" },
  { key: "ml", label: "Denial-prediction model" },
  { key: "rag", label: "Medical-policy RAG" },
  { key: "composing", label: "Composing risk score" },
];

export function ComposeClaim({ onScrubComplete, initialDraft }: Props) {
  // Preload the draft when editing an existing scrub (from the Scrub Result page);
  // otherwise start from an empty draft.
  const [draft, setDraft] = useState<DraftClaim>(
    initialDraft ? { ...EMPTY_DRAFT, ...initialDraft } : EMPTY_DRAFT,
  );
  const [samples, setSamples] = useState<SampleDraft[]>([]);
  const [memberQuery, setMemberQuery] = useState(initialDraft?.member_id ?? "");
  const [memberResults, setMemberResults] = useState<MemberSearchItem[]>([]);
  const [memberName, setMemberName] = useState<string>("");
  const [showMemberList, setShowMemberList] = useState(false);
  const [dxInput, setDxInput] = useState("");
  const [running, setRunning] = useState(false);
  const [activeStage, setActiveStage] = useState("");
  const [done, setDone] = useState<Set<string>>(new Set());
  const [statusMsg, setStatusMsg] = useState("");
  const [errorMsg, setErrorMsg] = useState("");
  const searchTimer = useRef<number | undefined>(undefined);

  useEffect(() => {
    api.getSamples().then((r) => setSamples(r.samples)).catch(() => {});
  }, []);

  // Debounced member search.
  useEffect(() => {
    if (memberQuery.trim().length < 2) {
      setMemberResults([]);
      return;
    }
    window.clearTimeout(searchTimer.current);
    searchTimer.current = window.setTimeout(() => {
      api.searchMembers(memberQuery.trim())
        .then(setMemberResults)
        .catch(() => setMemberResults([]));
    }, 250);
  }, [memberQuery]);

  const update = (patch: Partial<DraftClaim>) => setDraft((d) => ({ ...d, ...patch }));

  const setLine = (idx: number, patch: Partial<ClaimLine>) =>
    update({ lines: draft.lines.map((ln, i) => (i === idx ? { ...ln, ...patch } : ln)) });
  const addLine = () => update({ lines: [...draft.lines, { cpt: "", units: 1, pos: "11" }] });
  const removeLine = (idx: number) =>
    update({ lines: draft.lines.filter((_, i) => i !== idx) });

  const addDx = () => {
    const code = dxInput.trim().toUpperCase();
    if (code && !draft.dx_codes.includes(code)) update({ dx_codes: [...draft.dx_codes, code] });
    setDxInput("");
  };
  const removeDx = (code: string) =>
    update({ dx_codes: draft.dx_codes.filter((c) => c !== code) });

  const pickMember = (m: MemberSearchItem) => {
    update({ member_id: m.member_id, line_of_business: m.line_of_business ?? null });
    setMemberName(m.member_name || "");
    setMemberQuery(`${m.member_id}${m.member_name ? " — " + m.member_name : ""}`);
    setShowMemberList(false);
  };

  const loadSample = (scenario: string) => {
    const s = samples.find((x) => x.scenario === scenario);
    if (!s) return;
    setDraft({ ...EMPTY_DRAFT, ...s.draft });
    setMemberName("");
    setMemberQuery(s.draft.member_id);
    setErrorMsg("");
  };

  const runScrub = async () => {
    if (!draft.member_id) { setErrorMsg("Select a member first."); return; }
    setRunning(true);
    setErrorMsg("");
    setStatusMsg("");
    setActiveStage("");
    setDone(new Set());
    const completed = new Set<string>();
    try {
      await api.runScrubStream(draft, (ev: ScrubStreamEvent) => {
        if (ev.type === "status") {
          setActiveStage((prev) => {
            if (prev) { completed.add(prev); setDone(new Set(completed)); }
            return ev.stage;
          });
          setStatusMsg(ev.message);
        } else if (ev.type === "result") {
          const { type: _t, ...res } = ev;
          onScrubComplete(res as ScrubResult, draft);
        } else if (ev.type === "error") {
          setErrorMsg(ev.message);
        }
      });
    } catch (e) {
      setErrorMsg(String(e));
    } finally {
      setRunning(false);
      setActiveStage("");
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <ClipboardEdit className="text-databricks-red" /> Compose Claim
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Build a draft claim or prior-authorization request and scrub it for denial risk
          <span className="font-medium"> before</span> submitting it to the payer.
        </p>
      </div>

      {/* Sample loader */}
      <div className="card p-4 flex flex-wrap items-center gap-3">
        <FileText className="w-4 h-4 text-databricks-red" />
        <span className="text-sm font-medium text-gray-600">Load a sample draft:</span>
        <select
          className="border border-gray-300 rounded-md px-3 py-1.5 text-sm min-w-[280px]"
          defaultValue=""
          onChange={(e) => e.target.value && loadSample(e.target.value)}
        >
          <option value="" disabled>Choose a scenario…</option>
          {samples.map((s) => (
            <option key={s.scenario} value={s.scenario}>{s.title} — {s.expected}</option>
          ))}
        </select>
      </div>

      {/* Form */}
      <div className="card p-6 space-y-5">
        {/* Request type toggle */}
        <div className="flex gap-2">
          {(["claim", "prior_auth"] as const).map((rt) => (
            <button
              key={rt}
              onClick={() => update({ request_type: rt })}
              className={`px-4 py-2 rounded-md text-sm font-medium border transition-colors ${
                draft.request_type === rt
                  ? "bg-databricks-red text-white border-databricks-red"
                  : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {rt === "claim" ? "Medical Claim" : "Prior-Auth Request"}
            </button>
          ))}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Member autocomplete */}
          <div className="relative">
            <label className="block text-xs font-semibold text-gray-500 mb-1">MEMBER</label>
            <div className="relative">
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-2.5" />
              <input
                className="w-full border border-gray-300 rounded-md pl-9 pr-3 py-2 text-sm"
                placeholder="Search member id or name…"
                value={memberQuery}
                onChange={(e) => { setMemberQuery(e.target.value); setShowMemberList(true); }}
                onFocus={() => setShowMemberList(true)}
              />
            </div>
            {showMemberList && memberResults.length > 0 && (
              <div className="absolute z-10 mt-1 w-full bg-white border border-gray-200 rounded-md shadow-lg max-h-60 overflow-y-auto">
                {memberResults.map((m) => (
                  <button
                    key={m.member_id}
                    onClick={() => pickMember(m)}
                    className="w-full text-left px-3 py-2 text-sm hover:bg-gray-50 flex justify-between"
                  >
                    <span className="font-medium">{m.member_id}</span>
                    <span className="text-gray-500">{m.member_name}</span>
                  </button>
                ))}
              </div>
            )}
            {memberName && (
              <p className="text-xs text-gray-500 mt-1">
                {memberName}{draft.line_of_business ? ` · ${draft.line_of_business}` : ""}
              </p>
            )}
          </div>

          <div>
            <label className="block text-xs font-semibold text-gray-500 mb-1">PROVIDER NPI</label>
            <input
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              placeholder="10-digit NPI"
              value={draft.provider_npi}
              onChange={(e) => update({ provider_npi: e.target.value })}
            />
          </div>

          <div>
            <label className="block text-xs font-semibold text-gray-500 mb-1">DATE OF SERVICE</label>
            <input
              type="date"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              value={draft.date_of_service}
              onChange={(e) => update({ date_of_service: e.target.value })}
            />
          </div>

          <div>
            <label className="block text-xs font-semibold text-gray-500 mb-1">BILLED AMOUNT (optional)</label>
            <input
              type="number"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              placeholder="0.00"
              value={draft.billed_amount ?? ""}
              onChange={(e) => update({ billed_amount: e.target.value ? Number(e.target.value) : null })}
            />
          </div>
        </div>

        {/* CPT lines */}
        <div>
          <label className="block text-xs font-semibold text-gray-500 mb-2">PROCEDURE LINES</label>
          <div className="space-y-2">
            {draft.lines.map((ln, i) => (
              <div key={i} className="flex items-center gap-2">
                <input
                  className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm"
                  placeholder="CPT / HCPCS"
                  value={ln.cpt}
                  onChange={(e) => setLine(i, { cpt: e.target.value })}
                />
                <input
                  type="number"
                  className="w-20 border border-gray-300 rounded-md px-3 py-2 text-sm"
                  placeholder="Units"
                  value={ln.units}
                  onChange={(e) => setLine(i, { units: Number(e.target.value) || 1 })}
                />
                <input
                  className="w-24 border border-gray-300 rounded-md px-3 py-2 text-sm"
                  placeholder="POS"
                  value={ln.pos ?? ""}
                  onChange={(e) => setLine(i, { pos: e.target.value })}
                />
                <button
                  onClick={() => removeLine(i)}
                  disabled={draft.lines.length === 1}
                  className="p-2 text-gray-400 hover:text-databricks-red disabled:opacity-30"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            ))}
          </div>
          <button onClick={addLine} className="mt-2 text-sm text-databricks-red flex items-center gap-1 font-medium">
            <Plus className="w-4 h-4" /> Add line
          </button>
        </div>

        {/* Diagnosis chips */}
        <div>
          <label className="block text-xs font-semibold text-gray-500 mb-2">DIAGNOSIS CODES (ICD-10)</label>
          <div className="flex flex-wrap gap-2 mb-2">
            {draft.dx_codes.map((c) => (
              <span key={c} className="inline-flex items-center gap-1 bg-gray-100 rounded-full px-3 py-1 text-sm">
                {c}
                <button onClick={() => removeDx(c)} className="text-gray-400 hover:text-databricks-red">
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
          <div className="flex gap-2">
            <input
              className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm"
              placeholder="e.g. M17.11"
              value={dxInput}
              onChange={(e) => setDxInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), addDx())}
            />
            <button onClick={addDx} className="btn-secondary">Add</button>
          </div>
        </div>

        {/* Clinical notes */}
        <div>
          <label className="block text-xs font-semibold text-gray-500 mb-1">CLINICAL NOTES (optional)</label>
          <textarea
            rows={4}
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
            placeholder="Clinical documentation supporting medical necessity…"
            value={draft.clinical_notes ?? ""}
            onChange={(e) => update({ clinical_notes: e.target.value })}
          />
        </div>

        {errorMsg && (
          <div className="text-sm text-red-700 bg-red-50 border border-red-200 rounded-md px-3 py-2">
            {errorMsg}
          </div>
        )}

        <button onClick={runScrub} disabled={running} className="btn-primary flex items-center gap-2">
          {running ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
          {running ? "Scrubbing…" : "Run Scrub"}
        </button>
      </div>

      {/* Live progress */}
      {(running || done.size > 0) && (
        <div className="card p-6">
          <h3 className="font-semibold text-databricks-dark mb-4">Scrub pipeline</h3>
          <div className="space-y-3">
            {STAGES.map((s) => {
              const isDone = done.has(s.key);
              const isActive = activeStage === s.key;
              return (
                <div key={s.key} className="flex items-center gap-3">
                  {isDone ? (
                    <CheckCircle2 className="w-5 h-5 text-green-600" />
                  ) : isActive ? (
                    <Loader2 className="w-5 h-5 text-databricks-red animate-spin" />
                  ) : (
                    <div className="w-5 h-5 rounded-full border-2 border-gray-200" />
                  )}
                  <span className={`text-sm ${isActive ? "font-semibold text-databricks-dark" : isDone ? "text-gray-700" : "text-gray-400"}`}>
                    {s.label}
                  </span>
                </div>
              );
            })}
          </div>
          {statusMsg && <p className="text-xs text-gray-500 mt-4">{statusMsg}</p>}
        </div>
      )}
    </div>
  );
}
