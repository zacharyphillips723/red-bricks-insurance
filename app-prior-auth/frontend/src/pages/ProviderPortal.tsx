import { useState, useEffect, useCallback } from "react";
import { api, PortalProvider, PortalRequest, Correspondence } from "@/lib/api";
import { Building2, Send, FileText, Inbox, Upload, CheckCircle, ClipboardList } from "lucide-react";
import ReactMarkdown from "react-markdown";

const NPI_KEY = "pa.portal.npi";

function statusColor(s: string | null): string {
  if (!s) return "bg-gray-100 text-gray-600";
  if (s === "Approved" || s === "Appeal Overturned") return "bg-green-50 text-green-700";
  if (s === "Denied" || s === "Appeal Upheld") return "bg-red-50 text-red-700";
  if (s === "Additional Info Requested") return "bg-orange-50 text-orange-700";
  if (s === "Pending Review") return "bg-amber-50 text-amber-700";
  return "bg-blue-50 text-blue-700";
}

type Tab = "submit" | "requests" | "letters";

export function ProviderPortal() {
  const [providers, setProviders] = useState<PortalProvider[]>([]);
  const [npi, setNpi] = useState<string>(() => {
    try { return localStorage.getItem(NPI_KEY) || ""; } catch { return ""; }
  });
  const [tab, setTab] = useState<Tab>("requests");

  useEffect(() => {
    api.listPortalProviders().then((p) => {
      setProviders(p);
      if (!npi && p.length) selectNpi(p[0].requesting_provider_npi);
    }).catch(console.error);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const selectNpi = (n: string) => {
    setNpi(n);
    try { localStorage.setItem(NPI_KEY, n); } catch { /* ignore */ }
  };

  const current = providers.find((p) => p.requesting_provider_npi === npi);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Building2 size={22} className="text-databricks-dark" />
          <h2 className="text-2xl font-bold text-databricks-dark">Provider Portal</h2>
        </div>
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-500">Signed in as</span>
          <select
            value={npi}
            onChange={(e) => selectNpi(e.target.value)}
            className="border border-gray-300 rounded-md px-2 py-1.5"
          >
            {providers.map((p) => (
              <option key={p.requesting_provider_npi} value={p.requesting_provider_npi}>
                {p.provider_name || p.requesting_provider_npi} (NPI {p.requesting_provider_npi})
              </option>
            ))}
          </select>
        </div>
      </div>
      <p className="text-sm text-gray-500 -mt-2">
        External self-service for requesting providers: submit prior authorizations, track status,
        respond to information requests, and retrieve decision letters. Scoped to the signed-in NPI.
      </p>

      <div className="flex gap-1 border-b border-gray-200">
        {([["requests", "My Requests", ClipboardList], ["submit", "Submit Request", Send], ["letters", "Decision Letters", FileText]] as const).map(
          ([id, label, Icon]) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              className={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 -mb-px ${
                tab === id ? "border-databricks-red text-databricks-red" : "border-transparent text-gray-500 hover:text-gray-700"
              }`}
            >
              <Icon size={15} /> {label}
            </button>
          ),
        )}
      </div>

      {!npi ? (
        <div className="card text-gray-400">Select a provider to continue.</div>
      ) : tab === "submit" ? (
        <SubmitTab provider={current} onSubmitted={() => setTab("requests")} />
      ) : tab === "requests" ? (
        <RequestsTab npi={npi} />
      ) : (
        <LettersTab npi={npi} />
      )}
    </div>
  );
}

function SubmitTab({ provider, onSubmitted }: { provider?: PortalProvider; onSubmitted: () => void }) {
  const [form, setForm] = useState({
    member_id: "", member_name: "", service_type: "imaging", procedure_code: "",
    procedure_description: "", diagnosis_codes: "", line_of_business: "Commercial",
    urgency: "standard", clinical_summary: "",
  });
  const [busy, setBusy] = useState(false);
  const [tracking, setTracking] = useState<string | null>(null);
  const set = (k: string, v: string) => setForm((f) => ({ ...f, [k]: v }));

  const submit = async () => {
    if (!provider) return;
    setBusy(true);
    try {
      const created = await api.submitPortalRequest({
        ...form,
        requesting_provider_npi: provider.requesting_provider_npi,
        provider_name: provider.provider_name,
      });
      setTracking(created.auth_request_id);
      setTimeout(onSubmitted, 1800);
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (tracking) {
    return (
      <div className="card border-l-4 border-l-green-400">
        <div className="flex items-center gap-2 text-green-700 font-semibold">
          <CheckCircle size={18} /> Prior authorization submitted
        </div>
        <p className="text-sm text-gray-600 mt-1">
          Tracking number: <span className="font-mono font-medium">{tracking}</span>. You can track
          its status under “My Requests”.
        </p>
      </div>
    );
  }

  return (
    <div className="card grid grid-cols-2 gap-3 max-w-3xl">
      <Field label="Member ID"><input className={inp} value={form.member_id} onChange={(e) => set("member_id", e.target.value)} /></Field>
      <Field label="Member Name"><input className={inp} value={form.member_name} onChange={(e) => set("member_name", e.target.value)} /></Field>
      <Field label="Service Type">
        <select className={inp} value={form.service_type} onChange={(e) => set("service_type", e.target.value)}>
          {["imaging", "surgery", "dme", "behavioral_health", "pharmacy", "cardiology"].map((s) => <option key={s}>{s}</option>)}
        </select>
      </Field>
      <Field label="Urgency">
        <select className={inp} value={form.urgency} onChange={(e) => set("urgency", e.target.value)}>
          <option value="standard">standard</option>
          <option value="expedited">expedited</option>
        </select>
      </Field>
      <Field label="Procedure Code (CPT/HCPCS)"><input className={inp} value={form.procedure_code} onChange={(e) => set("procedure_code", e.target.value)} /></Field>
      <Field label="Procedure Description"><input className={inp} value={form.procedure_description} onChange={(e) => set("procedure_description", e.target.value)} /></Field>
      <Field label="Diagnosis Codes (ICD-10, pipe-separated)"><input className={inp} value={form.diagnosis_codes} onChange={(e) => set("diagnosis_codes", e.target.value)} /></Field>
      <Field label="Line of Business">
        <select className={inp} value={form.line_of_business} onChange={(e) => set("line_of_business", e.target.value)}>
          {["Commercial", "Medicare Advantage", "Medicaid", "ACA Marketplace"].map((s) => <option key={s}>{s}</option>)}
        </select>
      </Field>
      <div className="col-span-2">
        <Field label="Clinical Summary">
          <textarea className={`${inp} h-24`} value={form.clinical_summary} onChange={(e) => set("clinical_summary", e.target.value)} />
        </Field>
      </div>
      <div className="col-span-2">
        <button onClick={submit} disabled={busy || !form.member_id || !form.procedure_code} className="btn-primary disabled:opacity-40 flex items-center gap-1">
          <Send size={15} /> {busy ? "Submitting…" : "Submit Prior Authorization"}
        </button>
      </div>
    </div>
  );
}

function RequestsTab({ npi }: { npi: string }) {
  const [requests, setRequests] = useState<PortalRequest[]>([]);
  const [respondFor, setRespondFor] = useState<string | null>(null);
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api.listPortalRequests(npi).then(setRequests).catch(console.error);
  }, [npi]);
  useEffect(() => { load(); }, [load]);

  const respond = async (id: string) => {
    setBusy(true);
    try {
      await api.respondPortalRFI(id, note || "Additional clinical documentation submitted.");
      setRespondFor(null); setNote(""); load();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (!requests.length) return <div className="card text-gray-400">No requests on file for this provider.</div>;

  return (
    <div className="card p-0 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Tracking #</th><th className="px-4 py-3">Member</th>
            <th className="px-4 py-3">Service</th><th className="px-4 py-3">Status</th>
            <th className="px-4 py-3">Reason</th><th className="px-4 py-3"></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {requests.map((r) => (
            <>
              <tr key={r.auth_request_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs">{r.auth_request_id}</td>
                <td className="px-4 py-3">{r.member_name || "—"}</td>
                <td className="px-4 py-3">{r.service_type}<div className="text-xs text-gray-400 font-mono">{r.procedure_code}</div></td>
                <td className="px-4 py-3"><span className={`px-2 py-0.5 rounded text-xs font-medium ${statusColor(r.status)}`}>{r.status}</span></td>
                <td className="px-4 py-3 text-xs text-gray-500">{r.determination_reason || (r.denial_reason_code ?? "")}</td>
                <td className="px-4 py-3">
                  {r.needs_response && (
                    <button onClick={() => setRespondFor(respondFor === r.auth_request_id ? null : r.auth_request_id)} className="text-xs text-orange-700 flex items-center gap-1">
                      <Upload size={12} /> Respond
                    </button>
                  )}
                </td>
              </tr>
              {respondFor === r.auth_request_id && (
                <tr key={`${r.auth_request_id}-r`}>
                  <td colSpan={6} className="px-4 py-3 bg-orange-50/40">
                    <div className="flex gap-2 items-start">
                      <Inbox size={16} className="text-orange-600 mt-1.5" />
                      <textarea value={note} onChange={(e) => setNote(e.target.value)} placeholder="Describe the additional information / documentation you are submitting…" className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-sm h-16" />
                      <button onClick={() => respond(r.auth_request_id)} disabled={busy} className="btn-primary text-sm">Submit Response</button>
                    </div>
                  </td>
                </tr>
              )}
            </>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LettersTab({ npi }: { npi: string }) {
  const [requests, setRequests] = useState<PortalRequest[]>([]);
  const [letters, setLetters] = useState<Record<string, Correspondence[]>>({});

  useEffect(() => {
    api.listPortalRequests(npi).then(async (reqs) => {
      setRequests(reqs);
      const map: Record<string, Correspondence[]> = {};
      for (const r of reqs.slice(0, 40)) {
        try { const ls = await api.getPortalLetters(r.auth_request_id); if (ls.length) map[r.auth_request_id] = ls; }
        catch { /* ignore */ }
      }
      setLetters(map);
    }).catch(console.error);
  }, [npi]);

  const withLetters = requests.filter((r) => letters[r.auth_request_id]?.length);
  if (!withLetters.length) return <div className="card text-gray-400">No decision letters have been released yet.</div>;

  return (
    <div className="space-y-3">
      {withLetters.map((r) => letters[r.auth_request_id].map((l) => (
        <div key={l.notice_id} className="card">
          <div className="flex items-center gap-2 mb-2">
            <FileText size={15} className="text-blue-500" />
            <span className="font-medium capitalize">{l.notice_type.replace(/_/g, " ")}</span>
            <span className="font-mono text-xs text-gray-400">{r.auth_request_id}</span>
          </div>
          <div className="prose prose-sm max-w-none bg-gray-50 rounded p-3 max-h-72 overflow-y-auto">
            <ReactMarkdown>{l.body_markdown || ""}</ReactMarkdown>
          </div>
        </div>
      )))}
    </div>
  );
}

const inp = "w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm";
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return <label className="block"><span className="text-xs text-gray-500">{label}</span>{children}</label>;
}
