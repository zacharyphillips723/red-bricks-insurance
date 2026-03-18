import { useState, useRef, useEffect } from "react";
import {
  Search,
  UserCircle,
  ShieldAlert,
  Activity,
  FileText,
  Send,
  Loader2,
  ChevronDown,
  ChevronUp,
  Phone,
  ClipboardList,
  Heart,
  AlertTriangle,
  Calendar,
  DollarSign,
  Sparkles,
} from "lucide-react";
import {
  api,
  type MemberListItem,
  type Member360Detail,
  type CaseNote,
  type AgentResponse,
} from "@/lib/api";

const SUGGESTED_QUESTIONS = [
  "Summarize this member's care history",
  "What are the key risk factors?",
  "Describe recent outreach attempts",
  "What HEDIS gaps need attention?",
  "Summarize recent claims activity",
  "What medications have been discussed?",
];

// ---------------------------------------------------------------------------
// Risk tier badge
// ---------------------------------------------------------------------------
function RiskBadge({ tier }: { tier: string | null }) {
  const colors: Record<string, string> = {
    Critical: "bg-red-100 text-red-800 border-red-200",
    High: "bg-orange-100 text-orange-800 border-orange-200",
    Elevated: "bg-yellow-100 text-yellow-800 border-yellow-200",
    Moderate: "bg-green-100 text-green-800 border-green-200",
  };
  return (
    <span
      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold border ${
        colors[tier ?? ""] || "bg-gray-100 text-gray-600 border-gray-200"
      }`}
    >
      {tier || "Unknown"}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Document type badge
// ---------------------------------------------------------------------------
function DocTypeBadge({ type }: { type: string | null }) {
  const cfg: Record<string, { icon: typeof FileText; color: string; label: string }> = {
    case_note: { icon: ClipboardList, color: "text-blue-600 bg-blue-50", label: "Case Note" },
    call_transcript: { icon: Phone, color: "text-purple-600 bg-purple-50", label: "Call Transcript" },
    claims_summary: { icon: DollarSign, color: "text-emerald-600 bg-emerald-50", label: "Claims Summary" },
  };
  const c = cfg[type ?? ""] || { icon: FileText, color: "text-gray-600 bg-gray-50", label: type || "Document" };
  const Icon = c.icon;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${c.color}`}>
      <Icon className="w-3 h-3" /> {c.label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// RAF Gauge — simple visual gauge
// ---------------------------------------------------------------------------
function RafGauge({ score }: { score: number }) {
  const maxScore = 5;
  const pct = Math.min((score / maxScore) * 100, 100);
  const color =
    score > 3 ? "#dc2626" : score > 2.5 ? "#ea580c" : score > 2 ? "#ca8a04" : "#16a34a";
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-gray-500">
        <span>0</span>
        <span className="font-semibold text-sm" style={{ color }}>
          {score.toFixed(2)}
        </span>
        <span>{maxScore}+</span>
      </div>
      <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export function Member360() {
  // Search state
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<MemberListItem[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);

  // Selected member state
  const [member, setMember] = useState<Member360Detail | null>(null);
  const [caseNotes, setCaseNotes] = useState<CaseNote[]>([]);
  const [memberLoading, setMemberLoading] = useState(false);

  // Agent chat state
  const [agentQuestion, setAgentQuestion] = useState("");
  const [agentMessages, setAgentMessages] = useState<
    { role: "user" | "agent"; text: string }[]
  >([]);
  const [agentLoading, setAgentLoading] = useState(false);
  const chatBottomRef = useRef<HTMLDivElement>(null);

  // Expanded case notes
  const [expandedNotes, setExpandedNotes] = useState<Set<string>>(new Set());

  // Search members
  useEffect(() => {
    if (!searchQuery.trim() || searchQuery.length < 2) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }
    const timeout = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const results = await api.searchMembers(searchQuery);
        setSearchResults(results);
        setShowDropdown(true);
      } catch {
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);
    return () => clearTimeout(timeout);
  }, [searchQuery]);

  // Select a member
  const selectMember = async (memberId: string) => {
    setShowDropdown(false);
    setSearchQuery("");
    setMemberLoading(true);
    setAgentMessages([]);
    try {
      const [profile, notes] = await Promise.all([
        api.getMember360(memberId),
        api.getCaseNotes(memberId),
      ]);
      setMember(profile);
      setCaseNotes(notes);
    } catch (err) {
      console.error("Failed to load member:", err);
    } finally {
      setMemberLoading(false);
    }
  };

  // Ask agent
  const handleAskAgent = async (q?: string) => {
    const text = q || agentQuestion;
    if (!text.trim() || !member) return;

    setAgentMessages((prev) => [...prev, { role: "user", text }]);
    setAgentQuestion("");
    setAgentLoading(true);

    try {
      const response = await api.queryMemberAgent(member.member_id, text);
      setAgentMessages((prev) => [...prev, { role: "agent", text: response.answer }]);
    } catch (err) {
      setAgentMessages((prev) => [
        ...prev,
        { role: "agent", text: `Error: ${err instanceof Error ? err.message : "Unknown error"}` },
      ]);
    } finally {
      setAgentLoading(false);
      setTimeout(() => chatBottomRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
    }
  };

  const toggleNote = (docId: string) => {
    setExpandedNotes((prev) => {
      const next = new Set(prev);
      if (next.has(docId)) next.delete(docId);
      else next.add(docId);
      return next;
    });
  };

  const rafScore = member ? parseFloat(member.raf_score || "0") : 0;

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      {/* Header + Search */}
      <div className="mb-4">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <UserCircle className="w-6 h-6 text-databricks-red" /> Member 360
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Full member context for care management outreach
        </p>

        {/* Search bar */}
        <div className="relative mt-3 max-w-xl">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => searchResults.length > 0 && setShowDropdown(true)}
            placeholder="Search by member name or ID (e.g., MBR100042 or Smith)..."
            className="w-full pl-10 pr-4 py-2.5 rounded-xl border border-gray-300 text-sm
                       focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          />
          {searchLoading && (
            <Loader2 className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 animate-spin" />
          )}

          {/* Dropdown */}
          {showDropdown && searchResults.length > 0 && (
            <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded-xl shadow-lg max-h-72 overflow-y-auto">
              {searchResults.map((m) => (
                <button
                  key={m.member_id}
                  onClick={() => selectMember(m.member_id)}
                  className="w-full text-left px-4 py-3 hover:bg-gray-50 border-b border-gray-100 last:border-0 flex items-center justify-between"
                >
                  <div>
                    <span className="font-medium text-sm text-databricks-dark">
                      {m.member_name || `${m.first_name} ${m.last_name}`}
                    </span>
                    <span className="text-xs text-gray-400 ml-2">{m.member_id}</span>
                    <div className="text-xs text-gray-500 mt-0.5">
                      {m.gender} | Age {m.age} | {m.line_of_business} | {m.county}
                    </div>
                  </div>
                  <RiskBadge tier={m.risk_tier} />
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Loading state */}
      {memberLoading && (
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
        </div>
      )}

      {/* Empty state */}
      {!member && !memberLoading && (
        <div className="flex-1 flex items-center justify-center">
          <div className="text-center">
            <UserCircle className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-gray-400 mb-2">Search for a member</h3>
            <p className="text-sm text-gray-400">
              Enter a member name or ID to load their full 360 profile
            </p>
          </div>
        </div>
      )}

      {/* Member loaded */}
      {member && !memberLoading && (
        <div className="flex-1 overflow-y-auto space-y-4">
          {/* Top row: left panel + right chat */}
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
            {/* LEFT PANEL: Profile cards */}
            <div className="lg:col-span-3 space-y-4">
              {/* Demographics */}
              <div className="card p-5">
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <h3 className="text-lg font-bold text-databricks-dark">
                      {member.member_name}
                    </h3>
                    <p className="text-sm text-gray-500">{member.member_id}</p>
                  </div>
                  <RiskBadge tier={member.risk_tier} />
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                  <div>
                    <span className="text-gray-400 text-xs">Age / Gender</span>
                    <p className="font-medium">{member.age} / {member.gender}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">DOB</span>
                    <p className="font-medium">{member.date_of_birth}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Location</span>
                    <p className="font-medium">{member.city}, {member.state} {member.zip_code}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">County</span>
                    <p className="font-medium">{member.county}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Phone</span>
                    <p className="font-medium">{member.phone || "N/A"}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Email</span>
                    <p className="font-medium truncate">{member.email || "N/A"}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">LOB / Plan</span>
                    <p className="font-medium">{member.line_of_business} — {member.plan_type}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Group</span>
                    <p className="font-medium truncate">{member.group_name || "N/A"}</p>
                  </div>
                </div>
              </div>

              {/* Risk Profile */}
              <div className="card p-5">
                <h4 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                  <ShieldAlert className="w-4 h-4 text-databricks-red" /> Risk Profile
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <span className="text-gray-400 text-xs">RAF Score</span>
                    <RafGauge score={rafScore} />
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">HCC Count</span>
                    <p className="text-2xl font-bold text-databricks-dark">{member.hcc_count || 0}</p>
                  </div>
                  <div className="md:col-span-2">
                    <span className="text-gray-400 text-xs">HCC Codes</span>
                    <div className="flex flex-wrap gap-1 mt-1">
                      {(member.hcc_codes || "").split(",").filter(Boolean).map((code) => (
                        <span
                          key={code.trim()}
                          className="px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-xs font-mono"
                        >
                          {code.trim()}
                        </span>
                      ))}
                      {!member.hcc_codes && <span className="text-gray-400 text-xs">None</span>}
                    </div>
                  </div>
                </div>
              </div>

              {/* Claims Summary */}
              <div className="card p-5">
                <h4 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                  <DollarSign className="w-4 h-4 text-databricks-red" /> Claims Summary
                </h4>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                  <div>
                    <span className="text-gray-400 text-xs">Medical Claims</span>
                    <p className="text-xl font-bold text-databricks-dark">{member.medical_claim_count || 0}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Medical Paid YTD</span>
                    <p className="text-xl font-bold text-databricks-dark">
                      ${parseFloat(member.medical_total_paid_ytd || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Rx Claims</span>
                    <p className="text-xl font-bold text-databricks-dark">{member.pharmacy_claim_count || 0}</p>
                  </div>
                  <div>
                    <span className="text-gray-400 text-xs">Rx Spend YTD</span>
                    <p className="text-xl font-bold text-databricks-dark">
                      ${parseFloat(member.pharmacy_spend_ytd || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </p>
                  </div>
                </div>
                {member.top_diagnoses && (
                  <div className="mt-3">
                    <span className="text-gray-400 text-xs">Top Diagnoses</span>
                    <p className="text-sm text-gray-700 mt-1">{member.top_diagnoses}</p>
                  </div>
                )}
              </div>

              {/* HEDIS Gaps */}
              <div className="card p-5">
                <h4 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                  <Heart className="w-4 h-4 text-databricks-red" /> Quality / HEDIS Gaps
                </h4>
                <div className="flex items-center gap-4">
                  <div>
                    <span className="text-gray-400 text-xs">Open Gaps</span>
                    <p className="text-2xl font-bold text-databricks-dark">{member.hedis_gap_count || 0}</p>
                  </div>
                  <div className="flex-1">
                    <span className="text-gray-400 text-xs">Gap Measures</span>
                    <div className="flex flex-wrap gap-1 mt-1">
                      {(member.hedis_gap_measures || "")
                        .split(",")
                        .filter(Boolean)
                        .map((m) => (
                          <span
                            key={m.trim()}
                            className="px-2 py-0.5 bg-amber-50 text-amber-700 border border-amber-200 rounded text-xs"
                          >
                            <AlertTriangle className="w-3 h-3 inline mr-1" />
                            {m.trim()}
                          </span>
                        ))}
                      {!member.hedis_gap_measures && (
                        <span className="text-green-600 text-xs">All measures compliant</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              {/* Recent Encounters */}
              {member.last_encounter_date && (
                <div className="card p-5">
                  <h4 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                    <Calendar className="w-4 h-4 text-databricks-red" /> Last Encounter
                  </h4>
                  <div className="grid grid-cols-3 gap-3 text-sm">
                    <div>
                      <span className="text-gray-400 text-xs">Date</span>
                      <p className="font-medium">{member.last_encounter_date}</p>
                    </div>
                    <div>
                      <span className="text-gray-400 text-xs">Type</span>
                      <p className="font-medium capitalize">{member.last_encounter_type}</p>
                    </div>
                    <div>
                      <span className="text-gray-400 text-xs">PCP NPI</span>
                      <p className="font-medium font-mono text-xs">{member.pcp_npi || "N/A"}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* RIGHT PANEL: Agent Chat */}
            <div className="lg:col-span-2 flex flex-col card">
              <div className="px-4 py-3 border-b border-gray-200">
                <h4 className="text-sm font-semibold text-databricks-dark flex items-center gap-2">
                  <Sparkles className="w-4 h-4 text-databricks-red" /> Care Intelligence Agent
                </h4>
                <p className="text-xs text-gray-400">
                  Ask about this member's history, risk factors, or care needs
                </p>
              </div>

              {/* Chat messages */}
              <div className="flex-1 overflow-y-auto p-4 space-y-3 min-h-[300px] max-h-[500px]">
                {agentMessages.length === 0 && !agentLoading && (
                  <div className="space-y-2">
                    <p className="text-xs text-gray-400 mb-3">Suggested questions:</p>
                    {SUGGESTED_QUESTIONS.map((q) => (
                      <button
                        key={q}
                        onClick={() => handleAskAgent(q)}
                        className="block w-full text-left px-3 py-2 rounded-lg border border-gray-200
                                   hover:border-databricks-red hover:bg-red-50 transition-colors text-xs text-gray-600"
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                )}

                {agentMessages.map((msg, idx) => (
                  <div key={idx} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                    <div
                      className={`max-w-[90%] rounded-2xl px-3 py-2 text-sm whitespace-pre-wrap ${
                        msg.role === "user"
                          ? "bg-databricks-dark text-white rounded-tr-sm"
                          : "bg-gray-100 text-gray-800 rounded-tl-sm"
                      }`}
                    >
                      {msg.text}
                    </div>
                  </div>
                ))}

                {agentLoading && (
                  <div className="flex items-center gap-2 text-xs text-gray-400">
                    <Loader2 className="w-4 h-4 animate-spin text-databricks-red" />
                    Agent is thinking...
                  </div>
                )}
                <div ref={chatBottomRef} />
              </div>

              {/* Chat input */}
              <div className="p-3 border-t border-gray-200">
                <div className="flex gap-2">
                  <input
                    value={agentQuestion}
                    onChange={(e) => setAgentQuestion(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleAskAgent()}
                    placeholder="Ask about this member..."
                    className="flex-1 px-3 py-2 rounded-lg border border-gray-300 text-sm
                               focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
                    disabled={agentLoading}
                  />
                  <button
                    onClick={() => handleAskAgent()}
                    disabled={!agentQuestion.trim() || agentLoading}
                    className="btn-primary px-3 py-2"
                  >
                    <Send className="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          </div>

          {/* Bottom: Case Notes Timeline */}
          <div className="card p-5">
            <h4 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
              <FileText className="w-4 h-4 text-databricks-red" /> Documents & Case Notes
              <span className="text-xs font-normal text-gray-400">({caseNotes.length} documents)</span>
            </h4>

            {caseNotes.length === 0 && (
              <p className="text-sm text-gray-400">No documents found for this member.</p>
            )}

            <div className="space-y-3">
              {caseNotes.map((note) => {
                const isExpanded = expandedNotes.has(note.document_id);
                return (
                  <div key={note.document_id} className="border border-gray-200 rounded-lg overflow-hidden">
                    <button
                      onClick={() => toggleNote(note.document_id)}
                      className="w-full flex items-center justify-between px-4 py-3 hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <DocTypeBadge type={note.document_type} />
                        <span className="text-sm font-medium text-databricks-dark">{note.title}</span>
                        <span className="text-xs text-gray-400">{note.created_date}</span>
                        <span className="text-xs text-gray-400">by {note.author}</span>
                      </div>
                      {isExpanded ? (
                        <ChevronUp className="w-4 h-4 text-gray-400" />
                      ) : (
                        <ChevronDown className="w-4 h-4 text-gray-400" />
                      )}
                    </button>
                    {isExpanded && (
                      <div className="px-4 pb-4 border-t border-gray-100">
                        <pre className="text-xs text-gray-700 whitespace-pre-wrap font-mono bg-gray-50 rounded-lg p-3 mt-3 max-h-64 overflow-y-auto">
                          {note.full_text}
                        </pre>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
