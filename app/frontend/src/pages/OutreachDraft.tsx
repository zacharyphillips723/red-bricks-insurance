/**
 * Outreach Drafting — AI-generated personalized outreach scripts.
 */

import { useState } from "react";
import {
  Mail,
  Phone,
  MessageSquare,
  Sparkles,
  Loader2,
  Search,
  Copy,
  Check,
  RefreshCw,
} from "lucide-react";
import { api, type MemberListItem } from "@/lib/api";

const CHANNELS = [
  { id: "phone", label: "Phone", icon: Phone },
  { id: "email", label: "Email", icon: Mail },
  { id: "sms", label: "SMS", icon: MessageSquare },
] as const;

export function OutreachDraft() {
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<MemberListItem[]>([]);
  const [selectedMember, setSelectedMember] = useState<MemberListItem | null>(null);
  const [channel, setChannel] = useState<string>("phone");
  const [context, setContext] = useState("");
  const [loading, setLoading] = useState(false);
  const [draft, setDraft] = useState<{
    subject: string | null;
    script: string;
    key_talking_points: string[];
    member_name: string;
    generated_at: string;
  } | null>(null);
  const [copied, setCopied] = useState(false);

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    try {
      const results = await api.searchMembers(searchQuery);
      setSearchResults(results);
    } catch {
      setSearchResults([]);
    }
  };

  const selectMember = (member: MemberListItem) => {
    setSelectedMember(member);
    setSearchResults([]);
    setSearchQuery("");
    setDraft(null);
  };

  const generateDraft = async () => {
    if (!selectedMember) return;
    setLoading(true);
    try {
      const result = await api.generateOutreachDraft(
        selectedMember.member_id,
        channel,
        context || undefined,
      );
      setDraft(result);
    } catch (e) {
      console.error("Failed to generate outreach draft:", e);
    } finally {
      setLoading(false);
    }
  };

  const copyToClipboard = () => {
    if (!draft) return;
    const text = draft.subject
      ? `Subject: ${draft.subject}\n\n${draft.script}`
      : draft.script;
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div>
      <h2 className="text-2xl font-bold text-gray-800 flex items-center gap-2 mb-1">
        <Mail className="w-6 h-6 text-red-600" /> Outreach Drafting
      </h2>
      <p className="text-sm text-gray-500 mb-6">
        AI-generated personalized outreach scripts for phone, email, and SMS
      </p>

      {/* Search */}
      <div className="relative mb-6 max-w-xl">
        <input
          type="text"
          placeholder="Search by member name or ID..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-red-200 focus:border-red-400 outline-none"
        />
        <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
        {searchResults.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 bg-white border rounded-lg shadow-lg z-10 max-h-48 overflow-y-auto">
            {searchResults.map((m) => (
              <button
                key={m.member_id}
                onClick={() => selectMember(m)}
                className="w-full text-left px-4 py-2 hover:bg-gray-50 text-sm border-b last:border-b-0"
              >
                <span className="font-medium">{m.member_name || [m.first_name, m.last_name].filter(Boolean).join(" ") || m.member_id}</span>
                <span className="text-gray-400 ml-2">{m.member_id}</span>
              </button>
            ))}
          </div>
        )}
      </div>

      {selectedMember && (
        <div className="space-y-4">
          {/* Member header + config */}
          <div className="bg-white rounded-xl border p-4">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="font-semibold text-lg">
                  {selectedMember.member_name || [selectedMember.first_name, selectedMember.last_name].filter(Boolean).join(" ") || selectedMember.member_id}
                </h3>
                <p className="text-sm text-gray-500">
                  {selectedMember.member_id} | {selectedMember.gender} | Age {selectedMember.age} |{" "}
                  {selectedMember.risk_tier}
                </p>
              </div>
            </div>

            {/* Channel selection */}
            <div className="flex gap-2 mb-4">
              {CHANNELS.map((ch) => {
                const Icon = ch.icon;
                return (
                  <button
                    key={ch.id}
                    onClick={() => { setChannel(ch.id); setDraft(null); }}
                    className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border transition-colors ${
                      channel === ch.id
                        ? "bg-red-50 border-red-300 text-red-700"
                        : "bg-white border-gray-200 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    {ch.label}
                  </button>
                );
              })}
            </div>

            {/* Context */}
            <textarea
              placeholder="Optional: Add context (e.g., 'Member missed last appointment', 'Follow up on HbA1c results')..."
              value={context}
              onChange={(e) => setContext(e.target.value)}
              rows={2}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm resize-none focus:ring-2 focus:ring-red-200 focus:border-red-400 outline-none mb-4"
            />

            <button
              onClick={generateDraft}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm font-medium"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
              {loading ? "Generating..." : draft ? "Regenerate" : "Generate Draft"}
            </button>
          </div>

          {/* Draft Output */}
          {draft && (
            <div className="bg-white rounded-xl border p-5">
              <div className="flex items-center justify-between mb-4">
                <h4 className="font-semibold text-gray-800">
                  {channel === "email" ? "Email Draft" : channel === "sms" ? "SMS Draft" : "Phone Script"}
                </h4>
                <div className="flex gap-2">
                  <button
                    onClick={copyToClipboard}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
                  >
                    {copied ? <Check className="w-3.5 h-3.5 text-green-500" /> : <Copy className="w-3.5 h-3.5" />}
                    {copied ? "Copied" : "Copy"}
                  </button>
                  <button
                    onClick={generateDraft}
                    disabled={loading}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
                  >
                    <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
                    Regenerate
                  </button>
                </div>
              </div>

              {draft.subject && (
                <div className="mb-3">
                  <span className="text-xs font-medium text-gray-500 uppercase">Subject</span>
                  <p className="text-sm font-medium text-gray-800">{draft.subject}</p>
                </div>
              )}

              <div className="bg-gray-50 rounded-lg p-4 mb-4">
                <pre className="text-sm text-gray-700 whitespace-pre-wrap font-sans leading-relaxed">
                  {draft.script}
                </pre>
              </div>

              {draft.key_talking_points.length > 0 && (
                <div>
                  <span className="text-xs font-medium text-gray-500 uppercase">Key Talking Points</span>
                  <ul className="mt-1 space-y-1">
                    {draft.key_talking_points.map((pt, i) => (
                      <li key={i} className="flex items-start gap-2 text-sm text-gray-600">
                        <span className="text-red-500 mt-0.5">&#8226;</span>
                        {pt}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <p className="text-xs text-gray-400 mt-4">
                Generated {new Date(draft.generated_at).toLocaleString()}
              </p>
            </div>
          )}
        </div>
      )}

      {!selectedMember && (
        <div className="text-center py-16 text-gray-400">
          <Mail className="w-12 h-12 mx-auto mb-3 opacity-50" />
          <h3 className="font-semibold text-lg text-gray-500">Search for a member</h3>
          <p className="text-sm">Select a member to generate personalized outreach scripts</p>
        </div>
      )}
    </div>
  );
}
