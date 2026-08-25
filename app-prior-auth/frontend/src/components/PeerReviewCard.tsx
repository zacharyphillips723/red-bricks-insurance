import { useState, useEffect, useCallback } from "react";
import { api, PeerReview, Reviewer } from "@/lib/api";
import { Stethoscope, UserCheck, Phone, CheckCircle } from "lucide-react";

interface PeerReviewCardProps {
  requestId: string;
  reviewers: Reviewer[];
  onChanged?: () => void;
}

function statusPill(s: string | null): string {
  if (s === "Determination Made") return "bg-green-50 text-green-700";
  if (s === "P2P Completed") return "bg-blue-50 text-blue-700";
  if (s === "Scheduled") return "bg-amber-50 text-amber-700";
  return "bg-gray-100 text-gray-600";
}

export function PeerReviewCard({ requestId, reviewers, onChanged }: PeerReviewCardProps) {
  const [peerReviews, setPeerReviews] = useState<PeerReview[]>([]);
  const [specialty, setSpecialty] = useState("");
  const [peerReviewerId, setPeerReviewerId] = useState("");
  const [reason, setReason] = useState("");
  const [p2p, setP2p] = useState(false);
  const [busy, setBusy] = useState(false);
  const [determination, setDetermination] = useState("Uphold denial");
  const [detNotes, setDetNotes] = useState("");

  const load = useCallback(() => {
    api.listPeerReviews(requestId).then(setPeerReviews).catch(console.error);
  }, [requestId]);
  useEffect(() => { load(); }, [load]);

  // Physician reviewers only (Medical Director / Peer Reviewer).
  const physicians = reviewers.filter(
    (r) => r.role === "Medical Director" || r.role === "Peer Reviewer",
  );

  const handleRequest = async () => {
    setBusy(true);
    try {
      await api.requestPeerReview(requestId, {
        peer_reviewer_id: peerReviewerId || undefined,
        requested_specialty: specialty || undefined,
        reason: reason || undefined,
        p2p_requested: p2p,
      });
      setReason("");
      load();
      onChanged?.();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const handleDecide = async (peerReviewId: string) => {
    setBusy(true);
    try {
      await api.decidePeerReview(peerReviewId, {
        determination,
        determination_notes: detNotes || undefined,
        p2p_summary: p2p ? "Peer-to-peer discussion completed." : undefined,
      });
      setDetNotes("");
      load();
      onChanged?.();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const openReview = peerReviews.find((p) => p.status !== "Determination Made");

  return (
    <div className="card">
      <h3 className="font-semibold text-databricks-dark mb-3 flex items-center gap-2">
        <Stethoscope size={16} className="text-purple-500" /> Physician / Peer Review
      </h3>

      {/* Existing peer reviews */}
      {peerReviews.length > 0 && (
        <div className="space-y-2 mb-4">
          {peerReviews.map((p) => (
            <div key={p.peer_review_id} className="border border-gray-200 rounded-md p-3 text-sm space-y-1">
              <div className="flex items-center gap-2">
                <UserCheck size={14} className="text-gray-400" />
                <span className="font-medium">{p.peer_reviewer_name || "Awaiting specialty match"}</span>
                {p.peer_reviewer_role && <span className="text-xs text-gray-400">{p.peer_reviewer_role}</span>}
                {p.p2p_requested && (
                  <span className="text-xs text-blue-600 flex items-center gap-0.5">
                    <Phone size={11} /> P2P
                  </span>
                )}
                <span className={`ml-auto px-2 py-0.5 rounded text-xs font-medium ${statusPill(p.status)}`}>
                  {p.status}
                </span>
              </div>
              {p.requested_specialty && (
                <div className="text-xs text-gray-500">Specialty: {p.requested_specialty}</div>
              )}
              {p.reason && <div className="text-xs text-gray-600">{p.reason}</div>}
              {p.determination ? (
                <div className="flex items-center gap-1 text-xs text-green-700 mt-1">
                  <CheckCircle size={12} /> {p.determination}
                  {p.determination_notes && <span className="text-gray-500">— {p.determination_notes}</span>}
                </div>
              ) : (
                <div className="mt-2 border-t border-gray-100 pt-2 space-y-2">
                  <div className="flex gap-2">
                    <select
                      value={determination}
                      onChange={(e) => setDetermination(e.target.value)}
                      className="flex-1 border border-gray-300 rounded-md px-2 py-1 text-xs"
                    >
                      <option>Uphold denial</option>
                      <option>Overturn — approve</option>
                      <option>Overturn — partial approval</option>
                    </select>
                    <button
                      onClick={() => handleDecide(p.peer_review_id)}
                      disabled={busy}
                      className="btn-primary text-xs px-3"
                    >
                      Record
                    </button>
                  </div>
                  <input
                    value={detNotes}
                    onChange={(e) => setDetNotes(e.target.value)}
                    placeholder="Peer reviewer notes / P2P outcome…"
                    className="w-full border border-gray-300 rounded-md px-2 py-1 text-xs"
                  />
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Request a new peer review */}
      {!openReview && (
        <div className="space-y-2">
          <div className="flex gap-2">
            <select
              value={peerReviewerId}
              onChange={(e) => setPeerReviewerId(e.target.value)}
              className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-sm"
            >
              <option value="">Match by specialty…</option>
              {physicians.map((r) => (
                <option key={r.reviewer_id} value={r.reviewer_id}>
                  {r.display_name} — {r.specialty || r.role}
                </option>
              ))}
            </select>
            <input
              value={specialty}
              onChange={(e) => setSpecialty(e.target.value)}
              placeholder="Specialty"
              className="w-32 border border-gray-300 rounded-md px-2 py-1.5 text-sm"
            />
          </div>
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="Reason for escalation…"
            className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm"
          />
          <label className="flex items-center gap-2 text-sm text-gray-600">
            <input type="checkbox" checked={p2p} onChange={(e) => setP2p(e.target.checked)} />
            Request peer-to-peer (P2P) discussion
          </label>
          <button onClick={handleRequest} disabled={busy} className="btn-secondary text-sm w-full">
            {busy ? "Escalating…" : "Escalate to Peer Review"}
          </button>
        </div>
      )}
    </div>
  );
}
