"""Pure QA scorecard computation (RFI: Quality Assurance — weighted questions,
critical-error logic, pass/fail). No Databricks deps → unit-testable offline.

Scoring model:
  - Each active question has a `weight` and an `is_critical` flag.
  - `awarded` maps question_id -> points earned (0..weight). A question omitted
    from `awarded` (or awarded None) is treated as N/A and excluded from the
    denominator (RFI: not-applicable responses).
  - critical_error = any critical question that did NOT earn full weight.
  - passed = score_pct >= pass_threshold AND no critical_error (critical failure
    auto-fails regardless of the weighted percentage).
"""

from typing import Any

DEFAULT_PASS_THRESHOLD = 90.0


def compute_qa_score(
    questions: list[dict[str, Any]],
    awarded: dict[str, Any],
    pass_threshold: float = DEFAULT_PASS_THRESHOLD,
) -> dict[str, Any]:
    """Compute a QA scorecard result from question weights + awarded points."""
    total = 0.0
    max_score = 0.0
    critical_error = False

    for q in questions:
        qid = str(q.get("question_id"))
        weight = float(q.get("weight") or 0)
        is_critical = bool(q.get("is_critical"))

        if qid not in awarded or awarded[qid] is None:
            continue  # N/A — excluded from denominator

        pts = float(awarded[qid])
        pts = max(0.0, min(pts, weight))  # clamp to [0, weight]
        total += pts
        max_score += weight
        if is_critical and pts < weight:
            critical_error = True

    score_pct = round(total / max_score * 100, 2) if max_score > 0 else None
    passed = bool(score_pct is not None and score_pct >= pass_threshold and not critical_error)

    return {
        "total_score": round(total, 2),
        "max_score": round(max_score, 2),
        "score_pct": score_pct,
        "critical_error": critical_error,
        "passed": passed,
    }
