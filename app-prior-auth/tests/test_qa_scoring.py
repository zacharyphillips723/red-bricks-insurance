"""Offline unit tests for QA scorecard computation (backend/qa_scoring.py)."""

from backend import qa_scoring as qa

QUESTIONS = [
    {"question_id": "q1", "weight": 25, "is_critical": True},
    {"question_id": "q2", "weight": 20, "is_critical": True},
    {"question_id": "q3", "weight": 15, "is_critical": False},
    {"question_id": "q4", "weight": 15, "is_critical": False},
    {"question_id": "q5", "weight": 15, "is_critical": False},
    {"question_id": "q6", "weight": 10, "is_critical": False},
]


def test_perfect_score_passes():
    awarded = {q["question_id"]: q["weight"] for q in QUESTIONS}
    r = qa.compute_qa_score(QUESTIONS, awarded)
    assert r["total_score"] == 100.0
    assert r["max_score"] == 100.0
    assert r["score_pct"] == 100.0
    assert r["critical_error"] is False
    assert r["passed"] is True


def test_critical_failure_auto_fails_even_if_high_pct():
    # Miss only q2 (critical, weight 20) -> 80% and a critical error.
    awarded = {q["question_id"]: q["weight"] for q in QUESTIONS}
    awarded["q2"] = 0
    r = qa.compute_qa_score(QUESTIONS, awarded)
    assert r["critical_error"] is True
    assert r["passed"] is False


def test_below_threshold_fails_without_critical():
    # Lose both non-critical 15-pt questions -> 70%, no critical error.
    awarded = {q["question_id"]: q["weight"] for q in QUESTIONS}
    awarded["q3"] = 0
    awarded["q4"] = 0
    r = qa.compute_qa_score(QUESTIONS, awarded)
    assert r["score_pct"] == 70.0
    assert r["critical_error"] is False
    assert r["passed"] is False


def test_na_questions_excluded_from_denominator():
    # Answer only q1 (25) + q6 (10) fully; others N/A -> 100% of 35 possible.
    awarded = {"q1": 25, "q6": 10}
    r = qa.compute_qa_score(QUESTIONS, awarded)
    assert r["max_score"] == 35.0
    assert r["total_score"] == 35.0
    assert r["score_pct"] == 100.0
    assert r["passed"] is True


def test_partial_credit_and_clamping():
    awarded = {"q1": 20, "q2": 20, "q3": 15, "q4": 15, "q5": 15, "q6": 99}  # q6 over-max, q1 partial
    r = qa.compute_qa_score(QUESTIONS, awarded)
    # q6 clamped 10; q1 partial 20/25 -> critical q1 not full => critical_error
    assert r["total_score"] == 95.0
    assert r["critical_error"] is True
    assert r["passed"] is False


def test_no_answers_returns_none_pct():
    r = qa.compute_qa_score(QUESTIONS, {})
    assert r["score_pct"] is None
    assert r["passed"] is False
