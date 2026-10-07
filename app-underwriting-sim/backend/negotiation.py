"""Negotiation / re-rate loop.

Phase 3 of the underwriting vision roadmap. An underwriter (or AE on the group's
behalf) re-rates a saved quote with a natural-language change — "raise the
specific attachment to $300K", "drop the admin fee by $5", "+200 lives" — and the
agent translates it into concrete parameter overrides, re-prices through the same
deterministic funding-arrangement engine, and records the revision. The engine
stays authoritative; the LLM only maps language to parameters.
"""

from .data_loader import DataCache
from .funding_arrangements import price_funding_arrangement
from .llm import complete_json

# Parameters the re-rate loop may change, and which are numeric.
_TEXT_PARAMS = {"group_id", "lob", "group_name", "disability_product"}
_ALLOWED_PARAMS = _TEXT_PARAMS | {
    "member_count",
    # fully_insured
    "admin_load_pct", "margin_pct", "trend_pct",
    # aso
    "aso_admin_fee_pmpm", "specific_attachment", "aggregate_corridor_pct",
    "specific_sl_premium_pmpm", "aggregate_sl_premium_pmpm",
    # level_funded
    "admin_fee_pmpm", "claims_margin_pct", "surplus_share_pct",
    # specialty
    "manual_pmpm", "target_loss_ratio", "credibility",
    # stop_loss
    "sl_target_loss_ratio",
    # disability
    "avg_monthly_salary", "occupation_class",
}


def _sanitize(changes: dict) -> dict:
    """Keep only allowed params; coerce numeric params to float."""
    clean: dict = {}
    for k, v in (changes or {}).items():
        if k not in _ALLOWED_PARAMS or v is None:
            continue
        if k in _TEXT_PARAMS:
            clean[k] = str(v)
        else:
            try:
                clean[k] = float(v)
            except (ValueError, TypeError):
                continue
    return clean


def rerate_quote(cache: DataCache, quote: dict, instruction: str) -> dict:
    """Apply an NL change to a saved quote and re-price it.

    Returns {param_changes, merged_inputs, result}.
    """
    arrangement = quote.get("funding_arrangement", "fully_insured")
    inputs = dict(quote.get("inputs") or {})

    system = (
        "You translate an underwriter's natural-language pricing change into JSON parameter "
        "overrides for a group health funding quote. Only output parameters that actually change. "
        f"Allowed parameter keys: {sorted(_ALLOWED_PARAMS)}. "
        "Values must be numbers (not strings) except for group_id/lob/group_name/disability_product. "
        "For relative changes (e.g. '+200 lives', 'drop fee by 5'), compute the new absolute value "
        "from the current inputs."
    )
    user = (
        f"Funding arrangement: {arrangement}\n"
        f"Current inputs: {inputs}\n"
        f"Requested change: {instruction}\n\n"
        'Respond as {"param_changes": { ... }}.'
    )
    parsed = complete_json(system, user, max_tokens=600)
    changes = _sanitize(parsed.get("param_changes", {}) if isinstance(parsed, dict) else {})

    merged = {**inputs, **changes}
    result = price_funding_arrangement(cache, arrangement, merged)
    return {
        "param_changes": changes,
        "merged_inputs": merged,
        "result": result,
    }
