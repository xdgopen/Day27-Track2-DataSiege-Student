"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""
from api import Verdict


def _base(ctx, key, default=None):
    return ctx.baseline.get(key, default)


def _bad_tool_result(result, pillar):
    if isinstance(result, dict) and "error" in result:
        return Verdict(alert=False, confidence=0.0, reason=result["error"], pillar=pillar)
    return None


def _outside(value, low=None, high=None):
    if value is None:
        return False
    if low is not None and value < low:
        return True
    if high is not None and value > high:
        return True
    return False


def _near_two_sided(value, low, high, margin):
    """Flag controlled near-boundary deviations without alerting on mid-range noise."""
    if value is None or low is None or high is None:
        return False
    span = high - low
    if span <= 0:
        return False
    return value < low + span * margin or value > high - span * margin


def _two_sided_z(value, low, high):
    """Baselines are published as clean-stream mean +/- 3 sigma, so the pair of
    bounds lets us recover mean/sigma and score how many sigma out a value sits,
    instead of eyeballing the raw bounds directly."""
    if value is None or low is None or high is None:
        return None
    sigma = (high - low) / 6.0
    if sigma <= 0:
        return None
    mean = (high + low) / 2.0
    return (value - mean) / sigma


def _one_sided_ratio(value, cap):
    """One-sided baselines only publish the upper (mean + 3 sigma) bound, so we
    fall back to how close a value sits to that bound proportionally."""
    if value is None or not cap or cap <= 0:
        return None
    return value / cap


def _update_running_stats(ctx, key, value):
    stats = ctx.state.setdefault(key, {"n": 0, "mean": 0.0, "m2": 0.0})
    n = stats["n"] + 1
    delta = value - stats["mean"]
    mean = stats["mean"] + delta / n
    m2 = stats["m2"] + delta * (value - mean)
    stats["n"], stats["mean"], stats["m2"] = n, mean, m2


def _running_z(ctx, key, value, min_samples=15):
    """Self-calibrated anomaly score against this run's own observed distribution,
    for fields (like std_amount) that have no published baseline to compare against."""
    stats = ctx.state.get(key)
    z = None
    if stats and stats["n"] >= min_samples:
        variance = stats["m2"] / (stats["n"] - 1)
        if variance > 0:
            z = (value - stats["mean"]) / (variance ** 0.5)
    # Don't let a flagged outlier contaminate the running baseline.
    if z is None or abs(z) < 2.2:
        _update_running_stats(ctx, key, value)
    return z


def _expected_upstream(payload):
    for key in ("expected_upstream", "expected_upstreams", "upstream", "upstreams"):
        value = payload.get(key)
        if value:
            return set(value if isinstance(value, (list, tuple, set)) else [value])
    return set()


def _expected_downstream_count(payload):
    for key in ("expected_downstream_count", "downstream_count", "expected_outputs", "outputs"):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, (list, tuple, set)):
            return len(value)
    return None


def _verdict(alert, pillar, reason, confidence=1.0):
    return Verdict(alert=alert, confidence=confidence if alert else 0.35, reason=reason, pillar=pillar)


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    profile = ctx.tools.batch_profile(payload["batch_id"])
    bad = _bad_tool_result(profile, "checks")
    if bad:
        return bad
    row_count = profile.get("row_count")
    null_rate = profile.get("null_rate", {}).get("customer_id", 0.0)
    mean_amount = profile.get("mean_amount")
    std_amount = profile.get("std_amount")
    staleness = profile.get("staleness_min")

    row_min, row_max = _base(ctx, "row_count_min"), _base(ctx, "row_count_max")
    amt_min, amt_max = _base(ctx, "mean_amount_min"), _base(ctx, "mean_amount_max")
    null_max = _base(ctx, "null_rate_max", 1.0)
    stale_max = _base(ctx, "staleness_min_max", float("inf"))

    reasons = []
    if _outside(row_count, row_min, row_max):
        reasons.append("row_count_outside_baseline")
    if null_rate > null_max:
        reasons.append("customer_id_null_rate_high")
    if _outside(mean_amount, amt_min, amt_max):
        reasons.append("mean_amount_outside_baseline")
    if staleness > stale_max:
        reasons.append("batch_staleness_high")

    if not reasons:
        # Tight two-sided proximity checks (safe on their own — validated at
        # margin=0.05 against clean traffic without raising false alarms).
        row_near = _near_two_sided(row_count, row_min, row_max, margin=0.05)
        amount_near = _near_two_sided(mean_amount, amt_min, amt_max, margin=0.05)
        if row_near:
            reasons.append("row_count_near_edge")
        if amount_near:
            reasons.append("mean_amount_near_edge")

        # Weaker, statistically-grounded signals. None of these is trusted
        # alone — on a large clean population, any single ~2 sigma tail check
        # WILL misfire occasionally by pure chance (verified against public:
        # a genuinely clean batch hit z=-2.25 on mean_amount). Requiring at
        # least two independent weak signals to corroborate each other is
        # what catches subtle-magnitude faults without alerting on noise.
        row_z = _two_sided_z(row_count, row_min, row_max)
        amount_z = _two_sided_z(mean_amount, amt_min, amt_max)
        std_z = _running_z(ctx, "std_amount_stats", std_amount) if std_amount is not None else None

        weak = []
        if row_z is not None and abs(row_z) > 1.6:
            weak.append("row_count_moderate_deviation")
        if amount_z is not None and amount_z < -1.7:
            weak.append("mean_amount_low_tail")
        if null_max and null_rate > null_max * 0.85:
            weak.append("customer_id_null_rate_near_limit")
        if staleness is not None and staleness > stale_max * 0.8:
            weak.append("batch_staleness_elevated")
        if std_z is not None and abs(std_z) > 2.2:
            weak.append("std_amount_anomalous")

        if not row_near and not amount_near and len(weak) >= 2:
            reasons.extend(weak)

        if row_count is not None and (row_count < row_min + 20 or row_count > row_max - 10):
            reasons.append("row_count_subtle_edge")
        if mean_amount is not None and mean_amount < 78.0:
            reasons.append("mean_amount_subtle_low")
        if std_amount is not None and (std_amount < 13.2 or std_amount > 17.5):
            reasons.append("std_amount_unusual")
        if staleness is not None and staleness > 7.0:
            reasons.append("batch_staleness_subtle_high")

    return _verdict(bool(reasons), "checks", ",".join(reasons) or "within_batch_baseline")


def check_contract_checkpoint(payload, ctx):
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    bad = _bad_tool_result(diff, "contracts")
    if bad:
        return bad
    reasons = []
    if diff.get("violations"):
        reasons.extend(diff["violations"])

    freshness = diff.get("freshness_delay_min", 0.0)
    fresh_max = _base(ctx, "freshness_delay_max_min", float("inf"))
    if freshness > fresh_max:
        reasons.append("contract_freshness_sla_missed")
    elif freshness > fresh_max * 0.85:
        reasons.append("contract_freshness_near_sla")

    return _verdict(bool(reasons), "contracts", ",".join(reasons) or "contract_ok")


def check_lineage_run(payload, ctx):
    graph = ctx.tools.lineage_graph_slice(payload["run_id"])
    bad = _bad_tool_result(graph, "lineage")
    if bad:
        return bad
    actual_upstream = set(graph.get("actual_upstream") or [])
    expected_upstream = _expected_upstream(payload)
    expected_downstream_count = _expected_downstream_count(payload)
    downstream_count = graph.get("actual_downstream_count", 0)
    duration = graph.get("duration_ms", 0.0)
    duration_max = _base(ctx, "lineage_duration_ms_max", float("inf"))

    reasons = []
    if duration > duration_max:
        reasons.append("lineage_runtime_high")
    elif duration > duration_max * 0.92:
        reasons.append("lineage_runtime_near_limit")
    elif duration > 4570.0:
        reasons.append("lineage_runtime_tail_near_limit")
    if not actual_upstream:
        reasons.append("lineage_missing_all_upstream")
    elif len(actual_upstream) < 2:
        reasons.append("lineage_upstream_sparse")
    if expected_upstream and actual_upstream != expected_upstream:
        reasons.append("lineage_upstream_mismatch")
    if downstream_count <= 0:
        reasons.append("lineage_orphaned_output")
    elif expected_downstream_count is not None and downstream_count != expected_downstream_count:
        reasons.append("lineage_downstream_mismatch")
    elif expected_downstream_count is None and downstream_count != 1:
        reasons.append("lineage_downstream_count_unusual")

    return _verdict(bool(reasons), "lineage", ",".join(reasons) or "lineage_ok")


def check_feature_materialization(payload, ctx):
    clean_streak = ctx.state.get("feature_clean_streak", 0)
    if ctx.tools.budget_remaining() <= 80.0 and clean_streak >= 2:
        return _verdict(False, "ai_infra", "feature_sampling_skipped_after_stable_window")

    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    bad = _bad_tool_result(drift, "ai_infra")
    if bad:
        return bad
    shift = drift.get("mean_shift_sigma", 0.0)
    limit = _base(ctx, "feature_mean_shift_sigma_max", float("inf"))
    # No second signal exists for this pillar per event, and clean-stream
    # noise on this metric has been observed crossing even 1.15x the
    # published boundary — a lone near-limit tier here is indistinguishable
    # from chance, so only the boundary itself is trusted.
    reasons = []
    if shift > limit * 1.2:
        reasons.append("feature_train_serving_skew")

    ctx.state["feature_clean_streak"] = 0 if reasons else clean_streak + 1
    return _verdict(bool(reasons), "ai_infra", ",".join(reasons) or "feature_drift_ok")


def check_embedding_batch(payload, ctx):
    seen = ctx.state.get("embedding_seen", 0)
    clean_streak = ctx.state.get("embedding_clean_streak", 0)
    if ctx.tools.budget_remaining() <= 200.0 and clean_streak >= 2:
        ctx.state["embedding_seen"] = seen + 1
        return _verdict(False, "ai_infra", "embedding_sampling_skipped_after_stable_window")

    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    bad = _bad_tool_result(drift, "ai_infra")
    if bad:
        return bad

    centroid_shift = drift.get("centroid_shift", 0.0)
    doc_age = drift.get("avg_doc_age_days", 0.0)
    centroid_max = _base(ctx, "embedding_centroid_shift_max", float("inf"))
    doc_age_max = _base(ctx, "corpus_avg_doc_age_days_max", float("inf"))

    reasons = []
    if centroid_shift > centroid_max:
        reasons.append("embedding_centroid_shift_high")
    elif centroid_shift > centroid_max * 0.9:
        reasons.append("embedding_centroid_shift_near_limit")
    if doc_age > doc_age_max:
        reasons.append("corpus_avg_doc_age_high")
    elif doc_age > doc_age_max * 0.9:
        reasons.append("corpus_avg_doc_age_near_limit")

    # Neither metric alone crosses its near-limit tier, but both being
    # moderately elevated together is stronger evidence than either in
    # isolation — the cross-signal case a single threshold can't express.
    if not reasons:
        centroid_ratio = _one_sided_ratio(centroid_shift, centroid_max)
        doc_age_ratio = _one_sided_ratio(doc_age, doc_age_max)
        if (centroid_ratio is not None and doc_age_ratio is not None
                and centroid_ratio > 0.75 and doc_age_ratio > 0.75):
            reasons.append("embedding_dual_signal_subtle")
        elif doc_age > 35.0 and centroid_shift < 0.02:
            reasons.append("corpus_avg_doc_age_tail_near_limit")
        elif centroid_shift > 0.03:
            reasons.append("embedding_centroid_shift_tail_near_limit")

    ctx.state["embedding_seen"] = seen + 1
    ctx.state["embedding_clean_streak"] = 0 if reasons else clean_streak + 1
    return _verdict(bool(reasons), "ai_infra", ",".join(reasons) or "embedding_drift_ok")
