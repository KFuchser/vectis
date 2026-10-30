"""
Offline tests for the Gemini cost controls. No network, no Gemini, no Supabase: the Gemini
and Supabase clients are replaced with in-memory fakes.
"""
import json
import os
from types import SimpleNamespace

import pytest

# Dummy credentials so importing the orchestrator never touches real services (clients are
# constructed lazily/offline, then replaced by fakes below). Forced, not setdefault.
os.environ["SUPABASE_URL"] = "https://example.supabase.co"
os.environ["SUPABASE_KEY"] = "dummy-key"
os.environ["GOOGLE_API_KEY"] = "dummy-key"

import gemini_config as gc
import ingest_velocity_50 as ing
from service_models import PermitRecord, ComplexityTier


# ---------- helpers ----------

def usage_md(prompt=1000, out=500, thoughts=0):
    return SimpleNamespace(prompt_token_count=prompt, candidates_token_count=out,
                           thoughts_token_count=thoughts)


class FakeModels:
    def __init__(self, handler):
        self.handler = handler
        self.calls = 0
        self.prompts = []

    def generate_content(self, model, contents, config):
        self.calls += 1
        self.prompts.append(contents)
        return self.handler(self.calls, contents)


class FakeAI:
    def __init__(self, handler):
        self.models = FakeModels(handler)


def echo_commercial(n_call, prompt):
    """Answers every 'Input ID k' in the prompt as Commercial."""
    ids = [int(line.split("Input ID ")[1].split(":")[0])
           for line in prompt.splitlines() if "Input ID " in line]
    body = [{"id": i, "tier": "Commercial", "category": "Retail", "rationale": "t"} for i in ids]
    return SimpleNamespace(text=json.dumps(body), usage_metadata=usage_md())


def rec(pid, desc, val=100000.0, city="Austin"):
    return PermitRecord(permit_id=pid, city=city, description=desc, valuation=val)


@pytest.fixture(autouse=True)
def fresh_usage(monkeypatch):
    monkeypatch.setattr(ing, "usage", gc.UsageTracker("test"))
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 1.00)
    monkeypatch.setattr(ing, "MAX_RUN_COST_USD", 1.00)
    monkeypatch.delenv("ALLOW_COST_OVERRIDE", raising=False)
    monkeypatch.setattr(ing.time, "sleep", lambda *_: None)


def no_existing(_records):
    return {}


# ---------- gemini_config ----------

def test_classification_config_disables_thinking_and_caps_output():
    cfg = gc.classification_config()
    assert cfg.response_mime_type == "application/json"
    assert cfg.max_output_tokens == gc.CLASSIFICATION_MAX_OUTPUT_TOKENS
    if gc.thinking_supported():
        assert cfg.thinking_config.thinking_budget == 0


def test_classification_config_passes_overrides():
    assert gc.classification_config(system_instruction="x").system_instruction == "x"


def test_usage_tracker_counts_tokens_and_thinking():
    t = gc.UsageTracker("t")
    t.record(SimpleNamespace(usage_metadata=usage_md(1000, 200, 300)))
    assert (t.calls, t.input_tokens, t.output_tokens, t.thinking_tokens) == (1, 1000, 200, 300)
    # thinking is billed at the output rate
    assert t.cost_usd == pytest.approx(gc.estimate_cost_usd(1000, 500))
    assert "1 call(s)" in t.summary()


def test_usage_tracker_handles_missing_usage_metadata():
    t = gc.UsageTracker("t")
    t.record(SimpleNamespace(usage_metadata=None), 1000, 1000)
    t.record(SimpleNamespace())  # attribute absent entirely
    assert t.calls == 2 and t.calls_without_usage == 2
    assert t.cost_usd > 0  # charged at the fallback estimate, cap cannot be dodged


def test_check_cap_raises_over_cap(monkeypatch):
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 0.001)
    t = gc.UsageTracker("t")
    t.record(SimpleNamespace(usage_metadata=usage_md(100000, 100000)))
    with pytest.raises(gc.CostCapExceeded):
        t.check_cap()
    assert issubclass(gc.CostCapExceeded, gc.AIClassificationError)


def test_projected_cost_guard_and_override(monkeypatch):
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 0.01)
    with pytest.raises(gc.CostCapExceeded):
        gc.check_projected_cost("t", 1000, 2500, 2000)
    monkeypatch.setenv("ALLOW_COST_OVERRIDE", "1")
    assert gc.check_projected_cost("t", 1000, 2500, 2000) > 0.01


# ---------- lookback ----------

def test_lookback_defaults_env_and_cli_precedence():
    assert ing.resolve_lookback_days([], {}) == 3
    assert ing.resolve_lookback_days([], {"INGEST_LOOKBACK_DAYS": "90"}) == 90
    assert ing.resolve_lookback_days([], {"INGEST_LOOKBACK_DAYS": ""}) == 3
    assert ing.resolve_lookback_days(["--days", "7"], {"INGEST_LOOKBACK_DAYS": "90"}) == 7
    with pytest.raises(ValueError):
        ing.resolve_lookback_days(["--days", "0"], {})


# ---------- orchestrator classification ----------

def test_prefilter_still_resolves_without_ai(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    out = ing.process_and_classify_permits(
        [rec("1", "Replace roof", 3000), rec("2", "single family home remodel", 10000)],
        existing_lookup=no_existing)
    assert fake.models.calls == 0
    assert [r.complexity_tier for r in out] == [ComplexityTier.COMMODITY, ComplexityTier.RESIDENTIAL]


def test_skips_already_classified_and_preserves_stored_tier(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    stored = {("Austin", "1"): {"city": "Austin", "permit_id": "1", "complexity_tier": "Commercial",
                                "project_category": "Commercial - New", "ai_rationale": "old"},
              ("Austin", "2"): {"city": "Austin", "permit_id": "2", "complexity_tier": "Unknown",
                                "project_category": None, "ai_rationale": None}}
    out = ing.process_and_classify_permits(
        [rec("1", "Big office tower"), rec("2", "Hospital wing")],
        existing_lookup=lambda recs: stored)
    assert fake.models.calls == 1
    assert "Big office tower" not in fake.models.prompts[0]  # skipped row never sent
    assert "Hospital wing" in fake.models.prompts[0]
    by_id = {r.permit_id: r for r in out}
    assert by_id["1"].complexity_tier == ComplexityTier.COMMERCIAL  # kept, not reset to Unknown
    assert by_id["1"].ai_rationale == "old"


def test_second_run_makes_zero_ai_calls(monkeypatch):
    """Everything classified on run 1 is skipped on run 2 (simulated via the stored map)."""
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    run1 = [rec(str(i), f"Office build {i}") for i in range(5)]
    ing.process_and_classify_permits(run1, existing_lookup=no_existing)
    assert fake.models.calls == 1
    stored = {(r.city, r.permit_id): {"city": r.city, "permit_id": r.permit_id,
                                      "complexity_tier": "Commercial",
                                      "project_category": "Commercial - New",
                                      "ai_rationale": "x"} for r in run1}
    run2 = [rec(str(i), f"Office build {i}") for i in range(5)]
    ing.process_and_classify_permits(run2, existing_lookup=lambda recs: stored)
    assert fake.models.calls == 1  # unchanged


def test_dedup_by_description_sends_each_once_and_maps_back(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    recs = [rec("1", "New  Retail-Store"), rec("2", "new retail store", 50000),
            rec("3", "new retail store"), rec("4", "Warehouse shell")]
    out = ing.process_and_classify_permits(recs, existing_lookup=no_existing)
    assert fake.models.calls == 1
    assert fake.models.prompts[0].count("Input ID") == 2  # 2 unique descriptions, not 4
    assert all(r.complexity_tier == ComplexityTier.COMMERCIAL for r in out)


def test_blank_descriptions_are_not_merged(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    ing.process_and_classify_permits([rec("1", ""), rec("2", ""), rec("3", "No Description")],
                                     existing_lookup=no_existing)
    assert fake.models.prompts[0].count("Input ID") == 3


def test_persist_called_per_batch_and_survives_fatal_abort(monkeypatch):
    saved = []

    def handler(n, prompt):
        if n == 2:
            raise SimpleNamespace_error(402)
        return echo_commercial(n, prompt)

    monkeypatch.setattr(ing, "ai_client", FakeAI(handler))
    recs = [rec(str(i), f"unique office {i}") for i in range(ing.AI_CHUNK_SIZE + 5)]
    with pytest.raises(ing.AIClassificationError):
        ing.process_and_classify_permits(recs, persist=lambda rs: saved.extend(rs),
                                         existing_lookup=no_existing)
    assert len(saved) == ing.AI_CHUNK_SIZE  # first batch (paid for) was kept; 2nd was fatal
    assert all(r.complexity_tier == ComplexityTier.COMMERCIAL for r in saved)


def SimpleNamespace_error(code):
    e = RuntimeError(f"{code} billing")
    e.code = code
    return e


def test_preflight_cost_guard_aborts_before_any_call(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 0.001)
    with pytest.raises(gc.CostCapExceeded):
        ing.process_and_classify_permits([rec("1", "Office A"), rec("2", "Office B")],
                                         existing_lookup=no_existing)
    assert fake.models.calls == 0


def test_preflight_override_allows_run(monkeypatch):
    fake = FakeAI(echo_commercial)
    monkeypatch.setattr(ing, "ai_client", fake)
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 0.001)
    monkeypatch.setenv("ALLOW_COST_OVERRIDE", "1")
    # measured cost per fake call (1000 in / 500 out) is ~$0.001 -> may trip the in-loop cap
    # on the NEXT batch, but a single batch completes.
    out = ing.process_and_classify_permits([rec("1", "Office A")], existing_lookup=no_existing)
    assert fake.models.calls == 1
    assert out[0].complexity_tier == ComplexityTier.COMMERCIAL


def test_inloop_measured_cap_aborts_and_keeps_paid_work(monkeypatch):
    saved = []
    big = lambda n, p: SimpleNamespace(
        text=echo_commercial(n, p).text, usage_metadata=usage_md(5_000_000, 5_000_000))
    monkeypatch.setattr(ing, "ai_client", FakeAI(big))
    monkeypatch.setenv("ALLOW_COST_OVERRIDE", "1")  # get past pre-flight; in-loop must still bite
    recs = [rec(str(i), f"unique office {i}") for i in range(ing.AI_CHUNK_SIZE * 3)]
    with pytest.raises(gc.CostCapExceeded):
        ing.process_and_classify_permits(recs, persist=lambda rs: saved.extend(rs),
                                         existing_lookup=no_existing)
    assert ing.AI_CHUNK_SIZE == len(saved)  # exactly one call made, its results persisted
    assert ing.usage.calls == 1


def test_fetch_existing_tiers_queries_by_city_and_ids():
    seen = []

    class Q:
        def __init__(self): self.f = {}
        def select(self, *_): return self
        def eq(self, k, v): self.f[k] = v; return self
        def in_(self, k, v): self.f[k] = v; seen.append(dict(self.f)); return self
        def execute(self):
            return SimpleNamespace(data=[{"city": self.f["city"], "permit_id": self.f["permit_id"][0],
                                          "complexity_tier": "Commercial"}])

    client = SimpleNamespace(table=lambda name: Q())
    recs = [rec(str(i), "x") for i in range(ing.LOOKUP_CHUNK + 1)]
    got = ing.fetch_existing_tiers(recs, client=client)
    assert len(seen) == 2 and seen[0]["city"] == "Austin"
    assert ("Austin", "0") in got


def test_progressive_writer_dedupes_and_retries_failures():
    calls = []

    def upsert(rows):
        calls.append([r["permit_id"] for r in rows])
        return (0, len(rows)) if len(calls) == 1 else (len(rows), 0)

    w = ing.ProgressiveWriter(upsert)
    a, b = rec("1", "x"), rec("2", "y")
    w([a, b])          # fails -> nothing marked written
    w([a, b])          # retried and succeeds
    w([a, b])          # already written -> no upsert
    assert calls == [["1", "2"], ["1", "2"]]


# ---------- classify_engine ----------

def _engine_with_rows(monkeypatch, n_rows, handler):
    import classify_engine as ce
    rows = [{"city": "Austin", "permit_id": str(i), "description": f"unique thing {i}",
             "valuation": 1000} for i in range(n_rows)]
    upserts = []

    class Q:
        def select(self, *_): return self
        def eq(self, *_): return self
        def limit(self, *_): return self
        def upsert(self, chunk, **_): upserts.extend(chunk); return self
        def execute(self): return SimpleNamespace(data=rows)

    monkeypatch.setattr(ce, "supabase", SimpleNamespace(table=lambda n: Q()))
    monkeypatch.setattr(ce, "ai_client", FakeAI(handler))
    monkeypatch.setattr(ce, "usage", gc.UsageTracker("engine-test"))
    monkeypatch.setattr(ce.time, "sleep", lambda *_: None)
    return ce, upserts


def _engine_answer(n, prompt):
    start = prompt.index("[{")
    end = prompt.index("}]", start) + 2
    items = json.loads(prompt[start:end])
    body = [{"description": it["description"], "tier": "Commodity"} for it in items]
    return SimpleNamespace(text=json.dumps(body), usage_metadata=usage_md(500, 100))


def test_engine_preflight_guard_blocks_before_any_call(monkeypatch):
    ce, _ = _engine_with_rows(monkeypatch, 30, _engine_answer)
    monkeypatch.setattr(gc, "MAX_RUN_COST_USD", 0.0000001)
    with pytest.raises(gc.CostCapExceeded):
        ce.run_ai_processor()
    assert ce.ai_client.models.calls == 0


def test_engine_inloop_cap_aborts(monkeypatch):
    big = lambda n, p: SimpleNamespace(text=_engine_answer(n, p).text,
                                       usage_metadata=usage_md(5_000_000, 5_000_000))
    ce, upserts = _engine_with_rows(monkeypatch, 60, big)
    monkeypatch.setenv("ALLOW_COST_OVERRIDE", "1")
    with pytest.raises(gc.CostCapExceeded):
        ce.run_ai_processor()
    assert ce.ai_client.models.calls == 1 and len(upserts) == ce.BATCH_SIZE


def test_engine_normal_run_records_usage(monkeypatch):
    ce, upserts = _engine_with_rows(monkeypatch, 30, _engine_answer)
    ce.run_ai_processor()
    assert ce.usage.calls == 2 and len(upserts) == 30
