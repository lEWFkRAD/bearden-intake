"""Tests for Lite Doctrine — governance fingerprinting and drift detection.

Required tests from tickets:
  Ticket 1:
    - test_doctrine_manifest_round_trip
    - test_doctrine_hash_deterministic
    - test_doctrine_hash_changes_on_rule_map_change
    - test_engine_sets_doctrine_fields_when_enabled

  Ticket 2:
    - test_drift_guard_legacy_ok
    - test_drift_guard_major_warns
    - test_drift_guard_hash_warns
    - test_drift_guard_ok
"""

import json
import pytest


# ────────────────────────────────────────────────────────────────────
# Ticket 1: Doctrine Core
# ────────────────────────────────────────────────────────────────────

class TestFingerprint:
    """Tests for lite.doctrine.fingerprint primitives."""

    def test_canonical_json_sorted_keys(self):
        from lite.doctrine.fingerprint import canonical_json
        obj = {"z": 1, "a": 2, "m": {"b": 3, "a": 4}}
        result = canonical_json(obj)
        parsed = json.loads(result)
        assert list(parsed.keys()) == ["a", "m", "z"]
        assert list(parsed["m"].keys()) == ["a", "b"]

    def test_canonical_json_stable_separators(self):
        from lite.doctrine.fingerprint import canonical_json
        result = canonical_json({"key": "value"})
        assert result == '{"key":"value"}'
        assert " " not in result  # No whitespace

    def test_sha256_text_deterministic(self):
        from lite.doctrine.fingerprint import sha256_text
        h1 = sha256_text("hello")
        h2 = sha256_text("hello")
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex length

    def test_sha256_text_different_inputs(self):
        from lite.doctrine.fingerprint import sha256_text
        h1 = sha256_text("hello")
        h2 = sha256_text("world")
        assert h1 != h2

    def test_hash_dict_deterministic(self):
        from lite.doctrine.fingerprint import hash_dict
        d = {"rules": ["A", "B"], "version": "1.0"}
        h1 = hash_dict(d)
        h2 = hash_dict(d)
        assert h1 == h2

    def test_hash_dict_key_order_independent(self):
        from lite.doctrine.fingerprint import hash_dict
        d1 = {"z": 1, "a": 2}
        d2 = {"a": 2, "z": 1}
        assert hash_dict(d1) == hash_dict(d2)


class TestDoctrineManifest:
    """Tests for lite.doctrine.manifest.DoctrineManifest."""

    def test_doctrine_manifest_round_trip(self):
        """Manifest can be serialized to JSON and deserialized back identically."""
        from lite.doctrine.manifest import DoctrineManifest

        manifest = DoctrineManifest(
            doctrine_version="0.1.0",
            doctrine_hash="abc123" * 10 + "abcd",
            ardent_ruleset_version="0.1.0",
            ardent_ruleset_hash="def456" * 10 + "defg",
            contracts_schema_versions={"W-2": "1.0.0", "1099-INT": "1.0.0"},
            lens_version="0.1.0",
            rule_map_hash="ghi789" * 10 + "ghij",
            notes="Test manifest",
            created_at="2026-02-26T12:00:00+00:00",
        )

        # Round-trip through JSON
        json_str = manifest.model_dump_json()
        restored = DoctrineManifest.model_validate_json(json_str)

        assert restored.doctrine_version == manifest.doctrine_version
        assert restored.doctrine_hash == manifest.doctrine_hash
        assert restored.ardent_ruleset_version == manifest.ardent_ruleset_version
        assert restored.contracts_schema_versions == manifest.contracts_schema_versions
        assert restored.lens_version == manifest.lens_version
        assert restored.rule_map_hash == manifest.rule_map_hash
        assert restored.notes == manifest.notes

    def test_doctrine_manifest_schema_version_default(self):
        from lite.doctrine.manifest import DoctrineManifest
        m = DoctrineManifest(
            doctrine_version="0.1.0",
            doctrine_hash="x" * 64,
            ardent_ruleset_version="0.1.0",
            ardent_ruleset_hash="y" * 64,
            rule_map_hash="z" * 64,
        )
        assert m.schema_version == "1.0.0"


class TestDoctrineRegistry:
    """Tests for lite.doctrine.registry."""

    def test_doctrine_hash_deterministic(self):
        """Same codebase produces same doctrine hash across runs."""
        from lite.doctrine.registry import get_current_manifest, invalidate_cache

        invalidate_cache()
        m1 = get_current_manifest()
        invalidate_cache()
        m2 = get_current_manifest()

        assert m1.doctrine_hash == m2.doctrine_hash
        assert m1.doctrine_version == m2.doctrine_version
        assert len(m1.doctrine_hash) == 64  # SHA-256

    def test_doctrine_hash_changes_on_rule_map_change(self):
        """Modifying the rule map produces a different doctrine hash."""
        from lite.doctrine import registry
        from lite.doctrine.registry import get_current_manifest, invalidate_cache

        invalidate_cache()
        m_before = get_current_manifest()

        # Save originals
        orig_rule_map = dict(registry.RULE_MAP)
        orig_ruleset = dict(registry.ARDENT_RULESET)

        try:
            # Mutate the rule map by adding a new rule
            registry.ARDENT_RULESET = dict(registry.ARDENT_RULESET)
            registry.ARDENT_RULESET["rules"] = dict(registry.ARDENT_RULESET["rules"])
            registry.ARDENT_RULESET["rules"]["TAX-999"] = {
                "name": "Test rule", "severity": "info", "doc_types": ["W-2"]
            }
            registry.RULE_MAP = {
                rid: rdef["doc_types"]
                for rid, rdef in registry.ARDENT_RULESET["rules"].items()
            }

            invalidate_cache()
            m_after = get_current_manifest()

            assert m_before.doctrine_hash != m_after.doctrine_hash
            assert m_before.doctrine_version == m_after.doctrine_version  # Version didn't change
        finally:
            # Restore originals
            registry.ARDENT_RULESET = orig_ruleset
            registry.RULE_MAP = orig_rule_map
            invalidate_cache()

    def test_current_doctrine_version_is_semver(self):
        from lite.doctrine.registry import CURRENT_DOCTRINE_VERSION
        parts = CURRENT_DOCTRINE_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_manifest_has_all_required_fields(self):
        from lite.doctrine.registry import get_current_manifest, invalidate_cache
        invalidate_cache()
        m = get_current_manifest()
        assert m.doctrine_version
        assert m.doctrine_hash
        assert m.ardent_ruleset_version
        assert m.ardent_ruleset_hash
        assert m.contracts_schema_versions
        assert m.rule_map_hash
        assert m.created_at


class TestEngineDoctrineWiring:
    """Tests for doctrine fields on ArdentResult."""

    def test_engine_sets_doctrine_fields_when_enabled(self):
        """Every ArdentResult from evaluate() includes doctrine_version and doctrine_hash."""
        from lite.ardent.engine import evaluate
        from lite.doctrine.registry import get_current_manifest, invalidate_cache

        invalidate_cache()
        manifest = get_current_manifest()

        result = evaluate(candidates=[], context=None)

        assert result.doctrine_version is not None
        assert result.doctrine_hash is not None
        assert result.doctrine_version == manifest.doctrine_version
        assert result.doctrine_hash == manifest.doctrine_hash

    def test_ardent_result_serializes_doctrine_fields(self):
        """ArdentResult.model_dump() includes doctrine fields."""
        from lite.ardent.engine import evaluate

        result = evaluate(candidates=[], context=None)
        dumped = result.model_dump(mode="json")

        assert "doctrine_version" in dumped
        assert "doctrine_hash" in dumped
        assert dumped["doctrine_version"] is not None

    def test_ardent_result_without_doctrine_is_compatible(self):
        """ArdentResult with None doctrine fields serializes cleanly."""
        from lite.ardent.engine import ArdentResult

        result = ArdentResult(
            ruleset_id="test-1.0",
            ruleset_hash="abc123",
        )
        dumped = result.model_dump(mode="json")
        assert dumped["doctrine_version"] is None
        assert dumped["doctrine_hash"] is None


# ────────────────────────────────────────────────────────────────────
# Ticket 2: Doctrine Drift Guard
# ────────────────────────────────────────────────────────────────────

class TestDriftGuard:
    """Tests for lite.doctrine.drift.doctrine_drift_status."""

    def test_drift_guard_legacy_ok(self):
        """Logs with no doctrine_version get status='legacy'."""
        from lite.doctrine.drift import doctrine_drift_status

        result = doctrine_drift_status(
            log_version=None,
            log_hash=None,
            current_version="0.1.0",
            current_hash="abc123def456" * 5 + "abcd",
        )

        assert result["status"] == "legacy"
        assert "predates" in result["message"].lower() or "no governance" in result["message"].lower()
        assert result["log_version"] is None

    def test_drift_guard_major_warns(self):
        """Major version change triggers warn_major."""
        from lite.doctrine.drift import doctrine_drift_status

        result = doctrine_drift_status(
            log_version="0.1.0",
            log_hash="abc" * 21 + "a",
            current_version="1.0.0",
            current_hash="def" * 21 + "d",
        )

        assert result["status"] == "warn_major"
        assert "major" in result["message"].lower()

    def test_drift_guard_hash_warns(self):
        """Same version but different hash triggers warn_hash."""
        from lite.doctrine.drift import doctrine_drift_status

        result = doctrine_drift_status(
            log_version="0.1.0",
            log_hash="aaaa" * 16,
            current_version="0.1.0",
            current_hash="bbbb" * 16,
        )

        assert result["status"] == "warn_hash"
        assert "hash" in result["message"].lower() or "mismatch" in result["message"].lower()

    def test_drift_guard_ok(self):
        """Matching version and hash returns status='ok'."""
        from lite.doctrine.drift import doctrine_drift_status

        same_hash = "abcdef12" * 8
        result = doctrine_drift_status(
            log_version="0.1.0",
            log_hash=same_hash,
            current_version="0.1.0",
            current_hash=same_hash,
        )

        assert result["status"] == "ok"
        assert "matches" in result["message"].lower()

    def test_drift_guard_returns_short_hashes(self):
        """Result includes first 8 chars of each hash."""
        from lite.doctrine.drift import doctrine_drift_status

        result = doctrine_drift_status(
            log_version="0.1.0",
            log_hash="abcdef1234567890" * 4,
            current_version="0.1.0",
            current_hash="abcdef1234567890" * 4,
        )

        assert result["log_hash_short"] == "abcdef12"
        assert result["current_hash_short"] == "abcdef12"

    def test_drift_guard_minor_version_ok(self):
        """Minor/patch version changes within same major are NOT flagged as major drift."""
        from lite.doctrine.drift import doctrine_drift_status

        result = doctrine_drift_status(
            log_version="0.1.0",
            log_hash="aaaa" * 16,
            current_version="0.2.0",
            current_hash="bbbb" * 16,
        )

        # Same major (0), so it should be warn_hash (not warn_major)
        assert result["status"] == "warn_hash"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
