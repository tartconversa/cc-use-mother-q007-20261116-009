import unittest
from datetime import datetime
from decimal import Decimal

from railwatch.domain import Capture, Claim, ProductError, SourcePolicy, amount, canonical, day, fingerprint, instant, text, token, url


class DomainTests(unittest.TestCase):
    def policy_data(self):
        return {"source_id": "news", "host": "example.org", "kinds": ["freight", "milestone"],
                "max_age_days": 30, "trust_rank": 10, "version": 1, "active": True}

    def capture_data(self):
        return {"source_id": "news", "external_id": "item-1", "url": "https://example.org/a#fragment",
                "fetched_at": "2026-11-16T11:00:00+08:00", "published_at": "2026-11-15T10:00:00+08:00",
                "title": "Railway update", "body": "Freight volume reached 100 tonnes.",
                "media_type": "text/plain", "metadata": {"edition": 1}}

    def claim_data(self):
        return {"project_key": "rail-a", "kind": "freight", "metric": "cargo",
                "period": "2026-11-15", "amount": "100.0", "unit": "tonne",
                "quote": "Freight volume reached 100 tonnes."}

    def assert_code(self, code, function, *args):
        with self.assertRaises(ProductError) as caught:
            function(*args)
        self.assertEqual(caught.exception.code, code)

    def test_token_accepts_identifiers(self):
        self.assertEqual(token("a.b-c_1:2", "id"), "a.b-c_1:2")

    def test_token_rejects_empty(self):
        self.assert_code("invalid_token", token, "", "id")

    def test_token_rejects_slash(self):
        self.assert_code("invalid_token", token, "a/b", "id")

    def test_token_rejects_length(self):
        self.assert_code("invalid_token", token, "a" * 121, "id")

    def test_text_strips(self):
        self.assertEqual(text("  abc  ", "body"), "abc")

    def test_text_rejects_nul(self):
        self.assert_code("invalid_text", text, "ab" + chr(0), "body")

    def test_text_rejects_blank(self):
        self.assert_code("invalid_text", text, "   ", "body")

    def test_url_drops_fragment(self):
        self.assertEqual(url("https://Example.org/a#b"), "https://example.org/a")

    def test_url_rejects_credentials(self):
        self.assert_code("invalid_url", url, "https://x:y@example.org/a")

    def test_url_rejects_scheme(self):
        self.assert_code("invalid_url", url, "file:///tmp/a")

    def test_url_rejects_bad_port(self):
        self.assert_code("invalid_url", url, "https://example.org:bad/a")

    def test_day_normalizes(self):
        self.assertEqual(day("2026-11-15"), "2026-11-15")

    def test_day_rejects_calendar_error(self):
        self.assert_code("invalid_date", day, "2026-02-30")

    def test_instant_to_utc(self):
        self.assertEqual(instant("2026-11-16T11:00:00+08:00"), "2026-11-16T03:00:00+00:00")

    def test_instant_rejects_naive(self):
        self.assert_code("invalid_time", instant, "2026-11-16T11:00:00")

    def test_amount_normalizes_decimal(self):
        self.assertEqual(amount("100.00"), "100")

    def test_amount_rejects_negative(self):
        self.assert_code("invalid_amount", amount, -1)

    def test_amount_rejects_infinity(self):
        self.assert_code("invalid_amount", amount, "Infinity")

    def test_amount_rejects_boolean(self):
        self.assert_code("invalid_amount", amount, True)

    def test_policy_roundtrip(self):
        policy = SourcePolicy.parse(self.policy_data())
        self.assertEqual(policy.host, "example.org")
        self.assertEqual(policy.kinds, ("freight", "milestone"))

    def test_policy_rejects_duplicate_kinds(self):
        data = self.policy_data()
        data["kinds"] = ["freight", "freight"]
        self.assert_code("invalid_kinds", SourcePolicy.parse, data)

    def test_policy_rejects_rank(self):
        data = self.policy_data()
        data["trust_rank"] = 101
        self.assert_code("invalid_policy", SourcePolicy.parse, data)

    def test_policy_rejects_host(self):
        data = self.policy_data()
        data["host"] = "bad/host"
        self.assert_code("invalid_host", SourcePolicy.parse, data)

    def test_capture_normalizes_fields(self):
        item = Capture.parse(self.capture_data())
        self.assertEqual(item.url, "https://example.org/a")
        self.assertEqual(item.published_at, "2026-11-15T02:00:00+00:00")

    def test_capture_identity_stable(self):
        item = Capture.parse(self.capture_data())
        self.assertEqual(item.identity(), "news:item-1")

    def test_capture_hash_changes_with_body(self):
        first = Capture.parse(self.capture_data())
        changed = self.capture_data()
        changed["body"] += " More"
        self.assertNotEqual(first.content_hash(), Capture.parse(changed).content_hash())

    def test_capture_rejects_media(self):
        data = self.capture_data()
        data["media_type"] = "application/pdf"
        self.assert_code("invalid_media", Capture.parse, data)

    def test_capture_rejects_future_publication(self):
        data = self.capture_data()
        data["published_at"] = "2026-11-17T00:00:00Z"
        self.assert_code("future_publication", Capture.parse(data).check, SourcePolicy.parse(self.policy_data()))

    def test_capture_rejects_wrong_host(self):
        data = self.capture_data()
        data["url"] = "https://elsewhere.org/a"
        self.assert_code("source_mismatch", Capture.parse(data).check, SourcePolicy.parse(self.policy_data()))

    def test_capture_rejects_stale(self):
        data = self.capture_data()
        data["published_at"] = "2020-01-01T00:00:00Z"
        self.assert_code("stale_capture", Capture.parse(data).check, SourcePolicy.parse(self.policy_data()))

    def test_capture_rejects_disabled_source(self):
        data = self.policy_data()
        data["active"] = False
        self.assert_code("source_disabled", Capture.parse(self.capture_data()).check, SourcePolicy.parse(data))

    def test_claim_normalizes_quantity(self):
        claim = Claim.parse(self.claim_data())
        self.assertEqual(claim.amount, "100")
        self.assertEqual(claim.key(), ("rail-a", "freight", "cargo", "2026-11-15"))

    def test_claim_hash_is_stable(self):
        claim = Claim.parse(self.claim_data())
        self.assertEqual(claim.hash(), Claim.parse(self.claim_data()).hash())

    def test_claim_rejects_unsupported_unit(self):
        data = self.claim_data()
        data["unit"] = "miles"
        self.assert_code("invalid_claim", Claim.parse, data)

    def test_claim_rejects_future_day(self):
        data = self.claim_data()
        data["period"] = "not-a-day"
        self.assert_code("invalid_date", Claim.parse, data)

    def test_milestone_parses(self):
        data = self.claim_data()
        data.pop("amount")
        data.pop("unit")
        data["kind"] = "milestone"
        data["milestone"] = "testing"
        self.assertEqual(Claim.parse(data).value(), "testing")

    def test_milestone_rejects_amount(self):
        data = self.claim_data()
        data["kind"] = "milestone"
        data["milestone"] = "testing"
        self.assert_code("invalid_claim", Claim.parse, data)

    def test_fingerprint_is_key_order_independent(self):
        self.assertEqual(fingerprint({"a": 1, "b": 2}), fingerprint({"b": 2, "a": 1}))


if __name__ == "__main__":
    unittest.main()
