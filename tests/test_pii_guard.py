#!/usr/bin/env python3
"""
Tests for pii_guard.py — canonical PII tokenization module.
Run: python -m pytest tests/test_pii_guard.py -v
"""

import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pii_guard import PIITokenizer, guard_text, guard_messages, raw_pii_allowed


# ── SSN Detection ──

class TestSSNPatterns:
    def test_standard_ssn(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: 259-75-4021")
        assert count == 1
        assert "259-75-4021" not in text
        assert "[SSN_1]" in text

    def test_ssn_with_spaces(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: 259 75 4021")
        assert count == 1
        assert "259 75 4021" not in text

    def test_ssn_no_separators(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: 259754021")
        assert count == 1
        assert "259754021" not in text

    def test_masked_ssn_uppercase(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: XXX-XX-2224")
        assert count == 1
        assert "XXX-XX-2224" not in text

    def test_masked_ssn_asterisks(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: ***-**-1234")
        assert count == 1

    def test_multiple_ssns(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text(
            "Taxpayer: 259-75-4021\nSpouse: 413-71-2224\nDependent: 555-12-3456"
        )
        assert count == 3
        assert "259-75-4021" not in text
        assert "413-71-2224" not in text
        assert "555-12-3456" not in text

    def test_duplicate_ssn_reuses_token(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("SSN: 259-75-4021\nConfirm: 259-75-4021")
        assert count == 2
        assert len(tok.ssn_map) == 1  # Only 1 unique token

    def test_ein_not_matched_as_ssn(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("EIN: 58-1826789")
        assert count == 0
        assert "58-1826789" in text

    def test_w2_realistic(self):
        tok = PIITokenizer()
        ocr = """
        a Employee's SSN
        259-75-4021
        b Employer identification number
        58-1826789
        e Employee name: Jeffrey A Watts
        1 Wages: 49605.00
        """
        text, count = tok.tokenize_text(ocr)
        assert count == 1
        assert "259-75-4021" not in text
        assert "58-1826789" in text  # EIN preserved


# ── EIN Detection (opt-in) ──

class TestEINPatterns:
    def test_ein_not_tokenized_by_default(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("EIN: 58-1826789")
        assert count == 0
        assert "58-1826789" in text

    def test_ein_tokenized_when_enabled(self):
        tok = PIITokenizer(protect_eins=True)
        text, count = tok.tokenize_text("EIN: 58-1826789")
        assert count == 1
        assert "58-1826789" not in text
        assert "[EIN_1]" in text


# ── Phone Detection ──

class TestPhonePatterns:
    def test_phone_dashes(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Call: 706-217-3732")
        assert count == 1
        assert "706-217-3732" not in text
        assert "[PHONE_1]" in text

    def test_phone_dots(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Phone: 706.217.3732")
        assert count == 1
        assert "706.217.3732" not in text

    def test_phone_parens(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Phone: (706) 217-3732")
        assert count == 1

    def test_phone_not_matched_as_ssn(self):
        """Phone numbers should be categorized as PHONE, not SSN."""
        tok = PIITokenizer()
        tok.tokenize_text("Call: 706-217-3732")
        assert len(tok._maps["PHONE"]) == 1
        assert len(tok._maps["SSN"]) == 0


# ── Email Detection ──

class TestEmailPatterns:
    def test_standard_email(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Contact: jeff@bearden.com")
        assert count == 1
        assert "jeff@bearden.com" not in text
        assert "[EMAIL_1]" in text

    def test_complex_email(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Send to: jeff.watts+tax@bearden-cpa.co.us")
        assert count == 1
        assert "jeff.watts+tax@bearden-cpa.co.us" not in text


# ── Routing Number Detection ──

class TestRoutingPatterns:
    def test_routing_number(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Routing number: 061000104")
        assert count == 1
        assert "061000104" not in text

    def test_aba_routing(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("ABA: 021000021")
        assert count == 1

    def test_nine_digits_without_context_not_matched(self):
        """Bare 9-digit numbers without routing context should NOT match."""
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Reference: 123456789")
        # This gets matched as SSN (9-digit pattern), not routing
        # The routing pattern requires context words
        routing_count = len(tok._maps["ROUTING"])
        assert routing_count == 0


# ── Account Number Detection ──

class TestAccountPatterns:
    def test_account_number(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Account number: 1234567890")
        assert count == 1
        assert "1234567890" not in text

    def test_acct_abbreviation(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Acct# 12345678901234")
        assert count == 1

    def test_long_account(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Account: 12345678901234567")
        assert count == 1


# ── DOB Detection ──

class TestDOBPatterns:
    def test_dob_standard(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Date of birth: 03/15/1985")
        assert count == 1
        assert "03/15/1985" not in text

    def test_dob_abbreviation(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("DOB: 3/15/85")
        assert count == 1

    def test_birth_keyword(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("birth: 12/25/1990")
        assert count == 1


# ── False Positive Resistance ──

class TestFalsePositives:
    def test_dollar_amounts_not_matched(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Wages: $49,605.00\nFederal WH: $2,107.35")
        assert count == 0

    def test_dates_not_matched(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Date: 01/31/2024\nFiled: 12/15/2023")
        # Without "birth"/"DOB" context, dates should not match DOB pattern
        dob_count = len(tok._maps["DOB"])
        assert dob_count == 0

    def test_zip_codes_not_matched(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("ZIP: 30722")
        assert count == 0

    def test_omb_number_not_matched(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("OMB No. 1545-0008")
        assert count == 0

    def test_account_code_not_matched(self):
        tok = PIITokenizer()
        text, count = tok.tokenize_text("Account: WAB-161792")
        # Has "Account" context but value isn't 10-17 digits
        acct_count = len(tok._maps["ACCOUNT"])
        assert acct_count == 0


# ── Detokenization ──

class TestDetokenization:
    def test_text_roundtrip(self):
        tok = PIITokenizer()
        original = "SSN: 259-75-4021, Phone: 706-217-3732"
        tokenized, _ = tok.tokenize_text(original)
        restored = tok.detokenize_text(tokenized)
        assert "259-75-4021" in restored
        assert "706-217-3732" in restored

    def test_json_roundtrip(self):
        tok = PIITokenizer()
        tok.tokenize_text("SSN: 259-75-4021")
        test_obj = {
            "document_type": "W-2",
            "fields": {
                "employee_ssn": {"value": "[SSN_1]"},
                "wages": {"value": 49605.00},
            }
        }
        restored = tok.detokenize_json(test_obj)
        assert restored["fields"]["employee_ssn"]["value"] == "259-75-4021"
        assert restored["fields"]["wages"]["value"] == 49605.00

    def test_json_none_handling(self):
        tok = PIITokenizer()
        assert tok.detokenize_json(None) is None

    def test_json_list_handling(self):
        tok = PIITokenizer()
        tok.tokenize_text("SSN: 259-75-4021")
        result = tok.detokenize_json(["[SSN_1]", "other"])
        assert result[0] == "259-75-4021"
        assert result[1] == "other"


# ── Stats ──

class TestStats:
    def test_stats_basic(self):
        tok = PIITokenizer()
        tok.tokenize_text("SSN: 259-75-4021 phone: 706-217-3732")
        stats = tok.get_stats()
        assert stats["ssns_tokenized"] == 1
        assert stats["total_tokenized"] == 2
        assert "SSN" in stats["tokens"]
        assert "PHONE" in stats["tokens"]

    def test_stats_empty(self):
        tok = PIITokenizer()
        tok.tokenize_text("No PII here")
        stats = tok.get_stats()
        assert stats["total_tokenized"] == 0
        assert stats["ssns_tokenized"] == 0


# ── Guard Functions ──

class TestGuardFunctions:
    def test_guard_text_tokenizes(self):
        os.environ.pop("LLM_ALLOW_RAW_PII", None)
        safe, tok = guard_text("SSN: 259-75-4021")
        assert "259-75-4021" not in safe
        assert tok is not None

    def test_guard_text_bypass(self):
        os.environ["LLM_ALLOW_RAW_PII"] = "1"
        try:
            safe, tok = guard_text("SSN: 259-75-4021")
            assert "259-75-4021" in safe
            assert tok is None
        finally:
            os.environ.pop("LLM_ALLOW_RAW_PII", None)

    def test_guard_messages_text_content(self):
        os.environ.pop("LLM_ALLOW_RAW_PII", None)
        messages = [{"role": "user", "content": "SSN: 259-75-4021"}]
        safe_msgs, tok = guard_messages(messages)
        assert "259-75-4021" not in safe_msgs[0]["content"]
        assert tok is not None

    def test_guard_messages_multipart_content(self):
        os.environ.pop("LLM_ALLOW_RAW_PII", None)
        messages = [{"role": "user", "content": [
            {"type": "text", "text": "SSN: 259-75-4021"},
        ]}]
        safe_msgs, tok = guard_messages(messages)
        text_part = safe_msgs[0]["content"][0]
        assert "259-75-4021" not in text_part["text"]
        assert tok is not None

    def test_guard_messages_bypass(self):
        os.environ["LLM_ALLOW_RAW_PII"] = "1"
        try:
            messages = [{"role": "user", "content": "SSN: 259-75-4021"}]
            safe_msgs, tok = guard_messages(messages)
            assert "259-75-4021" in safe_msgs[0]["content"]
            assert tok is None
        finally:
            os.environ.pop("LLM_ALLOW_RAW_PII", None)


# ── Backward Compatibility ──

class TestBackwardCompat:
    def test_ssn_map_property(self):
        """Ensure legacy ssn_map property works for extract.py compatibility."""
        tok = PIITokenizer()
        tok.tokenize_text("SSN: 259-75-4021")
        assert "[SSN_1]" in tok.ssn_map
        assert tok.ssn_map["[SSN_1]"] == "259-75-4021"

    def test_ssn_reverse_property(self):
        tok = PIITokenizer()
        tok.tokenize_text("SSN: 259-75-4021")
        assert "259-75-4021" in tok.ssn_reverse
