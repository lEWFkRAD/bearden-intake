#!/usr/bin/env python3
"""
Test suite for PIITokenizer.
Run: python3 test_pii_tokenizer.py
"""

import sys
import os

# Minimal imports to test the tokenizer class in isolation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# We need to import PIITokenizer from extract.py
# But extract.py has imports that may not be available in test env
# So we'll test by extracting the class logic directly via exec

import re
import json

# ──────────────────────────────────────────────────────────────
# Copy the regex patterns and test them directly first
# ──────────────────────────────────────────────────────────────

SSN_PATTERN = re.compile(
    r'\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b'
    r'|'
    r'(?<!\w)([Xx*]{3}[-\s]?[Xx*]{2}[-\s]?\d{4})(?!\w)'
)

def test_ssn_regex():
    """Test that the SSN regex matches all expected patterns."""
    print("── Testing SSN regex patterns ──")

    should_match = [
        ("123-45-6789", "standard SSN with dashes"),
        ("123 45 6789", "SSN with spaces"),
        ("123456789", "SSN no separators"),
        ("XXX-XX-1234", "masked SSN uppercase"),
        ("xxx-xx-1234", "masked SSN lowercase"),
        ("***-**-1234", "masked SSN asterisks"),
        ("XXX-XX-2224", "masked like Stacy's W-2"),
    ]

    should_not_match = [
        ("12-3456789", "EIN format (should NOT match)"),
        ("58-2234927", "another EIN"),
        ("03-0501938", "K-1 EIN"),
        ("1234", "too short"),
        ("12345", "five digits"),
        ("phone 706-217-3732", "phone number"),
    ]

    passed = 0
    failed = 0

    for text, desc in should_match:
        m = SSN_PATTERN.search(text)
        if m:
            print(f"  ✓ MATCH: '{text}' — {desc}")
            passed += 1
        else:
            print(f"  ✗ MISS:  '{text}' — {desc}")
            failed += 1

    for text, desc in should_not_match:
        m = SSN_PATTERN.search(text)
        if m:
            print(f"  ✗ FALSE POSITIVE: '{text}' matched '{m.group()}' — {desc}")
            failed += 1
        else:
            print(f"  ✓ SKIP:  '{text}' — {desc}")
            passed += 1

    print(f"  Regex: {passed} passed, {failed} failed\n")
    return failed == 0


def test_tokenize_text():
    """Test text tokenization and detokenization with realistic OCR output."""
    print("── Testing text tokenization ──")

    # Standalone PIITokenizer for testing (mirrors the class in extract.py)
    class PIITokenizer:
        SSN_PATTERN = re.compile(
            r'\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b'
            r'|'
            r'(?<!\w)([Xx*]{3}[-\s]?[Xx*]{2}[-\s]?\d{4})(?!\w)'
        )
        def __init__(self):
            self.ssn_map = {}
            self.ssn_reverse = {}
            self._ssn_counter = 0
        def _next_ssn_token(self):
            self._ssn_counter += 1
            return f"[SSN_{self._ssn_counter}]"
        def _register_ssn(self, raw_ssn):
            normalized = raw_ssn.strip()
            if normalized in self.ssn_reverse:
                return self.ssn_reverse[normalized]
            token = self._next_ssn_token()
            self.ssn_map[token] = normalized
            self.ssn_reverse[normalized] = token
            return token
        def tokenize_text(self, text):
            if not text: return text, 0
            found = 0
            def replace_ssn(match):
                nonlocal found
                found += 1
                return self._register_ssn(match.group(0))
            result = self.SSN_PATTERN.sub(replace_ssn, text)
            return result, found
        def detokenize_text(self, text):
            if not text: return text
            for token, real in self.ssn_map.items():
                text = text.replace(token, real)
            return text
        def detokenize_json(self, obj):
            if obj is None: return None
            if isinstance(obj, str): return self.detokenize_text(obj)
            if isinstance(obj, dict): return {k: self.detokenize_json(v) for k, v in obj.items()}
            if isinstance(obj, list): return [self.detokenize_json(i) for i in obj]
            return obj
        def get_stats(self):
            return {"ssns_tokenized": len(self.ssn_map), "tokens": list(self.ssn_map.keys())}

    tok = PIITokenizer()
    failed = 0

    # Test 1: W-2 OCR text with SSN
    ocr_w2 = """
    a Employee's SSN
    259-75-4021
    OMB No. 1545-0008
    1 Wages, tips, other compensation  2 Federal income tax withheld
    49605.00                            2107.35
    Employer: BEARDEN ACCOUNTING FIRM (2024)
    b Employer identification number
    58-1826789
    e Employee name: Jeffrey A Watts
    """

    tokenized, count = tok.tokenize_text(ocr_w2)
    print(f"  Test 1 (W-2): found {count} SSN(s)")

    if count != 1:
        print(f"    ✗ Expected 1 SSN, got {count}")
        failed += 1
    else:
        print(f"    ✓ Found 1 SSN")

    if "259-75-4021" in tokenized:
        print(f"    ✗ SSN still present in tokenized text!")
        failed += 1
    else:
        print(f"    ✓ SSN removed from tokenized text")

    if "[SSN_1]" in tokenized:
        print(f"    ✓ Token [SSN_1] present in tokenized text")
    else:
        print(f"    ✗ Token not found in tokenized text")
        failed += 1

    # EIN should NOT be tokenized
    if "58-1826789" in tokenized:
        print(f"    ✓ EIN preserved (not tokenized)")
    else:
        print(f"    ✗ EIN was incorrectly tokenized!")
        failed += 1

    # Test 2: Detokenize
    detokenized = tok.detokenize_text(tokenized)
    if "259-75-4021" in detokenized:
        print(f"    ✓ SSN restored after detokenization")
    else:
        print(f"    ✗ SSN not restored!")
        failed += 1

    # Test 3: Masked SSN (like on W-2 copies)
    tok2 = PIITokenizer()
    masked_text = "Employee's SSN: XXX-XX-2224\nWages: 76540.06"
    tokenized2, count2 = tok2.tokenize_text(masked_text)
    print(f"\n  Test 3 (masked SSN): found {count2}")
    if count2 == 1:
        print(f"    ✓ Masked SSN detected and tokenized")
    else:
        print(f"    ✗ Expected 1 masked SSN, got {count2}")
        failed += 1

    # Test 4: Multiple SSNs in one document
    tok3 = PIITokenizer()
    multi_text = """
    Taxpayer SSN: 259-75-4021
    Spouse SSN: 413-71-2224
    Dependent SSN: 555-12-3456
    Employer EIN: 58-1826789
    """
    tokenized3, count3 = tok3.tokenize_text(multi_text)
    print(f"\n  Test 4 (multiple SSNs): found {count3}")
    if count3 == 3:
        print(f"    ✓ All 3 SSNs tokenized")
    else:
        print(f"    ✗ Expected 3, got {count3}")
        failed += 1

    if "58-1826789" in tokenized3:
        print(f"    ✓ EIN preserved")
    else:
        print(f"    ✗ EIN was incorrectly tokenized!")
        failed += 1

    # Test 5: Same SSN appears twice — should reuse token
    tok4 = PIITokenizer()
    dup_text = "SSN: 259-75-4021\nConfirm SSN: 259-75-4021"
    tokenized4, count4 = tok4.tokenize_text(dup_text)
    print(f"\n  Test 5 (duplicate SSN): found {count4}")
    if count4 == 2:
        print(f"    ✓ Both occurrences tokenized")
    else:
        print(f"    ✗ Expected 2 matches, got {count4}")
        failed += 1
    # Should only have 1 token in the map
    if len(tok4.ssn_map) == 1:
        print(f"    ✓ Reused same token for duplicate SSN")
    else:
        print(f"    ✗ Created {len(tok4.ssn_map)} tokens instead of 1")
        failed += 1

    # Test 6: JSON detokenization
    tok5 = PIITokenizer()
    tok5.tokenize_text("SSN: 259-75-4021")  # Register the token
    test_json = {
        "document_type": "W-2",
        "recipient_ssn_last4": "4021",
        "fields": {
            "wages": {"value": 49605.00},
            "employee_ssn": {"value": "[SSN_1]"}
        }
    }
    detok_json = tok5.detokenize_json(test_json)
    if detok_json["fields"]["employee_ssn"]["value"] == "259-75-4021":
        print(f"\n  Test 6 (JSON detokenize): ✓ SSN restored in nested JSON")
    else:
        print(f"\n  Test 6 (JSON detokenize): ✗ SSN not restored")
        failed += 1

    # Test 7: No false positives on dollar amounts, dates, phone numbers
    tok6 = PIITokenizer()
    safe_text = """
    Wages: $49,605.00
    Federal WH: $2,107.35
    Date: 01/31/2024
    Phone: 706-217-3732
    ZIP: 30722
    Account: WAB-161792
    OMB No. 1545-0008
    """
    tokenized6, count6 = tok6.tokenize_text(safe_text)
    print(f"\n  Test 7 (no false positives): found {count6}")
    if count6 == 0:
        print(f"    ✓ No false positives on dollar amounts, dates, phones, ZIPs")
    else:
        print(f"    ✗ {count6} false positive(s)!")
        # Show what was matched
        for token, val in tok6.ssn_map.items():
            print(f"      False positive: '{val}' → {token}")
        failed += 1

    print(f"\n  Text tokenization: {'ALL PASSED' if failed == 0 else f'{failed} FAILED'}\n")
    return failed == 0


def test_stats():
    """Test the stats reporting."""
    print("── Testing stats ──")

    # Use same standalone class
    class PIITokenizer:
        SSN_PATTERN = re.compile(
            r'\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b'
            r'|'
            r'(?<!\w)([Xx*]{3}[-\s]?[Xx*]{2}[-\s]?\d{4})(?!\w)'
        )
        def __init__(self):
            self.ssn_map = {}
            self.ssn_reverse = {}
            self._ssn_counter = 0
        def _next_ssn_token(self):
            self._ssn_counter += 1
            return f"[SSN_{self._ssn_counter}]"
        def _register_ssn(self, raw_ssn):
            normalized = raw_ssn.strip()
            if normalized in self.ssn_reverse:
                return self.ssn_reverse[normalized]
            token = self._next_ssn_token()
            self.ssn_map[token] = normalized
            self.ssn_reverse[normalized] = token
            return token
        def tokenize_text(self, text):
            if not text: return text, 0
            found = 0
            def replace_ssn(match):
                nonlocal found
                found += 1
                return self._register_ssn(match.group(0))
            result = self.SSN_PATTERN.sub(replace_ssn, text)
            return result, found
        def get_stats(self):
            return {"ssns_tokenized": len(self.ssn_map), "tokens": list(self.ssn_map.keys())}

    tok = PIITokenizer()
    tok.tokenize_text("SSN: 259-75-4021 and 413-71-2224")
    stats = tok.get_stats()
    if stats["ssns_tokenized"] == 2:
        print(f"  ✓ Stats report correct count: {stats['ssns_tokenized']}")
    else:
        print(f"  ✗ Stats report wrong count: {stats['ssns_tokenized']}")
        return False
    print(f"  Tokens: {stats['tokens']}\n")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("  PII Tokenizer Test Suite")
    print("=" * 60)
    print()

    results = []
    results.append(("SSN regex", test_ssn_regex()))
    results.append(("Text tokenization", test_tokenize_text()))
    results.append(("Stats", test_stats()))

    print("=" * 60)
    print("  RESULTS")
    print("=" * 60)
    all_pass = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_pass = False

    print()
    if all_pass:
        print("  ALL TESTS PASSED")
    else:
        print("  SOME TESTS FAILED")
    print("=" * 60)
    sys.exit(0 if all_pass else 1)
