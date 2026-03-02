#!/usr/bin/env python3
"""
pii_guard.py — PII tokenization and image redaction for LLM API calls.

Canonical location: bearden-intake/pii_guard.py
Replaces SSNs, EINs, phone numbers, emails, account/routing numbers, and DOBs
with tokens before sending to external LLM APIs. Reverses tokens in responses.

EINs (Employer Identification Numbers) are handled separately from SSNs because
some workflows (classification, grouping) need them. The `protect_eins` flag
controls whether EINs are tokenized (default: False for backward compat).

Usage:
    from pii_guard import PIITokenizer

    tok = PIITokenizer()
    safe_text, count = tok.tokenize_text(raw_ocr)
    # ... send safe_text to LLM ...
    result = tok.detokenize_json(parsed_response)

Environment:
    LLM_ALLOW_RAW_PII=1   Bypass guard entirely (dev/debug only)
"""

import os
import re
import sys
import base64
import json
from io import BytesIO
from datetime import datetime

# Optional deps — graceful fallback
try:
    import pytesseract
    from PIL import Image, ImageDraw
    HAS_TESSERACT = True
except ImportError:
    HAS_TESSERACT = False


def raw_pii_allowed():
    """Check if LLM_ALLOW_RAW_PII env var bypasses the guard."""
    return os.environ.get("LLM_ALLOW_RAW_PII", "0") == "1"


def pii_guard_log(event, job_id=None, **kwargs):
    """Audit log to stderr. Never logs sensitive content — only counts and metadata."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = [f"[PII_GUARD {ts}] {event}"]
    if job_id:
        parts.append(f"job={job_id}")
    for k, v in kwargs.items():
        parts.append(f"{k}={v}")
    print(" | ".join(parts), file=sys.stderr)


class PIITokenizer:
    """
    Replaces PII with tokens before API calls, reverses them after.

    Supported patterns:
      - SSN:     123-45-6789, 123 45 6789, 123456789, XXX-XX-1234
      - EIN:     12-3456789 (optional, off by default)
      - Phone:   706-217-3732, 706.217.3732, 7062173732
      - Email:   user@example.com
      - Routing: 9-digit near "routing" context
      - Account: 10-17 digit near "account" context
      - DOB:     MM/DD/YYYY near "birth"/"DOB" context

    Token map lives in memory only — never written to disk.
    """

    # ── SSN patterns ──
    # Full SSNs: 123-45-6789, 123 45 6789, 123456789
    # Masked SSNs: XXX-XX-1234, ***-**-1234, xxx-xx-1234
    SSN_PATTERN = re.compile(
        r'\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b'
        r'|'
        r'(?<!\w)([Xx*]{3}[-\s]?[Xx*]{2}[-\s]?\d{4})(?!\w)'
    )

    # Last-4 SSN: "SSN: last 4: 1234"
    SSN_LAST4_PATTERN = re.compile(
        r'(?:SSN|social\s*security)[\s:]*(?:last\s*4[\s:]*)(\d{4})',
        re.IGNORECASE
    )

    # ── EIN pattern ──
    # XX-XXXXXXX (2 digits, dash, 7 digits)
    EIN_PATTERN = re.compile(r'\b(\d{2}-\d{7})\b')

    # ── Phone pattern ──
    # 706-217-3732, 706.217.3732, (706) 217-3732, 7062173732
    PHONE_PATTERN = re.compile(
        r'(?<!\d)'
        r'(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})'
        r'(?!\d)'
    )

    # ── Email pattern ──
    EMAIL_PATTERN = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    )

    # ── Routing number (9 digits near "routing" context) ──
    ROUTING_PATTERN = re.compile(
        r'(?:routing|ABA|transit)[\s#:]*(\d{9})\b',
        re.IGNORECASE
    )

    # ── Account number (10-17 digits near "account" context) ──
    ACCOUNT_PATTERN = re.compile(
        r'(?:account|acct)[\s#:]*(\d{10,17})\b',
        re.IGNORECASE
    )

    # ── DOB (MM/DD/YYYY near "birth"/"DOB" context) ──
    DOB_PATTERN = re.compile(
        r'(?:birth|DOB|date\s+of\s+birth)[\s:]*(\d{1,2}/\d{1,2}/\d{2,4})',
        re.IGNORECASE
    )

    def __init__(self, protect_eins=False):
        """
        Args:
            protect_eins: If True, also tokenize EINs. Default False because
                          many workflows need EINs for classification/grouping.
        """
        self.protect_eins = protect_eins

        # Per-category token maps: token → real value
        self._maps = {
            "SSN": {},
            "EIN": {},
            "PHONE": {},
            "EMAIL": {},
            "ROUTING": {},
            "ACCOUNT": {},
            "DOB": {},
        }
        # Reverse maps: real value → token
        self._reverse = {k: {} for k in self._maps}
        # Counters
        self._counters = {k: 0 for k in self._maps}

    # ── Legacy accessors (backward compat with extract.py) ──
    @property
    def ssn_map(self):
        return self._maps["SSN"]

    @property
    def ssn_reverse(self):
        return self._reverse["SSN"]

    def _next_token(self, category):
        self._counters[category] += 1
        return f"[{category}_{self._counters[category]}]"

    def _register(self, raw, category):
        normalized = raw.strip()
        if normalized in self._reverse[category]:
            return self._reverse[category][normalized]
        token = self._next_token(category)
        self._maps[category][token] = normalized
        self._reverse[category][normalized] = token
        return token

    # Legacy alias
    def _register_ssn(self, raw_ssn):
        return self._register(raw_ssn, "SSN")

    def _next_ssn_token(self):
        return self._next_token("SSN")

    def tokenize_text(self, text):
        """Replace PII in text with tokens. Returns (tokenized_text, total_found_count)."""
        if not text:
            return text, 0

        found = 0

        # SSNs first (highest priority — must not collide with phone/EIN patterns)
        def replace_ssn(match):
            nonlocal found
            found += 1
            return self._register(match.group(0), "SSN")
        text = self.SSN_PATTERN.sub(replace_ssn, text)

        # EINs (only if protect_eins enabled)
        if self.protect_eins:
            def replace_ein(match):
                nonlocal found
                found += 1
                return self._register(match.group(1), "EIN")
            text = self.EIN_PATTERN.sub(replace_ein, text)

        # Context-dependent patterns (routing, account, DOB)
        def replace_routing(match):
            nonlocal found
            found += 1
            full = match.group(0)
            num = match.group(1)
            return full.replace(num, self._register(num, "ROUTING"))
        text = self.ROUTING_PATTERN.sub(replace_routing, text)

        def replace_account(match):
            nonlocal found
            found += 1
            full = match.group(0)
            num = match.group(1)
            return full.replace(num, self._register(num, "ACCOUNT"))
        text = self.ACCOUNT_PATTERN.sub(replace_account, text)

        def replace_dob(match):
            nonlocal found
            full = match.group(0)
            date = match.group(1)
            found += 1
            return full.replace(date, self._register(date, "DOB"))
        text = self.DOB_PATTERN.sub(replace_dob, text)

        # Phone numbers
        def replace_phone(match):
            nonlocal found
            found += 1
            return self._register(match.group(0), "PHONE")
        text = self.PHONE_PATTERN.sub(replace_phone, text)

        # Emails
        def replace_email(match):
            nonlocal found
            found += 1
            return self._register(match.group(0), "EMAIL")
        text = self.EMAIL_PATTERN.sub(replace_email, text)

        return text, found

    def detokenize_text(self, text):
        """Reverse all tokens back to real values in a string."""
        if not text:
            return text
        for category_map in self._maps.values():
            for token, real in category_map.items():
                text = text.replace(token, real)
        return text

    def detokenize_json(self, obj):
        """Recursively reverse tokens in a parsed JSON object."""
        if obj is None:
            return None
        if isinstance(obj, str):
            return self.detokenize_text(obj)
        if isinstance(obj, dict):
            return {k: self.detokenize_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.detokenize_json(item) for item in obj]
        return obj

    def redact_image(self, pil_image):
        """
        Find SSN-pattern text regions in the image and black them out.
        Returns a new PIL Image with SSN regions redacted.

        Uses pytesseract word-level bounding boxes to locate digits,
        then blacks out any sequence matching SSN patterns.
        """
        if not HAS_TESSERACT:
            return pil_image

        img = pil_image.copy()
        draw = ImageDraw.Draw(img)

        try:
            data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        except Exception:
            return img

        n = len(data['text'])
        words = []
        for i in range(n):
            txt = data['text'][i].strip()
            if txt:
                words.append({
                    'text': txt,
                    'left': data['left'][i],
                    'top': data['top'][i],
                    'width': data['width'][i],
                    'height': data['height'][i],
                    'index': i
                })

        full_text_positions = []
        running_text = ""
        for wi, w in enumerate(words):
            start = len(running_text)
            running_text += w['text'] + " "
            full_text_positions.append((start, len(running_text) - 1, wi))

        for match in self.SSN_PATTERN.finditer(running_text):
            mstart, mend = match.start(), match.end()
            indices_to_redact = []
            for start, end, wi in full_text_positions:
                if start < mend and end > mstart:
                    indices_to_redact.append(wi)

            for wi in indices_to_redact:
                w = words[wi]
                pad = 4
                draw.rectangle(
                    [w['left'] - pad, w['top'] - pad,
                     w['left'] + w['width'] + pad, w['top'] + w['height'] + pad],
                    fill='black'
                )

            self._register(match.group(0), "SSN")

        return img

    def redacted_image_to_b64(self, pil_image):
        """Redact SSNs from image and return base64 JPEG string."""
        redacted = self.redact_image(pil_image)
        buf = BytesIO()
        redacted.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    def get_stats(self):
        """Return summary of tokenization for logging."""
        stats = {}
        total = 0
        for category, m in self._maps.items():
            count = len(m)
            if count > 0:
                stats[f"{category.lower()}_tokenized"] = count
                total += count
        stats["total_tokenized"] = total
        stats["tokens"] = {
            cat: list(m.keys()) for cat, m in self._maps.items() if m
        }
        # Legacy compat
        stats["ssns_tokenized"] = len(self._maps["SSN"])
        return stats


def guard_text(text, job_id=None):
    """Convenience: tokenize text with default guard. Returns (tokenized, tokenizer) or (original, None) if bypassed."""
    if raw_pii_allowed():
        pii_guard_log("BYPASSED", job_id=job_id, reason="LLM_ALLOW_RAW_PII=1")
        return text, None
    tok = PIITokenizer()
    safe, count = tok.tokenize_text(text)
    if count > 0:
        pii_guard_log("TOKENIZED", job_id=job_id, count=count, categories=_summarize(tok))
    return safe, tok


def guard_messages(messages, job_id=None):
    """
    Tokenize PII in a messages list (Anthropic API format).
    Returns (safe_messages, tokenizer) — use tokenizer.detokenize_json() on response.
    """
    if raw_pii_allowed():
        pii_guard_log("BYPASSED", job_id=job_id, reason="LLM_ALLOW_RAW_PII=1")
        return messages, None

    tok = PIITokenizer()
    safe_messages = []
    total_count = 0

    for msg in messages:
        safe_msg = {"role": msg["role"]}
        content = msg.get("content", "")
        if isinstance(content, str):
            safe_text, count = tok.tokenize_text(content)
            total_count += count
            safe_msg["content"] = safe_text
        elif isinstance(content, list):
            safe_parts = []
            for part in content:
                if part.get("type") == "text":
                    safe_text, count = tok.tokenize_text(part["text"])
                    total_count += count
                    safe_parts.append({"type": "text", "text": safe_text})
                elif part.get("type") == "image":
                    # Redact SSNs from image if tesseract available
                    source = part.get("source", {})
                    if source.get("type") == "base64" and HAS_TESSERACT:
                        try:
                            img_bytes = base64.b64decode(source["data"])
                            pil_img = Image.open(BytesIO(img_bytes))
                            safe_b64 = tok.redacted_image_to_b64(pil_img)
                            safe_parts.append({
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": source.get("media_type", "image/jpeg"),
                                    "data": safe_b64
                                }
                            })
                            continue
                        except Exception:
                            pass
                    safe_parts.append(part)
                else:
                    safe_parts.append(part)
            safe_msg["content"] = safe_parts
        else:
            safe_msg["content"] = content
        safe_messages.append(safe_msg)

    if total_count > 0:
        pii_guard_log("TOKENIZED", job_id=job_id, count=total_count, categories=_summarize(tok))

    return safe_messages, tok


def _summarize(tok):
    """Compact category summary for logging."""
    parts = []
    for cat, m in tok._maps.items():
        if m:
            parts.append(f"{cat}={len(m)}")
    return ",".join(parts) if parts else "none"
