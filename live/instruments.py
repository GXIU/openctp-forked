"""Instrument normalization and sid encoding.

Accepts 600000 / sh600000 / SH600000 / 600000.SH / 600000.SSE forms and
normalizes to a 6-digit code + exchange prefix. SID encoding matches
basicd/obgx: region_id(CN=2) * 1_000_000 + code.
"""

from dataclasses import dataclass


def _exchange_for(code6: str) -> str:
    if code6[0] in "569":  # 600/601/603/605, 688, 900...
        return "sh"
    if code6[0] in "032":  # 000/001/002/003, 300, 200...
        return "sz"
    if code6[0] in "84":
        return "bj"
    raise ValueError(f"cannot infer exchange for code {code6!r}")


@dataclass(frozen=True)
class Instrument:
    code: str  # 6-digit numeric code
    exchange: str  # "sh" | "sz" | "bj"
    md_symbol: str  # "sh600000" — what SubscribeMarketData receives
    sid: int  # 2_000_000 + code

    def __repr__(self) -> str:  # compact for logs
        return f"Instrument({self.md_symbol})"


_EXCHANGE_ALIASES = {
    "sh": "sh",
    "ss": "sh",
    "sse": "sh",
    "sz": "sz",
    "szse": "sz",
    "bj": "bj",
    "bse": "bj",
}


def normalize_instrument(raw: str) -> Instrument:
    s = raw.strip().lower()
    has_suffix = False
    if "." in s:
        code, _, suffix = s.partition(".")
        has_suffix = True
        xch = _EXCHANGE_ALIASES.get(suffix, "")
    elif s[:2] in ("sh", "sz", "bj"):
        xch, code = s[:2], s[2:]
    else:
        code, xch = s, ""
    if not (code.isdigit() and len(code) == 6):
        raise ValueError(f"bad instrument {raw!r}: expected 6-digit code")
    exchange = xch or (_exchange_for(code) if not has_suffix else "")
    if exchange not in ("sh", "sz", "bj"):
        raise ValueError(f"bad exchange {xch!r} for {raw!r}")
    return Instrument(
        code=code,
        exchange=exchange,
        md_symbol=f"{exchange}{code}",
        sid=2_000_000 + int(code),
    )
