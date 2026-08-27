import pytest

from live.instruments import normalize_instrument


def test_normalize_forms():
    for raw in ("600000", "sh600000", "SH600000", "600000.SH", "600000.SSE"):
        i = normalize_instrument(raw)
        assert (i.code, i.exchange, i.md_symbol, i.sid) == ("600000", "sh", "sh600000", 2_600_000)


def test_exchange_inference():
    assert normalize_instrument("000858").exchange == "sz"
    assert normalize_instrument("300750").exchange == "sz"
    assert normalize_instrument("688981").exchange == "sh"
    assert normalize_instrument("430047").exchange == "bj"


def test_bad_inputs():
    with pytest.raises(ValueError):
        normalize_instrument("60000")
    with pytest.raises(ValueError):
        normalize_instrument("abcdef")
    with pytest.raises(ValueError):
        normalize_instrument("600000.HK")
