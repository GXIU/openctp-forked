"""M1 normalizer tests — synthetic raw capture lines against the header mappings."""


from alignment_ob.normalize import (
    Normalizer,
    decode_ts,
    md_symbol_of,
    normalize_capture,
)

TRADING_DAY = 20220104


def _md(**kw):
    line = {
        "type": "md",
        "SecurityID": "600000",
        "ExchangeID": "1",
        "DataTimeStamp": 93000000,
        "LastPrice": 8.30,
        "TotalVolumeTrade": 12345,
        "NumTrades": 500,
        "MDSecurityStat": "2",
    }
    for i in range(1, 11):
        line[f"BidPrice{i}"] = 8.29 - i * 0.01
        line[f"BidVolume{i}"] = 1000 + i
        line[f"AskPrice{i}"] = 8.30 + i * 0.01
        line[f"AskVolume{i}"] = 2000 + i
        line[f"Bid{i}NumOrders"] = 10 + i
        line[f"Ask{i}NumOrders"] = 20 + i
    line.update(kw)
    return line


def _tr(**kw):
    line = {
        "type": "tr",
        "SecurityID": "600000",
        "ExchangeID": "1",
        "TradeTime": 93001123,
        "TradePrice": 8.31,
        "TradeVolume": 100,
        "ExecType": "1",
        "MainSeq": 1,
        "SubSeq": 7,
        "BuyNo": 901,
        "SellNo": 902,
        "TradeBSFlag": "B",
        "BizIndex": 3,
    }
    line.update(kw)
    return line


def _od(**kw):
    line = {
        "type": "od",
        "SecurityID": "600000",
        "ExchangeID": "1",
        "OrderTime": 93002123,
        "Price": 8.32,
        "Volume": 200,
        "Side": "1",
        "OrderType": "2",
        "MainSeq": 2,
        "SubSeq": 8,
        "OrderNO": 903,
        "OrderStatus": "A",
        "BizIndex": 4,
    }
    line.update(kw)
    return line


def test_decode_ts():
    # HHMMSSmmm (8-9 digits) and HHMMSS (5-6 digits), plus passthrough
    assert decode_ts(93001123) == (9 * 3600 + 30 * 60 + 1) * 10**9 + 123_000_000
    assert decode_ts(93001) == (9 * 3600 + 30 * 60 + 1) * 10**9
    assert decode_ts(143001123) == (14 * 3600 + 30 * 60 + 1) * 10**9 + 123_000_000
    assert decode_ts(34200000000000) == 34200000000000  # already ns


def test_md_symbol_of():
    assert md_symbol_of("600000", "1") == "sh600000"
    assert md_symbol_of("000001", "2") == "sz000001"


def test_md_updates_book_and_emits_nothing():
    norm = Normalizer(TRADING_DAY)
    assert norm.normalize_line(_md()) == []
    book = norm.book("sh600000")
    assert book.bid_price[0] == 828  # 8.29-0.01 = 8.28 yuan → 828 cents
    assert book.ask_price[0] == 831  # 8.30+0.01 = 8.31
    assert book.bid_qty[0] == 1001
    assert book.ask_orders[0] == 21
    assert book.total_volume == 12345


def test_transaction_to_trade_event_stamps_book():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md())
    [ev] = norm.normalize_line(_tr())
    assert ev.event == 1 and ev.sign == 1  # TradeBSFlag 'B'
    assert ev.price == 831.0 and ev.vol_traded == 100.0  # cents / shares
    assert ev.t_ns == decode_ts(93001123)
    assert ev.extra["qty"] == 100.0
    assert ev.extra["order_id"] == 901  # BuyNo (aggressor buy)
    # book stamped from the md snapshot
    assert ev.extra["bid_price_0"] == 828 and ev.extra["ask_price_0"] == 831


def test_transaction_sell_sign_and_sellno():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md())
    [ev] = norm.normalize_line(_tr(TradeBSFlag="S", BuyNo=901, SellNo=902))
    assert ev.sign == -1 and ev.extra["order_id"] == 902  # SellNo


def test_transaction_auction_stat_is_event_4():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md(MDSecurityStat="1"))
    [ev] = norm.normalize_line(_tr())
    assert ev.event == 4


def test_transaction_sz_exec_cancel_is_event_2():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md())
    [ev] = norm.normalize_line(_tr(ExchangeID="2", SecurityID="000001", ExecType="2", TradeBSFlag="N"))
    assert ev.event == 2 and ev.vol_placed == 100.0 and ev.vol_traded == 0.0


def test_order_detail_add_and_delete():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md())
    [add] = norm.normalize_line(_od())  # Side '1' buy, Status 'A'
    assert add.event == 0 and add.sign == 1
    assert add.price == 832.0 and add.vol_placed == 200.0
    assert add.extra["order_id"] == 903
    # level_placed: order price 832 ≥ bid_1 829 → level 1
    assert add.level_placed == 1.0
    [delete] = norm.normalize_line(
        _od(OrderStatus="D", Side="2", Price=8.28, MainSeq=3, SubSeq=9)
    )
    assert delete.event == 2 and delete.sign == -1


def test_resend_dedupe_by_main_sub_seq():
    norm = Normalizer(TRADING_DAY)
    norm.normalize_line(_md())
    evs = norm.normalize_line(_tr())
    assert len(evs) == 1
    assert norm.normalize_line(_tr(type="rtr")) == []  # resend of same seq
    assert norm.normalize_line(_tr(MainSeq=3, type="rtr"))  # new seq passes


def test_level_placed_missing_book_is_zero():
    norm = Normalizer(TRADING_DAY)
    [add] = norm.normalize_line(_od())
    assert add.level_placed == 0.0


def test_normalize_capture_file(tmp_path):
    import json

    path = tmp_path / "capture.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        f.write("CONNECT_TIMER [x]:[y]...\n")  # .so chatter — skipped
        f.writelines(json.dumps(line) + "\n" for line in (_md(), _tr(), _od(), {"type": "login", "error_id": 0}))
    events = normalize_capture(path, TRADING_DAY)
    assert [ev.event for ev in events] == [1, 0]
    assert all(ev.extra["ask_qty_0"] == 2001 for ev in events)  # book stamped
