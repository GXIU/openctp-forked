"""Smoke test: connect mdapi to TTS 7x24 sim, subscribe a stock, print ticks.

Usage: uv run python smoke_md.py [instrument]  (default sh600000)
"""
import sys
import time

from openctp_ctp import thostmduserapi as mdapi

FRONT = "tcp://180.168.146.187:10131"  # TTS 7x24 行情前置
instrument = sys.argv[1] if len(sys.argv) > 1 else "sh600000"


class MdImpl(mdapi.CThostFtdcMdSpi):
    def __init__(self):
        super().__init__()
        self.api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi()
        self.api.RegisterSpi(self)
        self.api.RegisterFront(FRONT)
        self.got = 0

    def OnFrontConnected(self):
        print("OnFrontConnected")
        self.api.ReqUserLogin(mdapi.CThostFtdcReqUserLoginField(), 0)

    def OnRspUserLogin(self, rsp, info, req_id, is_last):
        print(f"Login: ErrorID={info.ErrorID} {info.ErrorMsg}")
        self.api.SubscribeMarketData([instrument.encode()], 1)

    def OnRspSubMarketData(self, specific, info, req_id, is_last):
        print(f"Subscribed: {specific.InstrumentID}")

    def OnRtnDepthMarketData(self, d):
        self.got += 1
        print(f"tick {self.got}: {d.InstrumentID} last={d.LastPrice} "
              f"vol={d.Volume} bid1={d.BidPrice1}/{d.BidVolume1} "
              f"ask1={d.AskPrice1}/{d.AskVolume1} t={d.UpdateTime}.{d.UpdateMillisec}")


def main():
    impl = MdImpl()
    impl.api.Init()
    deadline = time.time() + 30
    while time.time() < deadline and impl.got < 3:
        time.sleep(0.1)
    impl.api.Release()
    print(f"got {impl.got} ticks")


if __name__ == "__main__":
    main()
