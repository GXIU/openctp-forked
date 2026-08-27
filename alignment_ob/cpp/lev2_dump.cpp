// lev2_dump — M0 raw capture: TORA L2 (华鑫奇点) streams → JSONL on stdout.
//
// Links liblev2mdapi.so (API_Stock_lv2_C++ v4.0.8). Prints one JSON object
// per callback: {"type": "md|tr|od|rtr|rod|rsp|front|login", ...raw fields...}
// — raw field values verbatim (prices double YUAN, timestamps int as-is).
// M1 normalization happens later in Python (alignment_ob/normalize.py).
//
// Build (g++):
//   g++ -O2 -std=c++17 cpp/lev2_dump.cpp -o sdk/bin/lev2_dump \
//       -L sdk/C++_API_v4.0.8_20251126 -llev2mdapi \
//       -Wl,-rpath,'$ORIGIN/../C++_API_v4.0.8_20251126' -pthread
//
// Usage (China host):
//   lev2_dump <front> <user> <password> <acct_type> <sids_csv> [resend=0|1]
//   e.g. lev2_dump tcp://210.14.72.21:4402 00974579 '***' 1 600000,000001 1
//   env TORA_DEPARTMENT=<一级机构代码>  (required by some account logins)
//
// Flow: Create → RegisterSpi → RegisterFront → DeclareMKSubTypes("MTO"[+"RD"])
// → Init → OnFrontConnected → ReqUserLogin → OnRspUserLogin → Subscribe
// (MarketData + Transaction + OrderDetail [+ Resend*] per sid) → Join.
// Kill with Ctrl-C; the stream is appendable across restarts (dedupe is the
// normalizer's job).

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unistd.h>

#include "../sdk/C++_API_v4.0.8_20251126/TORATstpLev2MdApi.h"

using namespace TORALEV2API;

namespace {

// ---- minimal JSON emitter (no deps) --------------------------------

void j_str(std::string& out, const char* s) {
    out += '"';
    for (const char* p = s; *p; ++p) {
        char c = *p;
        if (c == '"' || c == '\\') { out += '\\'; out += c; }
        else if (c < 0x20) { char buf[8]; std::snprintf(buf, 8, "\\u%04x", c); out += buf; }
        else out += c;
    }
    out += '"';
}

template <typename T>
void j_field(std::string& out, const char* name, T v) {
    j_str(out, name);
    out += ':';
    if constexpr (std::is_same_v<T, char>) j_str(out, std::string(1, v).c_str());
    else if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*>) j_str(out, v);
    else if constexpr (std::is_floating_point_v<T>) {
        char buf[32];
        std::snprintf(buf, 32, "%.6f", v);
        out += buf;
    } else out += std::to_string(v);
}

void emit(const std::string& type, const std::string& body) {
    std::string out = "{\"type\":";
    j_str(out, type.c_str());
    out += ',';
    out += body;
    out += "}\n";
    std::fputs(out.c_str(), stdout);
    std::fflush(stdout);
}

// Appends fields comma-separated (first call adds no comma).
struct Fields {
    std::string body;
    bool first = true;
    template <typename T>
    void add(const char* name, T v) {
        if (!first) body += ',';
        first = false;
        j_field(body, name, v);
    }
};

// ---- market-data accessors (struct fields are NOT contiguous) -------

double& bid_price(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->BidPrice1; case 2: return p->BidPrice2;
    case 3: return p->BidPrice3; case 4: return p->BidPrice4; case 5: return p->BidPrice5;
    case 6: return p->BidPrice6; case 7: return p->BidPrice7; case 8: return p->BidPrice8;
    case 9: return p->BidPrice9; default: return p->BidPrice10; }
}
double& ask_price(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->AskPrice1; case 2: return p->AskPrice2;
    case 3: return p->AskPrice3; case 4: return p->AskPrice4; case 5: return p->AskPrice5;
    case 6: return p->AskPrice6; case 7: return p->AskPrice7; case 8: return p->AskPrice8;
    case 9: return p->AskPrice9; default: return p->AskPrice10; }
}
long long& bid_volume(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->BidVolume1; case 2: return p->BidVolume2;
    case 3: return p->BidVolume3; case 4: return p->BidVolume4; case 5: return p->BidVolume5;
    case 6: return p->BidVolume6; case 7: return p->BidVolume7; case 8: return p->BidVolume8;
    case 9: return p->BidVolume9; default: return p->BidVolume10; }
}
long long& ask_volume(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->AskVolume1; case 2: return p->AskVolume2;
    case 3: return p->AskVolume3; case 4: return p->AskVolume4; case 5: return p->AskVolume5;
    case 6: return p->AskVolume6; case 7: return p->AskVolume7; case 8: return p->AskVolume8;
    case 9: return p->AskVolume9; default: return p->AskVolume10; }
}
int& bid_num_orders(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->Bid1NumOrders; case 2: return p->Bid2NumOrders;
    case 3: return p->Bid3NumOrders; case 4: return p->Bid4NumOrders; case 5: return p->Bid5NumOrders;
    case 6: return p->Bid6NumOrders; case 7: return p->Bid7NumOrders; case 8: return p->Bid8NumOrders;
    case 9: return p->Bid9NumOrders; default: return p->Bid10NumOrders; }
}
int& ask_num_orders(CTORATstpLev2MarketDataField* p, int i) {
    switch (i) { case 1: return p->Ask1NumOrders; case 2: return p->Ask2NumOrders;
    case 3: return p->Ask3NumOrders; case 4: return p->Ask4NumOrders; case 5: return p->Ask5NumOrders;
    case 6: return p->Ask6NumOrders; case 7: return p->Ask7NumOrders; case 8: return p->Ask8NumOrders;
    case 9: return p->Ask9NumOrders; default: return p->Ask10NumOrders; }
}

// ---- SPI: every callback → one JSON line ----------------------------

class Spi : public CTORATstpLev2MdSpi {
public:
    std::atomic<bool> connected{false};

    void OnFrontConnected() override { emit("front", "\"connected\":true"); connected = true; }

    void OnFrontDisconnected(int nReason) override {
        std::string body = "\"reason\":";
        body += std::to_string(nReason);
        emit("front", body);
    }

    void OnRspError(CTORATstpRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override {
        (void)bIsLast;
        Fields f;
        f.add("request_id", nRequestID);
        f.add("error_id", pRspInfo ? pRspInfo->ErrorID : 0);
        if (pRspInfo) f.add("error_msg", pRspInfo->ErrorMsg);
        emit("rsp_error", f.body);
    }

    void OnRspSubMarketData(CTORATstpSpecificSecurityField* pSpecificSecurity,
                            CTORATstpRspInfoField* pRspInfo, int nRequestID,
                            bool bIsLast) override {
        (void)bIsLast;
        Fields f;
        f.add("security_id", pSpecificSecurity ? pSpecificSecurity->SecurityID : "");
        f.add("error_id", pRspInfo ? pRspInfo->ErrorID : 0);
        if (pRspInfo) f.add("error_msg", pRspInfo->ErrorMsg);
        emit("sub_md", f.body);
    }

    void OnRspSubTransaction(CTORATstpSpecificSecurityField* pSpecificSecurity,
                             CTORATstpRspInfoField* pRspInfo, int nRequestID,
                             bool bIsLast) override {
        (void)bIsLast;
        Fields f;
        f.add("security_id", pSpecificSecurity ? pSpecificSecurity->SecurityID : "");
        f.add("error_id", pRspInfo ? pRspInfo->ErrorID : 0);
        if (pRspInfo) f.add("error_msg", pRspInfo->ErrorMsg);
        emit("sub_tr", f.body);
    }

    void OnRspSubOrderDetail(CTORATstpSpecificSecurityField* pSpecificSecurity,
                             CTORATstpRspInfoField* pRspInfo, int nRequestID,
                             bool bIsLast) override {
        (void)bIsLast;
        Fields f;
        f.add("security_id", pSpecificSecurity ? pSpecificSecurity->SecurityID : "");
        f.add("error_id", pRspInfo ? pRspInfo->ErrorID : 0);
        if (pRspInfo) f.add("error_msg", pRspInfo->ErrorMsg);
        emit("sub_od", f.body);
    }

    void OnRspUserLogin(CTORATstpRspUserLoginField* pRspUserLogin,
                        CTORATstpRspInfoField* pRspInfo, int nRequestID,
                        bool bIsLast) override {
        (void)pRspUserLogin; (void)bIsLast;
        Fields f;
        f.add("request_id", nRequestID);
        f.add("error_id", pRspInfo ? pRspInfo->ErrorID : 0);
        if (pRspInfo) f.add("error_msg", pRspInfo->ErrorMsg);
        emit("login", f.body);
    }

    void OnRtnMarketData(CTORATstpLev2MarketDataField* p,
                         const int FirstLevelBuyNum,
                         const int FirstLevelBuyOrderVolumes[],
                         const int FirstLevelSellNum,
                         const int FirstLevelSellOrderVolumes[]) override {
        (void)FirstLevelBuyNum; (void)FirstLevelBuyOrderVolumes;
        (void)FirstLevelSellNum; (void)FirstLevelSellOrderVolumes;
        Fields f;
        f.add("SecurityID", p->SecurityID);
        f.add("ExchangeID", p->ExchangeID); f.add("DataTimeStamp", p->DataTimeStamp);
        f.add("PreClosePrice", p->PreClosePrice); f.add("OpenPrice", p->OpenPrice);
        f.add("NumTrades", p->NumTrades); f.add("TotalVolumeTrade", p->TotalVolumeTrade);
        f.add("TotalValueTrade", p->TotalValueTrade); f.add("TotalBidVolume", p->TotalBidVolume);
        f.add("AvgBidPrice", p->AvgBidPrice); f.add("TotalAskVolume", p->TotalAskVolume);
        f.add("AvgAskPrice", p->AvgAskPrice); f.add("HighestPrice", p->HighestPrice);
        f.add("LowestPrice", p->LowestPrice); f.add("LastPrice", p->LastPrice);
        f.add("IOPV", p->IOPV); f.add("Info1", p->Info1); f.add("Info2", p->Info2);
        f.add("Info3", p->Info3); f.add("UpperLimitPrice", p->UpperLimitPrice);
        f.add("LowerLimitPrice", p->LowerLimitPrice); f.add("ClosePrice", p->ClosePrice);
        f.add("MDSecurityStat", p->MDSecurityStat); f.add("TotalBidNumber", p->TotalBidNumber);
        f.add("TotalOfferNumber", p->TotalOfferNumber);
        f.add("BidTradeMaxDuration", p->BidTradeMaxDuration);
        f.add("OfferTradeMaxDuration", p->OfferTradeMaxDuration);
        f.add("WithdrawBuyNumber", p->WithdrawBuyNumber);
        f.add("WithdrawBuyAmount", p->WithdrawBuyAmount);
        f.add("WithdrawBuyMoney", p->WithdrawBuyMoney);
        f.add("WithdrawSellNumber", p->WithdrawSellNumber);
        f.add("WithdrawSellAmount", p->WithdrawSellAmount);
        f.add("WithdrawSellMoney", p->WithdrawSellMoney);
        for (int i = 1; i <= 10; ++i) {
            char name[16];
            std::snprintf(name, 16, "BidPrice%d", i);   f.add(name, bid_price(p, i));
            std::snprintf(name, 16, "BidVolume%d", i);  f.add(name, bid_volume(p, i));
            std::snprintf(name, 16, "AskPrice%d", i);   f.add(name, ask_price(p, i));
            std::snprintf(name, 16, "AskVolume%d", i);  f.add(name, ask_volume(p, i));
            std::snprintf(name, 16, "Bid%dNumOrders", i); f.add(name, bid_num_orders(p, i));
            std::snprintf(name, 16, "Ask%dNumOrders", i); f.add(name, ask_num_orders(p, i));
        }
        emit("md", f.body);
    }

    void OnRtnTransaction(CTORATstpLev2TransactionField* p) override {
        Fields f;
        f.add("SecurityID", p->SecurityID);
        f.add("ExchangeID", p->ExchangeID); f.add("TradeTime", p->TradeTime);
        f.add("TradePrice", p->TradePrice); f.add("TradeVolume", p->TradeVolume);
        f.add("ExecType", p->ExecType); f.add("MainSeq", p->MainSeq);
        f.add("SubSeq", p->SubSeq); f.add("BuyNo", p->BuyNo); f.add("SellNo", p->SellNo);
        f.add("Info1", p->Info1); f.add("Info2", p->Info2); f.add("Info3", p->Info3);
        f.add("TradeBSFlag", p->TradeBSFlag); f.add("BizIndex", p->BizIndex);
        emit("tr", f.body);
    }

    void OnRtnOrderDetail(CTORATstpLev2OrderDetailField* p) override {
        Fields f;
        f.add("SecurityID", p->SecurityID);
        f.add("ExchangeID", p->ExchangeID); f.add("OrderTime", p->OrderTime);
        f.add("Price", p->Price); f.add("Volume", p->Volume);
        f.add("Side", p->Side); f.add("OrderType", p->OrderType);
        f.add("MainSeq", p->MainSeq); f.add("SubSeq", p->SubSeq);
        f.add("Info1", p->Info1); f.add("Info2", p->Info2); f.add("Info3", p->Info3);
        f.add("OrderNO", p->OrderNO); f.add("OrderStatus", p->OrderStatus);
        f.add("BizIndex", p->BizIndex);
        emit("od", f.body);
    }

    void OnRtnResendTransaction(CTORATstpLev2ResendTransactionField* p) override {
        Fields f;
        f.add("SecurityID", p->SecurityID);
        f.add("ExchangeID", p->ExchangeID); f.add("TradeTime", p->TradeTime);
        f.add("TradePrice", p->TradePrice); f.add("TradeVolume", p->TradeVolume);
        f.add("ExecType", p->ExecType); f.add("MainSeq", p->MainSeq);
        f.add("SubSeq", p->SubSeq); f.add("BuyNo", p->BuyNo); f.add("SellNo", p->SellNo);
        f.add("Info1", p->Info1); f.add("Info2", p->Info2); f.add("Info3", p->Info3);
        f.add("TradeBSFlag", p->TradeBSFlag); f.add("BizIndex", p->BizIndex);
        emit("rtr", f.body);
    }

    void OnRtnResendOrderDetail(CTORATstpLev2ResendOrderDetailField* p) override {
        Fields f;
        f.add("SecurityID", p->SecurityID);
        f.add("ExchangeID", p->ExchangeID); f.add("OrderTime", p->OrderTime);
        f.add("Price", p->Price); f.add("Volume", p->Volume);
        f.add("Side", p->Side); f.add("OrderType", p->OrderType);
        f.add("MainSeq", p->MainSeq); f.add("SubSeq", p->SubSeq);
        f.add("Info1", p->Info1); f.add("Info2", p->Info2); f.add("Info3", p->Info3);
        f.add("OrderNO", p->OrderNO); f.add("OrderStatus", p->OrderStatus);
        f.add("BizIndex", p->BizIndex);
        emit("rod", f.body);
    }
};

char exch_of(const std::string& sid) {
    // 6xxxxx → SSE('1'), 0xxxxx/3xxxxx → SZSE('2')
    return (sid.size() >= 1 && sid[0] == '6') ? TORA_TSTP_EXD_SSE : TORA_TSTP_EXD_SZSE;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 6) {
        std::fprintf(stderr,
            "usage: lev2_dump <front> <user> <password> <acct_type> <sids_csv> [resend=0|1]\n"
            "  acct_type: '0' UserID, '1' 资金账号\n");
        return 2;
    }
    const std::string front = argv[1], user = argv[2], password = argv[3],
                      acct_type = argv[4], sids_csv = argv[5];
    const bool resend = argc > 6 && std::string(argv[6]) == "1";

    std::fprintf(stderr, "lev2mdapi version: %s\n", CTORATstpLev2MdApi::GetApiVersion());

    CTORATstpLev2MdApi* api = CTORATstpLev2MdApi::CreateTstpLev2MdApi(TORA_TSTP_MST_TCP, false);
    Spi spi;
    api->RegisterSpi(&spi);
    char front_buf[256];
    std::snprintf(front_buf, 256, "%s", front.c_str());
    api->RegisterFront(front_buf);

    // M snapshot + T transaction + O orderdetail (+ R/D resend streams)
    std::string sub = "MTO";
    if (resend) sub += "RD";
    TTORATstpMKSubTypesType sub_types{};
    std::memcpy(sub_types, sub.c_str(), sub.size());
    api->DeclareMKSubTypes(sub_types);

    api->Init();

    // wait for OnFrontConnected (sleep-poll; login after connect)
    for (int i = 0; i < 200 && !spi.connected; ++i) usleep(50 * 1000);
    if (!spi.connected) {
        std::fprintf(stderr, "no front connection after 10s\n");
        return 3;
    }

    CTORATstpReqUserLoginField login{};
    std::snprintf(login.LogInAccount, sizeof(login.LogInAccount), "%s", user.c_str());
    login.LogInAccountType = acct_type.empty() ? TORA_TSTP_LACT_AccountID : acct_type[0];
    std::snprintf(login.Password, sizeof(login.Password), "%s", password.c_str());
    login.AuthMode = TORA_TSTP_AM_Password;
    const char* dept = std::getenv("TORA_DEPARTMENT");
    if (dept) std::snprintf(login.DepartmentID, sizeof(login.DepartmentID), "%s", dept);
    api->ReqUserLogin(&login, 1);

    // split sids, subscribe all three streams per sid
    std::vector<std::string> sids;
    std::string cur;
    for (char c : sids_csv) {
        if (c == ',') { if (!cur.empty()) sids.push_back(cur), cur.clear(); }
        else cur += c;
    }
    if (!cur.empty()) sids.push_back(cur);

    for (auto& s : sids) {
        char ex = exch_of(s);
        char* one[1] = {const_cast<char*>(s.c_str())};
        api->SubscribeMarketData(one, 1, ex);
        api->SubscribeTransaction(one, 1, ex);
        api->SubscribeOrderDetail(one, 1, ex);
        if (resend) {
            api->SubscribeResendTransaction(one, 1, ex);
            api->SubscribeResendOrderDetail(one, 1, ex);
        }
    }
    std::fprintf(stderr, "subscribed %zu sids (resend=%d)\n", sids.size(), resend ? 1 : 0);

    api->Join();  // block until process killed
    return 0;
}
