#include <climits>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <filesystem>

using json = nlohmann::json;

struct MarketDataEvent {
    std::string ts_recv = "";
    std::string ts_event = "";
    uint8_t rtype = 0;
    uint16_t publisher_id = 0;
    uint32_t instrument_id = UINT32_MAX;
    char action = '\0';
    char side = 'N';
    int64_t price = INT64_MAX;
    uint32_t size = UINT32_MAX;
    uint32_t channel_id = 0;
    uint64_t order_id = UINT64_MAX;
    uint8_t flags = 0;
    int32_t ts_in_delta = 0;
    uint32_t sequence = 0;
    std::string symbol = "";
};

static int64_t parsePrice(const std::string &s) {
    bool negative = (!s.empty() && s[0] == '-');
    std::size_t start = negative ? 1 : 0;
    auto dot = s.find('.', start);

    std::string intPart = (dot == std::string::npos)
                            ? s.substr(start)
                            : s.substr(start, dot - start);
    std::string fracPart = (dot == std::string::npos) ? "" : s.substr(dot + 1);

    // Pad or truncate fractional part to exactly 9 digits.
    if (fracPart.size() < 9) {
        fracPart.append(9 - fracPart.size(), '0');
    } else {
        fracPart = fracPart.substr(0, 9);
    }

    int64_t result = 0;
    for (char c : intPart) {
        result = result * 10 + (c - '0');
    }

    result *= 1'000'000'000LL;
    for (char c : fracPart) {
        result = result * 10 + (c - '0');
    }


    if (negative) {
        result *= -1;
    }

    return result;
}

MarketDataEvent parseEvent(const json &j) {
    MarketDataEvent e;

    e.ts_recv = j.value("ts_recv", "");

    const auto &hd = j.at("hd");
    e.ts_event = hd.value("ts_event", "");
    e.rtype = hd.value("rtype", uint8_t{0});
    e.publisher_id = hd.value("publisher_id", uint16_t{0});
    e.instrument_id = hd.value("instrument_id", uint32_t{0});

    std::string actionStr = j.value("action", "");
    e.action = actionStr.empty() ? '\0' : actionStr[0];

    std::string sideStr = j.value("side", "");
    e.side = sideStr.empty() ? '\0' : sideStr[0];

    if (j.at("price").is_null()) {
        e.price = INT64_MAX;
    } else {
        e.price = parsePrice(j.at("price").get<std::string>());
    }

    e.size = j.value("size", uint32_t{0});
    e.channel_id = j.value("channel_id", uint32_t{0});
    e.order_id = std::stoull(j.at("order_id").get<std::string>());
    e.flags = j.value("flags", uint8_t{0});
    e.ts_in_delta = j.value("ts_in_delta", int32_t{0});
    e.sequence = j.value("sequence", uint32_t{0});
    e.symbol = j.value("symbol", "");

    return e;
}

void processMarketDataEvent(const MarketDataEvent &e) {
    std::string priceStr;
    if (e.price == INT64_MAX) {
        priceStr = "UNDEF";
    } else {
    bool negative = (e.price < 0);
    int64_t abs = negative ? -e.price : e.price;
    int64_t intPart = abs / 1'000'000'000LL;
    int64_t fracPart = abs % 1'000'000'000LL;

    char buf[32];
    std::snprintf(buf, sizeof(buf), "%s%lld.%09lld",
                    negative ? "-" : "",
                    static_cast<long long>(intPart),
                    static_cast<long long>(fracPart));
    priceStr = buf;
    }

    std::cout << "ts_event=" << e.ts_event
            << " order_id=" << e.order_id
            << " side=" << e.side
            << " price=" << priceStr
            << " size=" << e.size
            << " action=" << e.action
            << "\n";
}

void processOneFile(const std::string& filename) {
    std::ifstream in(filename);
    if (!in) {
        std::cerr << "Error: could not open file: " << filename << "\n";
        return;
    }

    std::size_t count = 0;
    std::string firstTs, lastTs;
    std::vector<MarketDataEvent> firstTen;
    std::deque<MarketDataEvent> lastTen;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;

        MarketDataEvent e;
        try {
            json j = json::parse(line);
            e = parseEvent(j);
        } catch (const std::exception &ex) {
            std::cerr << "Warning: skipping line " << (count + 1) << " — " << ex.what() << "\n";
            continue;
        }

        ++count;

        if (firstTs.empty()) {
            firstTs = e.ts_recv;
        }
        lastTs = e.ts_recv;

        if (firstTen.size() < 10) {
            firstTen.push_back(e);
        }

        lastTen.push_back(e);
        if (lastTen.size() > 10) {
            lastTen.pop_front();
        }
    }

    std::cout << "First 10 events\n";
    for (const auto &e : firstTen) {
        processMarketDataEvent(e);
    }

    std::cout << "\nLast 10 events\n";
    for (const auto &e : lastTen) {
        processMarketDataEvent(e);
    }

    std::cout << "\nTotal messages : " << count << "\n";
    std::cout << "First ts_recv : " << firstTs << "\n";
    std::cout << "Last ts_recv : " << lastTs << "\n";
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <ndjson-file>\n";
        return 1;
    }

    processOneFile(argv[1]);
    return 0;
}