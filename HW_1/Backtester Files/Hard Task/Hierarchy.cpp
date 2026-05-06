#include <climits>
#include <cstdint>
#include <ctime>
#include <deque>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <filesystem>
#include <thread>
#include <vector>
#include <queue>
#include <chrono>

using json = nlohmann::json;
namespace fs = std::filesystem;

struct MarketDataEvent {
    uint64_t ts_recv = UINT64_MAX;
    uint64_t ts_event = UINT64_MAX;
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

    bool operator<(const MarketDataEvent& other) const {
        if (ts_recv != other.ts_recv) {
            return ts_recv < other.ts_recv;
        }
        if (ts_event != other.ts_event) {
            return ts_event < other.ts_event;
        }

        return sequence < other.sequence;
    }
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

static uint64_t parseTimestamp(const std::string &s) {
    if (s.empty()) {
        return UINT64_MAX;
    }
    // Expected format: YYYY-MM-DDTHH:MM:SS[.fraction]Z
    std::tm tm{};
    tm.tm_year = std::stoi(s.substr(0, 4)) - 1900;
    tm.tm_mon  = std::stoi(s.substr(5, 2)) - 1;
    tm.tm_mday = std::stoi(s.substr(8, 2));
    tm.tm_hour = std::stoi(s.substr(11, 2));
    tm.tm_min  = std::stoi(s.substr(14, 2));
    tm.tm_sec  = std::stoi(s.substr(17, 2));
    time_t secs = timegm(&tm);

    uint64_t ns = 0;
    auto dot = s.find('.');
    if (dot != std::string::npos) {
        std::string frac = s.substr(dot + 1);
        if (!frac.empty() && frac.back() == 'Z') {
            frac.pop_back();
        }
        if (frac.size() < 9) {
            frac.append(9 - frac.size(), '0');
        } else {
            frac = frac.substr(0, 9);
        }
        for (char c : frac) {
            ns = ns * 10 + (c - '0');
        }
    }
    return static_cast<uint64_t>(secs) * 1'000'000'000ULL + ns;
}

static std::string formatTimestamp(uint64_t ns) {
    if (ns == UINT64_MAX) {
        return "UNDEF";
    }
    time_t secs = static_cast<time_t>(ns / 1'000'000'000ULL);
    uint64_t frac = ns % 1'000'000'000ULL;
    std::tm tm{};
    gmtime_r(&secs, &tm);
    char buf[40];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%09lluZ",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec,
                  static_cast<unsigned long long>(frac));
    return buf;
}

MarketDataEvent parseEvent(const json &j) {
    MarketDataEvent e;

    e.ts_recv = parseTimestamp(j.value("ts_recv", ""));

    const auto &hd = j.at("hd");
    e.ts_event = parseTimestamp(hd.value("ts_event", ""));
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

    std::cout << "ts_event=" << formatTimestamp(e.ts_event)
            << " order_id=" << e.order_id
            << " side=" << e.side
            << " price=" << priceStr
            << " size=" << e.size
            << " action=" << e.action
            << "\n";
}

std::vector<MarketDataEvent> processOneFile(const std::string& filename) {
    std::vector<MarketDataEvent> events;
    std::ifstream in(filename);
    if (!in) {
        std::cerr << "Error: could not open file: " << filename << "\n";
        return {};
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;

        MarketDataEvent e;
        try {
            json j = json::parse(line);
            e = parseEvent(j);
        } catch (const std::exception &ex) {
            std::cerr << "Warning: skipping line " << ex.what() << "\n";
            continue;
        }

        events.push_back(e);
    }

    return events;
}


struct DataForSet {
    MarketDataEvent event;
    int ind;

    bool operator<(const DataForSet& other) const {
        if (event < other.event) {
            return false;
        }
        if (other.event < event) {
            return true; // I invert the comparison here to use priority_queue which uses max-heap by default!!!
        }

        return ind < other.ind;
    }
};

std::vector<MarketDataEvent> FlatMerge(std::vector<std::vector<MarketDataEvent>>& events_of_files) {
    int n = events_of_files.size();
    for (int i = 0; i < n; ++i) {
        std::reverse(events_of_files[i].begin(), events_of_files[i].end());
    }

    std::vector<MarketDataEvent> result;
    std::priority_queue<DataForSet> pq;
    for (int i = 0; i < n; ++i) {
        if (!events_of_files[i].empty()) {
            pq.push({events_of_files[i].back(), i});
            events_of_files[i].pop_back();
        }
    }

    while(!pq.empty()) {
        auto [event, ind] = pq.top();
        pq.pop();
        result.push_back(event);

        if (!events_of_files[ind].empty()) {
            pq.push({events_of_files[ind].back(), ind});
            events_of_files[ind].pop_back();
        }
    }

    return result;
}

std::vector<MarketDataEvent> Merge2Vecs(const std::vector<MarketDataEvent>& vec1, const std::vector<MarketDataEvent>& vec2) {
    std::vector<MarketDataEvent> result;
    result.reserve(vec1.size() + vec2.size());
    int left = 0, right = 0;
    
    while (left < vec1.size() || right < vec2.size()) {
        if (left < vec1.size() && right < vec2.size()) {
            if (vec1[left] < vec2[right]) {
                result.push_back(vec1[left]);
                left++;
            } else {
                result.push_back(vec2[right]);
                right++;    
            }
            continue;
        }

        if (left < vec1.size()) {
            result.push_back(vec1[left]);
            left++;
            continue;
        }

        result.push_back(vec2[right]);
        right++;
    }

    return result;
}

std::vector<MarketDataEvent> HierarchyMerge(const std::vector<std::vector<MarketDataEvent>>& events_of_files) {
    if (events_of_files.size() == 0) {
        return {};
    }

    if (events_of_files.size() == 1) {
        return events_of_files[0];
    }

    std::vector<std::vector<MarketDataEvent>> tmp;
    int left = 0, right = events_of_files.size() - 1;

    while (left <= right) {
        if (left < right) {
            tmp.push_back(Merge2Vecs(events_of_files[left], events_of_files[right]));
            left++;
            right--;
            continue;
        }
        tmp.push_back(events_of_files[left]);
        left++;
        right--;
    }

    return HierarchyMerge(tmp);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <ndjson-file>\n";
        return 1;
    }

    std::string folder_path = argv[1];
    std::vector<std::thread> producers;

    try {
        if (fs::exists(folder_path) && fs::is_directory(folder_path)) {
            auto p0 = std::chrono::steady_clock::now();
            int cnt = 0;
            for (const auto& file : fs::directory_iterator(folder_path)) {
                if (fs::is_regular_file(file.path())) {  
                    const std::string filepath = file.path().string();                                                                                                                                                    
                    if (filepath.size() < 9 ||
                        filepath.substr(filepath.size() - 9) != ".mbo.json") { 
                            continue;
                    }
                    cnt++;
                }
            }

            std::vector<std::vector<MarketDataEvent>> events_of_files(cnt);

            int curr = 0;
            for (const auto& file : fs::directory_iterator(folder_path)) {
                if (fs::is_regular_file(file.path())) {  
                    const std::string filepath = file.path().string();                                                                                                                                                    
                    if (filepath.size() < 9 ||
                        filepath.substr(filepath.size() - 9) != ".mbo.json") { 
                            continue;
                    }

                    std::cout << "--- Processing file: " << file.path().filename() << " ---\n";
                    
                    producers.emplace_back([curr, filepath, &events_of_files](){
                        events_of_files[curr] = processOneFile(filepath);
                    });
                    curr++;

                    std::cout << std::endl;
                }
            }

            for (auto& t : producers) {
                if (t.joinable()) {
                    t.join();
                }
            }

            auto p1 = std::chrono::steady_clock::now();
            std::cout << "All files processed successfully.\n";

            // Silence stdout — printing 31M lines from the dispatcher would dominate wall-clock.
            std::ofstream null_sink("/dev/null");
            auto* old_buf = std::cout.rdbuf(null_sink.rdbuf());

            auto m0 = std::chrono::steady_clock::now();
            std::vector<MarketDataEvent> merged = HierarchyMerge(events_of_files);

            std::thread dispatcher([&merged]() {                                                                                                                                                                                                  
                for (const auto& e : merged) {                                                                                                                                                                                                    
                    processMarketDataEvent(e);                                                                                                                                                                                                    
                }
            });                                                                                                                                                                                                                                   
            dispatcher.join();
            auto m1 = std::chrono::steady_clock::now();

            std::cout.rdbuf(old_buf);  // restore stdout for the report

            double parse_s = std::chrono::duration<double>(p1 - p0).count();
            double merge_s = std::chrono::duration<double>(m1 - m0).count();
            double total_s = parse_s + merge_s;
            std::size_t msgs = merged.size();

            std::cerr << "[HierarchyMerge]"
                      << " msgs=" << msgs
                      << " parse=" << parse_s << "s"
                      << " merge+dispatch=" << merge_s << "s"
                      << " total=" << total_s << "s"
                      << " throughput=" << (msgs / total_s) << " msgs/s\n";

        } else {
            std::cerr << "Error: " << folder_path << " is not a valid folder path\n";
            return 1;
        }
    } catch (const fs::filesystem_error& ex) {
        std::cerr << "Filesystem error: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}