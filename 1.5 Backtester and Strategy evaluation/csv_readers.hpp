#pragma once

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>

struct BookSnapshot {
    uint64_t local_timestamp;
    std::array<double, 25> ask_price;
    std::array<double, 25> ask_amount;
    std::array<double, 25> bid_price;
    std::array<double, 25> bid_amount;
};

struct Trade {
    uint64_t local_timestamp;
    char side;    // 'B' for "buy", 'S' for "sell"
    double price;
    double amount;
};

namespace csv_detail {

// Advance p past the next comma (or to '\0'). Returns false if no comma found.
inline bool skip_comma(const char*& p) {
    while (*p && *p != ',') ++p;
    if (*p == ',') { ++p; return true; }
    return false;  // reached end without comma
}

// Parse uint64 starting at p, advance p past the value (stops at ',' or '\0').
inline bool read_u64(const char*& p, uint64_t& out) {
    if (!*p) return false;
    char* end = nullptr;
    out = std::strtoull(p, &end, 10);
    if (end == p) return false;
    p = end;
    return true;
}

// Parse double starting at p, advance p past the value.
inline bool read_f64(const char*& p, double& out) {
    if (!*p) return false;
    char* end = nullptr;
    out = std::strtod(p, &end);
    if (end == p) return false;
    p = end;
    return true;
}

}  // namespace csv_detail

class CsvLobReader {
public:
    explicit CsvLobReader(const std::string& path) : path_(path) {
        file_.open(path);
        if (!file_.is_open())
            throw std::runtime_error("CsvLobReader: cannot open file: " + path);
        // Consume header line
        std::string header;
        std::getline(file_, header);
    }

    // Returns false on EOF or malformed line.
    bool next(BookSnapshot& out) {
        if (!std::getline(file_, line_)) return false;
        if (line_.empty()) return false;

        const char* p = line_.data();

        // Skip unnamed index column
        if (!csv_detail::skip_comma(p)) return false;

        // local_timestamp
        if (!csv_detail::read_u64(p, out.local_timestamp)) return false;

        // 25 groups: asks[i].price, asks[i].amount, bids[i].price, bids[i].amount
        for (int i = 0; i < 25; ++i) {
            if (!csv_detail::skip_comma(p)) return false;
            if (!csv_detail::read_f64(p, out.ask_price[i]))  return false;
            if (!csv_detail::skip_comma(p)) return false;
            if (!csv_detail::read_f64(p, out.ask_amount[i])) return false;
            if (!csv_detail::skip_comma(p)) return false;
            if (!csv_detail::read_f64(p, out.bid_price[i]))  return false;
            if (!csv_detail::skip_comma(p)) return false;
            if (!csv_detail::read_f64(p, out.bid_amount[i])) return false;
        }

        return true;
    }

private:
    std::string   path_;
    std::ifstream file_;
    std::string   line_;  // reused buffer
};

class CsvTradeReader {
public:
    explicit CsvTradeReader(const std::string& path) : path_(path) {
        file_.open(path);
        if (!file_.is_open())
            throw std::runtime_error("CsvTradeReader: cannot open file: " + path);
        // Consume header line
        std::string header;
        std::getline(file_, header);
    }

    // Returns false on EOF or malformed line.
    bool next(Trade& out) {
        if (!std::getline(file_, line_)) return false;
        if (line_.empty()) return false;

        const char* p = line_.data();

        // Skip unnamed index column
        if (!csv_detail::skip_comma(p)) return false;

        // local_timestamp
        if (!csv_detail::read_u64(p, out.local_timestamp)) return false;

        // side: literal "buy" or "sell"
        if (!csv_detail::skip_comma(p)) return false;
        if (std::strncmp(p, "buy", 3) == 0 && (p[3] == ',' || p[3] == '\0')) {
            out.side = 'B';
            p += 3;
        } else if (std::strncmp(p, "sell", 4) == 0 && (p[4] == ',' || p[4] == '\0')) {
            out.side = 'S';
            p += 4;
        } else {
            return false;
        }

        // price
        if (!csv_detail::skip_comma(p)) return false;
        if (!csv_detail::read_f64(p, out.price)) return false;

        // amount
        if (!csv_detail::skip_comma(p)) return false;
        if (!csv_detail::read_f64(p, out.amount)) return false;

        return true;
    }

private:
    std::string   path_;
    std::ifstream file_;
    std::string   line_;
};
