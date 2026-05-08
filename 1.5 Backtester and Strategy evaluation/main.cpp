#include <climits>
#include <cstdint>
#include <deque>
#include <cmath>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "csv_readers.hpp"

using namespace std;

struct Config {
    double gamma = 0.1;
    double k = 1e6;
    double horizon_seconds = 3600.0;
    size_t vol_window = 2000;
    std::string lob_path = "MD/lob.csv";
    std::string trades_path = "MD/trades.csv";
    std::string pnl_log_path = "pnl_log.csv";
    std::string results_csv = "results.csv";
    std::string config_id = "default";
    bool use_inventory_skew = true;
};

inline Config load_config(const std::string& path) {
    Config c;
    std::ifstream f(path);
    if (!f.is_open()) throw std::runtime_error("cannot open config: " + path);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        if      (key == "gamma")              c.gamma = std::stod(val);
        else if (key == "k")                  c.k = std::stod(val);
        else if (key == "horizon_seconds")    c.horizon_seconds = std::stod(val);
        else if (key == "vol_window")         c.vol_window = std::stoul(val);
        else if (key == "lob_path")           c.lob_path = val;
        else if (key == "trades_path")        c.trades_path = val;
        else if (key == "pnl_log_path")       c.pnl_log_path = val;
        else if (key == "results_csv")        c.results_csv = val;
        else if (key == "config_id")          c.config_id = val;
        else if (key == "use_inventory_skew") c.use_inventory_skew = (val == "true" || val == "1");
    }
    return c;
}


class StrategyAvellanedaStoikov {
private:
    struct MyQuote {
        double price = 0.0;
        double amount = 0.0;
        bool active = false;
    };

    class VolatilityHandler {
    public:
        explicit VolatilityHandler(size_t N) : N_(N) {}

        void Update(double s, double t) {
            if (last_s == -1) {
                last_s = s;
                last_t = t;
                return;
            }

            double diff_s = s - last_s;
            double diff_t = t - last_t;
            last_s = s;
            last_t = t;
            window_.emplace_back(diff_s * diff_s, diff_t);
            sum_ds2_ += diff_s * diff_s;
            sum_dt_ += diff_t;

            if (window_.size() > N_) {
                sum_ds2_ -= window_.front().first;
                sum_dt_ -= window_.front().second;

                window_.pop_front();
            }
        }

        double GetSigma() const {
            if (sum_dt_ > 0) {
                return sqrt(sum_ds2_ / sum_dt_);
            }

            return 0;
        }

    private:
        std::deque<std::pair<double, double>> window_; //ds^2, dt
        double sum_ds2_ = 0;
        double sum_dt_ = 0;
        double last_s = -1;
        double last_t = 0;
        size_t N_;
    };

public:
    explicit StrategyAvellanedaStoikov(const Config& cfg)
        : risk_aversion(cfg.gamma)
        , k(cfg.k)
        , horizon_seconds_(cfg.horizon_seconds)
        , volatilityHandler(cfg.vol_window)
        , use_inventory_skew_(cfg.use_inventory_skew)
        , config_id_(cfg.config_id)
        , results_csv_path_(cfg.results_csv)
    {
        pnl_log_.open(cfg.pnl_log_path);
        pnl_log_ << "t,mid,q,x,pnl,sigma,spread\n";
    }

    void BookUpdate(const BookSnapshot& bookSnapshot) {
//        s = (bookSnapshot.bid_price[0] + bookSnapshot.ask_price[0]) / 2;
        s = (bookSnapshot.bid_price[0] * bookSnapshot.ask_amount[0] + bookSnapshot.ask_price[0] * bookSnapshot.bid_amount[0])
                / (bookSnapshot.bid_amount[0] + bookSnapshot.ask_amount[0]);
        t = bookSnapshot.local_timestamp * 1e-6;
        volatilityHandler.Update(s, t);

        if (std::isnan(T) || t >= T) {
            T = t + horizon_seconds_;
        }

        double sigma = volatilityHandler.GetSigma();
        if (sigma > 0.0) {
            double r = use_inventory_skew_ ? ReservationPrice() : s;
            double delta = OptimalSpread();

            my_ask.price = r + delta / 2.0;
            my_ask.amount = 1.0;
            my_ask.active = true;

            my_bid.price  = r - delta / 2.0;
            my_bid.amount = 1.0;
            my_bid.active = true;
        }

        ++book_update_count_;
        if (book_update_count_ % 10000 == 0) {
            double sigma2 = volatilityHandler.GetSigma();
            std::cout << "n=" << book_update_count_
                      << " s=" << s
                      << " sigma=" << sigma2
                      << " T-t=" << (T - t)
                      << " r=" << (sigma2 > 0 ? (use_inventory_skew_ ? ReservationPrice() : s) : 0.0)
                      << " spread=" << (sigma2 > 0 ? OptimalSpread() : 0.0)
                      << " q=" << q
                      << " x=" << x
                      << "\n";
        }
        if (book_update_count_ % pnl_log_every_ == 0) {
            double pnl = x + q * s;
            double spread_now = (sigma > 0.0) ? OptimalSpread() : 0.0;
            pnl_log_ << t << "," << s << "," << q << "," << x << ","
                     << pnl << "," << sigma << "," << spread_now << "\n";
        }
    }

    void TradeUpdate(const Trade& trade) {
        if (std::isnan(s)) return;

        if (trade.side == 'B' && my_ask.active && trade.price >= my_ask.price) {
            double size = my_ask.amount;
            q -= size;
            x += size * my_ask.price;
            turnover_ += size * my_ask.price;
            ++sells_;
            my_ask.active = false;
        } else if (trade.side == 'S' && my_bid.active && trade.price <= my_bid.price) {
            double size = my_bid.amount;
            q += size;
            x -= size * my_bid.price;
            turnover_ += size * my_bid.price;
            ++buys_;
            my_bid.active = false;
        }
    }

    void Summary() const {
        double pnl = x + q * s;
        std::cout << "=== SUMMARY ===\n"
                  << "book_updates=" << book_update_count_ << "\n"
                  << "total_fills="  << buys_ + sells_       << "\n"
                  << "buys="         << buys_              << "\n"
                  << "sells="        << sells_             << "\n"
                  << "final_q="      << q                  << "\n"
                  << "final_x="      << x                  << "\n"
                  << "final_mid="    << s                  << "\n"
                  << "turnover="     << turnover_          << "\n"
                  << "PNL = " << pnl             << "\n";
    }

    void Finalize() {
        double pnl = x + q * s;
        // Check if results file exists to decide whether to write header
        bool write_header = false;
        {
            std::ifstream check(results_csv_path_);
            write_header = !check.is_open();
        }
        std::ofstream out(results_csv_path_, std::ios::app);
        if (!out.is_open()) {
            std::cerr << "warning: cannot open results file: " << results_csv_path_ << "\n";
            return;
        }
        if (write_header) {
            out << "config_id,gamma,use_inventory_skew,book_updates,total_fills,"
                   "buys,sells,final_q,final_x,final_mid,turnover,pnl\n";
        }
        out << config_id_ << ","
            << risk_aversion << ","
            << (use_inventory_skew_ ? "true" : "false") << ","
            << book_update_count_ << ","
            << (buys_ + sells_) << ","
            << buys_ << ","
            << sells_ << ","
            << q << ","
            << x << ","
            << s << ","
            << turnover_ << ","
            << pnl << "\n";
    }

private:
    double ReservationPrice() {
        double sigma = volatilityHandler.GetSigma();
        return s - (q * risk_aversion * sigma * sigma * (T - t));
    }

    double OptimalSpread() {
        double sigma = volatilityHandler.GetSigma();
        return risk_aversion * sigma * sigma * (T - t) + (2 / risk_aversion) * log(1 + risk_aversion / k);
    }

    MyQuote my_bid;
    MyQuote my_ask;

    double s = std::nan("");
    double q = 0;
    double t = std::nan("");
    double T = std::nan("");
    double x = 0; // cash position

    double horizon_seconds_;
    double risk_aversion = 100;
    VolatilityHandler volatilityHandler;
    double k = 1e6; // TODO
    uint64_t book_update_count_ = 0;
    uint64_t buys_  = 0;
    uint64_t sells_ = 0;
    double   turnover_ = 0.0;

    bool        use_inventory_skew_ = true;
    std::string config_id_;
    std::string results_csv_path_;

    std::ofstream pnl_log_;
    static constexpr uint64_t pnl_log_every_ = 1000;
};


template <class Strat>
void replay(const string& lob_path, const string& trade_path, Strat& strat) {
    CsvLobReader lobReader = CsvLobReader(lob_path);
    CsvTradeReader tradeReader = CsvTradeReader(trade_path);

    BookSnapshot bookSnapshot;
    Trade trade;

    bool moreLobs = lobReader.next(bookSnapshot);
    bool moreTrades = tradeReader.next(trade);

    while (moreLobs && moreTrades) {
        if (trade.local_timestamp <= bookSnapshot.local_timestamp) {
            strat.TradeUpdate(trade);
            moreTrades = tradeReader.next(trade);
        } else {
            strat.BookUpdate(bookSnapshot);
            moreLobs = lobReader.next(bookSnapshot);
        }
    }

    while (moreTrades) {
        strat.TradeUpdate(trade);
        moreTrades = tradeReader.next(trade);
    }

    while (moreLobs) {
        strat.BookUpdate(bookSnapshot);
        moreLobs = lobReader.next(bookSnapshot);
    }
}


int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.cfg>\n";
        return 1;
    }
    Config cfg = load_config(argv[1]);
    StrategyAvellanedaStoikov strategy(cfg);
    replay(cfg.lob_path, cfg.trades_path, strategy);
    strategy.Summary();
    strategy.Finalize();
    return 0;
}
