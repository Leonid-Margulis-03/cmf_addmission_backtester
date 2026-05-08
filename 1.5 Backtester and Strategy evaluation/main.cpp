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

class StrategyAvellanedaStoikov {
private:
    struct MyQuote {
        double price = 0.0;
        double amount = 0.0;
        bool active = false;
    };

    class VolatilityHandler {
    public:
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

            if (window_.size() > N) {
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
        static constexpr size_t N = 2000;
    };
public:
    void BookUpdate(const BookSnapshot& bookSnapshot) {
//        s = (bookSnapshot.bid_price[0] + bookSnapshot.ask_price[0]) / 2;
        s = (bookSnapshot.bid_price[0] * bookSnapshot.ask_amount[0] + bookSnapshot.ask_price[0] * bookSnapshot.bid_amount[0])
                / (bookSnapshot.bid_amount[0] + bookSnapshot.ask_amount[0]);
        t = bookSnapshot.local_timestamp * 1e-6;
        volatilityHandler.Update(s, t);

        if (std::isnan(T) || t >= T) {
            T = t + horizon_seconds;
        }

        double sigma = volatilityHandler.GetSigma();
        if (sigma > 0.0) {
            double r = ReservationPrice();
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
            std::cout << "n=" << book_update_count_
                      << " s=" << s
                      << " sigma=" << sigma
                      << " T-t=" << (T - t)
                      << " r=" << (sigma > 0 ? ReservationPrice() : 0.0)
                      << " spread=" << (sigma > 0 ? OptimalSpread() : 0.0)
                      << " q=" << q
                      << " x=" << x
                      << "\n";
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

    static constexpr double horizon_seconds = 3600.0;
    double risk_aversion = 100;
    VolatilityHandler volatilityHandler;
    double k = 1e6; // TODO
    uint64_t book_update_count_ = 0;
    uint64_t buys_  = 0;
    uint64_t sells_ = 0;
    double   turnover_ = 0.0;
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
    StrategyAvellanedaStoikov strategyAvellanedaStoikov;
    const string& lob_path = "MD/lob.csv";
    const string& trades_path = "MD/trades.csv";

    replay(lob_path, trades_path, strategyAvellanedaStoikov);
    strategyAvellanedaStoikov.Summary();
    return 0;
}