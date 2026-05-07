#include <climits>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <vector>

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

class Strategy {
public:
    void TestStrategy(int argc, char **argv) {
        
    }

private:

};

int main(int argc, char **argv) {
    return 0;
}