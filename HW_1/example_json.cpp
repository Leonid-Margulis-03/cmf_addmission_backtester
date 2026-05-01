#include <iostream>
#include <nlohmann/json.hpp>

// For convenience
using json = nlohmann::json;

int main() {
    // Create a JSON object
    json j = {
        {"status", "success"},
        {"library", "nlohmann/json"},
        {"is_awesome", true}
    };

    // Print it out with an indent of 4 spaces
    std::cout << j.dump(4) << std::endl;

    return 0;
}