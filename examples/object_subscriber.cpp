#include "event_dispatcher/dispatcher.hpp"

#include <iostream>
#include <memory>
#include <string>

class connection_monitor final {
  public:
    void on_event(const std::string& event) const {
        std::cout << "connection monitor: " << event << '\n';
    }
};

int main() {
    event_dispatcher::dispatcher<std::string> dispatcher;
    auto monitor = std::make_shared<connection_monitor>();

    // Strong capture keeps the application object alive for the subscription.
    // Phase 9 will add an opt-in weak-ownership helper for automatic expiry.
    auto token =
        dispatcher.subscribe([monitor](const std::string& event) { monitor->on_event(event); });

    static_cast<void>(dispatcher.publish("connected"));
    dispatcher.shutdown();
    static_cast<void>(token);
}
