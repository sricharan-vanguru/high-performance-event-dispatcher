#include "event_dispatcher/registry/snapshot_registry.hpp"

#include <iostream>
#include <string>

int main() {
    event_dispatcher::registry::snapshot_registry<std::string> registry;

    auto subscription = registry.subscribe([](const std::string& event) {
        std::cout << "subscriber received: " << event << '\n';
    });

    // A worker will eventually perform this same sequence: acquire one
    // immutable list, then offer the event to each state in that snapshot.
    const auto old_snapshot = registry.acquire_snapshot();
    for (const auto& subscriber : *old_snapshot) {
        static_cast<void>(subscriber->try_invoke("connected"));
    }

    subscription.reset();

    // old_snapshot still keeps the control block alive, so dereferencing it is
    // safe. Logical deactivation makes try_invoke return false and guarantees
    // that the callback is not entered after reset() has returned.
    for (const auto& subscriber : *old_snapshot) {
        if (!subscriber->try_invoke("must-not-run")) {
            std::cout << "old snapshot safely skipped inactive subscriber\n";
        }
    }
}
