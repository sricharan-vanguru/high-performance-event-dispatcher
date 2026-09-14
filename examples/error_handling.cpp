#include "event_dispatcher/dispatcher.hpp"

#include <exception>
#include <iostream>
#include <stdexcept>

int main() {
    event_dispatcher::dispatcher<int> dispatcher{{}, [](std::exception_ptr error) {
                                                     try {
                                                         std::rethrow_exception(error);
                                                     } catch (const std::exception& exception) {
                                                         std::cerr << "callback error: "
                                                                   << exception.what() << '\n';
                                                     }
                                                 }};

    auto failing = dispatcher.subscribe(
        [](const int&) { throw std::runtime_error{"subscriber could not process event"}; });
    auto healthy = dispatcher.subscribe(
        [](const int& event) { std::cout << "healthy subscriber received " << event << '\n'; });

    static_cast<void>(dispatcher.publish(7));
    dispatcher.shutdown();
    static_cast<void>(failing);
    static_cast<void>(healthy);
}
