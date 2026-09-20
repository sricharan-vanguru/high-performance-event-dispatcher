#include "event_dispatcher/subscription/subscription.hpp"

#include "event_dispatcher/detail/subscription_control.hpp"

#include <utility>

namespace event_dispatcher {

subscription::subscription() noexcept = default;

subscription::subscription(std::shared_ptr<detail::subscription_control> control) noexcept
    : control_(std::move(control)) {}

subscription::subscription(subscription&&) noexcept = default;

subscription& subscription::operator=(subscription&& other) noexcept {
    if (this != &other) {
        // Releasing shared ownership alone would leave the registry's callback
        // active. Retire the old registration before taking ownership of the
        // incoming one.
        reset();
        control_ = std::move(other.control_);
    }
    return *this;
}

subscription::~subscription() { reset(); }

void subscription::reset() noexcept {
    if (control_) {
        control_->unsubscribe();
        control_.reset();
    }
}

bool subscription::subscribed() const noexcept { return control_ && control_->subscribed(); }

subscription_metrics subscription::metrics() const noexcept {
    return control_ ? control_->metrics() : subscription_metrics{};
}

subscription::operator bool() const noexcept { return subscribed(); }

} // namespace event_dispatcher
