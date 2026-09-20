#pragma once

#include "event_dispatcher/delivery.hpp"

namespace event_dispatcher::detail {

// Type-erased control interface used by the non-template subscription token.
// Concrete subscriber_state<Event> objects implement the lifetime protocol.
class subscription_control {
  public:
    virtual ~subscription_control() = default;
    virtual void unsubscribe() noexcept = 0;
    [[nodiscard]] virtual bool subscribed() const noexcept = 0;
    [[nodiscard]] virtual subscription_metrics metrics() const noexcept = 0;
};

} // namespace event_dispatcher::detail
