#pragma once

#include <memory>

namespace event_dispatcher {

namespace detail {
class subscription_control;
} // namespace detail

// Move-only RAII ownership of one registration. Resetting or destroying the
// handle initiates safe unsubscription through the dispatcher-owned control.
class subscription final {
  public:
    subscription() noexcept;
    explicit subscription(std::shared_ptr<detail::subscription_control> control) noexcept;

    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;
    subscription(subscription&&) noexcept;
    subscription& operator=(subscription&& other) noexcept;

    ~subscription();

    void reset() noexcept;
    [[nodiscard]] bool subscribed() const noexcept;
    explicit operator bool() const noexcept;

  private:
    std::shared_ptr<detail::subscription_control> control_;
};

} // namespace event_dispatcher
