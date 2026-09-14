#pragma once

#include <memory>

namespace event_dispatcher {

namespace detail {

class subscription_control {
public:
    virtual ~subscription_control() = default;
    virtual void unsubscribe() noexcept = 0;
    [[nodiscard]] virtual bool subscribed() const noexcept = 0;
};

} // namespace detail

// Move-only RAII ownership of one registration. Resetting or destroying the
// handle initiates safe unsubscription through the dispatcher-owned control.
class subscription final {
public:
    subscription() noexcept = default;
    explicit subscription(std::shared_ptr<detail::subscription_control> control) noexcept
        : control_(std::move(control)) {}

    subscription(const subscription&) = delete;
    subscription& operator=(const subscription&) = delete;
    subscription(subscription&&) noexcept = default;
    subscription& operator=(subscription&&) noexcept = default;

    ~subscription() { reset(); }

    void reset() noexcept {
        if (control_) {
            control_->unsubscribe();
            control_.reset();
        }
    }

    [[nodiscard]] bool subscribed() const noexcept {
        return control_ && control_->subscribed();
    }

    explicit operator bool() const noexcept { return subscribed(); }

private:
    std::shared_ptr<detail::subscription_control> control_;
};

} // namespace event_dispatcher
