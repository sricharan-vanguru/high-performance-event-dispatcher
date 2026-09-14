#pragma once

#include "event_dispatcher/config.hpp"
#include "event_dispatcher/queue/bounded_mutex_queue.hpp"
#include "event_dispatcher/queue/concurrent_queue.hpp"
#include "event_dispatcher/queue/pop_result.hpp"
#include "event_dispatcher/queue/queue_status.hpp"
#include "event_dispatcher/registry/snapshot_registry.hpp"
#include "event_dispatcher/subscription/subscriber_state.hpp"
#include "event_dispatcher/subscription/subscription.hpp"

// This umbrella header is intentionally small. Concrete dispatcher, queue,
// registry, and reclamation policies will be introduced in separate modules.
