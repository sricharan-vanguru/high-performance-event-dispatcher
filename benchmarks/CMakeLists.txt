add_executable(event_dispatcher_queue_comparison_benchmark
    bounded_mutex_queue_benchmark.cpp
)
target_link_libraries(event_dispatcher_queue_comparison_benchmark
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_snapshot_registry_benchmark
    snapshot_registry_benchmark.cpp
)
target_link_libraries(event_dispatcher_snapshot_registry_benchmark
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_allocation_batch_benchmark allocation_batch_benchmark.cpp)
target_link_libraries(event_dispatcher_allocation_batch_benchmark
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_delivery_policy_benchmark delivery_policy_benchmark.cpp)
target_link_libraries(event_dispatcher_delivery_policy_benchmark
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)
