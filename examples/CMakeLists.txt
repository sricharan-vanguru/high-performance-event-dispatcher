add_executable(event_dispatcher_configuration_example
    configuration.cpp
)
target_link_libraries(event_dispatcher_configuration_example
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_queue_example
    queue_usage.cpp
)
target_link_libraries(event_dispatcher_queue_example
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_registry_example
    registry_usage.cpp
)
target_link_libraries(event_dispatcher_registry_example
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_executable(event_dispatcher_dispatcher_example dispatcher_usage.cpp)
target_link_libraries(event_dispatcher_dispatcher_example
    PRIVATE event_dispatcher::event_dispatcher project_options project_warnings
)
