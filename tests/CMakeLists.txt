add_executable(event_dispatcher_structure_tests
    structure_tests.cpp
)
target_link_libraries(event_dispatcher_structure_tests
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_test(NAME event_dispatcher.structure COMMAND event_dispatcher_structure_tests)

add_executable(event_dispatcher_bounded_mutex_queue_tests
    bounded_mutex_queue_tests.cpp
)
target_link_libraries(event_dispatcher_bounded_mutex_queue_tests
    PRIVATE
        event_dispatcher::event_dispatcher
        project_options
        project_warnings
)

add_test(NAME event_dispatcher.bounded_mutex_queue
    COMMAND event_dispatcher_bounded_mutex_queue_tests
)
