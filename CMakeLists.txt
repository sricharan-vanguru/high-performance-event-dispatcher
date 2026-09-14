cmake_minimum_required(VERSION 3.24)

project(
    high_performance_event_dispatcher
    VERSION 0.4.0
    DESCRIPTION "A modular, high-performance concurrent event dispatcher"
    LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)

add_library(event_dispatcher
    src/config.cpp
    src/execution/worker_pool.cpp
    src/subscription/subscription.cpp
)
add_library(event_dispatcher::event_dispatcher ALIAS event_dispatcher)

target_compile_features(event_dispatcher PUBLIC cxx_std_20)
target_include_directories(event_dispatcher
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(event_dispatcher PUBLIC project_options project_warnings)

if(EVENT_DISPATCHER_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(EVENT_DISPATCHER_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(EVENT_DISPATCHER_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
