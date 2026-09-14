cmake_minimum_required(VERSION 3.24)
project(event_dispatcher_package_consumer LANGUAGES CXX)

find_package(HighPerformanceEventDispatcher 1.0 REQUIRED)

add_executable(event_dispatcher_package_consumer main.cpp)
target_link_libraries(event_dispatcher_package_consumer
    PRIVATE event_dispatcher::event_dispatcher
)
