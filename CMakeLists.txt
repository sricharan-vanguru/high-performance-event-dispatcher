cmake_minimum_required(VERSION 3.24)

project(
    high_performance_event_dispatcher
    VERSION 1.1.0
    DESCRIPTION "A modular, high-performance concurrent event dispatcher"
    LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
find_package(Threads REQUIRED)

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
target_link_libraries(event_dispatcher
    PUBLIC Threads::Threads
    PRIVATE
        $<BUILD_INTERFACE:project_options>
        $<BUILD_INTERFACE:project_warnings>
)

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

install(TARGETS event_dispatcher
    EXPORT HighPerformanceEventDispatcherTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES LICENSE README.md CHANGELOG.md
    DESTINATION ${CMAKE_INSTALL_DOCDIR}
)

set(package_directory "${CMAKE_INSTALL_LIBDIR}/cmake/HighPerformanceEventDispatcher")
configure_package_config_file(
    cmake/HighPerformanceEventDispatcherConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/HighPerformanceEventDispatcherConfig.cmake"
    INSTALL_DESTINATION "${package_directory}"
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/HighPerformanceEventDispatcherConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion
)

install(EXPORT HighPerformanceEventDispatcherTargets
    FILE HighPerformanceEventDispatcherTargets.cmake
    NAMESPACE event_dispatcher::
    DESTINATION "${package_directory}"
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/HighPerformanceEventDispatcherConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/HighPerformanceEventDispatcherConfigVersion.cmake"
    DESTINATION "${package_directory}"
)
