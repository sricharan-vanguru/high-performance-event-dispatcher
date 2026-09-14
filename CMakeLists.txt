cmake_minimum_required(VERSION 3.24)

project(
    high_performance_event_dispatcher
    VERSION 1.2.0
    DESCRIPTION "A modular, high-performance concurrent event dispatcher"
    LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(CMakePackageConfigHelpers)
include(CheckCXXSourceCompiles)
include(CMakePushCheckState)
include(GNUInstallDirs)
find_package(Threads REQUIRED)

# Some Clang/Linux toolchains lower std::atomic::is_lock_free() to a symbol in
# libatomic, while GCC can provide the same operation without an extra library.
# Detect the actual link requirement instead of keying behavior to a compiler
# brand. PUBLIC propagation is required because the call appears in templates
# instantiated by consumers of the installed headers.
set(event_dispatcher_atomic_probe [[
    #include <atomic>
    #include <cstddef>
    int main() {
        std::atomic<std::size_t> value{0};
        return value.is_lock_free() ? 0 : 1;
    }
]])
cmake_push_check_state(RESET)
check_cxx_source_compiles("${event_dispatcher_atomic_probe}"
    EVENT_DISPATCHER_ATOMICS_LINK_WITHOUT_LIBRARY
)
set(event_dispatcher_atomic_runtime_library)
if(NOT EVENT_DISPATCHER_ATOMICS_LINK_WITHOUT_LIBRARY)
    set(CMAKE_REQUIRED_LIBRARIES atomic)
    check_cxx_source_compiles("${event_dispatcher_atomic_probe}"
        EVENT_DISPATCHER_ATOMICS_LINK_WITH_LIBATOMIC
    )
    if(NOT EVENT_DISPATCHER_ATOMICS_LINK_WITH_LIBATOMIC)
        message(FATAL_ERROR "std::atomic runtime support is unavailable")
    endif()
    set(event_dispatcher_atomic_runtime_library atomic)
endif()
cmake_pop_check_state()

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
    PUBLIC
        Threads::Threads
        ${event_dispatcher_atomic_runtime_library}
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
