option(EVENT_DISPATCHER_BUILD_TESTS "Build tests" ON)
option(EVENT_DISPATCHER_BUILD_EXAMPLES "Build examples" ON)
option(EVENT_DISPATCHER_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(EVENT_DISPATCHER_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(EVENT_DISPATCHER_ENABLE_THREAD_SANITIZER "Enable ThreadSanitizer" OFF)
option(EVENT_DISPATCHER_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

if(EVENT_DISPATCHER_ENABLE_SANITIZERS AND EVENT_DISPATCHER_ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR
        "AddressSanitizer/UndefinedBehaviorSanitizer and ThreadSanitizer "
        "must use separate build directories"
    )
endif()

add_library(project_options INTERFACE)
add_library(project_warnings INTERFACE)

target_compile_features(project_options INTERFACE cxx_std_20)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(project_warnings INTERFACE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
    )

    if(EVENT_DISPATCHER_WARNINGS_AS_ERRORS)
        target_compile_options(project_warnings INTERFACE -Werror)
    endif()

    if(EVENT_DISPATCHER_ENABLE_SANITIZERS)
        target_compile_options(project_options INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(project_options INTERFACE -fsanitize=address,undefined)
    endif()

    if(EVENT_DISPATCHER_ENABLE_THREAD_SANITIZER)
        target_compile_options(project_options INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(project_options INTERFACE -fsanitize=thread)
    endif()
endif()
