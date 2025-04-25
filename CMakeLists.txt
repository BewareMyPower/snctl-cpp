cmake_minimum_required(VERSION 3.13)

if (NOT CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake")
endif ()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
project(snctl-cpp CXX)
if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif ()

find_path(SIMPLEINI_INCLUDE_DIRS "ConvertUTF.c")
if (SIMPLEINI_INCLUDE_DIRS)
    message(STATUS "SIMPLEINI_INCLUDE_DIRS: ${SIMPLEINI_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "SimpleIni not found")
endif ()
include_directories(${SIMPLEINI_INCLUDE_DIRS})

find_package(RdKafka CONFIG REQUIRED)
set(DEPS RdKafka::rdkafka)

add_executable(snctl-cpp main.cc)
target_link_libraries(snctl-cpp PRIVATE ${DEPS})
