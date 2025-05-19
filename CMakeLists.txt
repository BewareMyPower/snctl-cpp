# Copyright 2025 Yunze Xu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.13)

if (NOT CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake")
endif ()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
project(snctl-cpp CXX)
if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif ()

file(READ version.txt VERSION_STR)
string(STRIP ${VERSION_STR} VERSION_STR)

find_path(SIMPLEINI_INCLUDE_DIRS "ConvertUTF.c")
if (SIMPLEINI_INCLUDE_DIRS)
    message(STATUS "SIMPLEINI_INCLUDE_DIRS: ${SIMPLEINI_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "SimpleIni not found")
endif ()
include_directories("${CMAKE_SOURCE_DIR}/include" ${SIMPLEINI_INCLUDE_DIRS})

find_package(RdKafka CONFIG REQUIRED)
find_package(argparse CONFIG REQUIRED)

add_executable(snctl-cpp src/main.cc)
target_compile_definitions(snctl-cpp PUBLIC VERSION_STR="${VERSION_STR}")
target_link_libraries(snctl-cpp PRIVATE argparse::argparse RdKafka::rdkafka)
