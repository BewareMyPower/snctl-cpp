#!/bin/ash
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

set -e

cd /app
export VCPKG_FORCE_SYSTEM_BINARIES=1
./vcpkg/bootstrap-vcpkg.sh
cmake -B build-$ARCH
cmake --build build-$ARCH
cp ./build-$ARCH/snctl-cpp .
