#!/bin/bash
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
cd `dirname $0`

if [[ ! -f snctl-cpp ]]; then
    echo "No snctl-cpp found in current directory"
    exit 1
fi
if [[ ! -f sncloud.ini ]]; then
    echo "No sncloud.ini found in current directory"
    exit 1
fi

mkdir -p ~/.snctl-cpp
cp -f snctl-cpp ~/.snctl-cpp/snctl-cpp
cp -f sncloud.ini ~/.snctl-cpp/sncloud.ini
