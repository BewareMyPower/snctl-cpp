/**
 * Copyright 2025 Yunze Xu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

// Call the deleter on ptr when it goes out of scope
//   ptr must be a pointer type T*
//   deleter must be a function pointer type void(*)(T*)
#define GUARD(ptr, deleter)                                                    \
  std::unique_ptr<std::remove_reference_t<decltype(*(ptr))>,                   \
                  decltype((deleter))>                                         \
      ptr##_guard{ptr, deleter};
