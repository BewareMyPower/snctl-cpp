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

#include <atomic>
#include <csignal>

class StopSignalGuard final {
public:
  StopSignalGuard() noexcept
      : previous_sigint_(std::signal(SIGINT, handle_signal)),
        previous_sigterm_(std::signal(SIGTERM, handle_signal)) {
    stop_requested().store(false);
  }

  ~StopSignalGuard() {
    std::signal(SIGINT, previous_sigint_);
    std::signal(SIGTERM, previous_sigterm_);
    stop_requested().store(false);
  }

  static bool is_stop_requested() noexcept { return stop_requested().load(); }

  static void request_stop() noexcept { stop_requested().store(true); }

private:
  using SignalHandler = void (*)(int);

  static void handle_signal(int) noexcept { request_stop(); }

  static std::atomic<bool> &stop_requested() noexcept {
    static std::atomic<bool> stop = false;
    return stop;
  }

  SignalHandler previous_sigint_;
  SignalHandler previous_sigterm_;
};
