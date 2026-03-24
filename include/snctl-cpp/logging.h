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

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

namespace logging {

inline std::mutex &mutex() {
  static std::mutex instance;
  return instance;
}

inline std::string
format_timestamp(std::chrono::system_clock::time_point time_point) {
  const auto time = std::chrono::system_clock::to_time_t(time_point);
  const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          time_point.time_since_epoch()) %
                      1000;

  std::tm local_time{};
#if defined(_WIN32)
  localtime_s(&local_time, &time);
#else
  localtime_r(&time, &local_time);
#endif

  std::ostringstream oss;
  oss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S") << '.'
      << std::setfill('0') << std::setw(3) << millis.count();
  return oss.str();
}

inline std::string timestamp_now() {
  return format_timestamp(std::chrono::system_clock::now());
}

inline void write_line(std::ostream &output, std::string_view message) {
  std::lock_guard<std::mutex> lock(mutex());
  output << timestamp_now() << ' ' << message << '\n';
  output.flush();
}

class Line final {
public:
  explicit Line(std::ostream &output) : output_(output) {}

  Line(const Line &) = delete;
  Line &operator=(const Line &) = delete;

  Line(Line &&other) noexcept
      : output_(other.output_), buffer_(std::move(other.buffer_)),
        flushed_(other.flushed_) {
    other.flushed_ = true;
  }

  ~Line() { flush(); }

  template <typename T> Line &operator<<(const T &value) {
    buffer_ << value;
    return *this;
  }

private:
  void flush() {
    if (flushed_) {
      return;
    }
    write_line(output_, buffer_.str());
    flushed_ = true;
  }

  std::ostream &output_;
  std::ostringstream buffer_;
  bool flushed_ = false;
};

inline Line out() { return Line(std::cout); }

inline Line err() { return Line(std::cout); }

} // namespace logging
