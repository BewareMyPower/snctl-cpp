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

#include <librdkafka/rdkafka.h>
#include <stdexcept>

class RdKafkaEvent final {
public:
  static RdKafkaEvent poll(rd_kafka_queue_t *rkqu) {
    auto *event = rd_kafka_queue_poll(rkqu, -1 /* infinite timeout */);
    if (rd_kafka_event_error(event) != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_event_error_string(event));
    }
    return RdKafkaEvent(event);
  }

  RdKafkaEvent(const RdKafkaEvent &) = delete;
  RdKafkaEvent(RdKafkaEvent &&rhs) noexcept : event_(rhs.event_) {
    rhs.event_ = nullptr;
  }

  ~RdKafkaEvent() {
    if (event_ != nullptr) {
      rd_kafka_event_destroy(event_);
    }
  }

  // Get the underlying C handle for rdkafka's C APIs to use
  auto handle() const noexcept { return event_; }

private:
  rd_kafka_event_t *event_;

  explicit RdKafkaEvent(rd_kafka_event_t *event) : event_(event) {}
};
