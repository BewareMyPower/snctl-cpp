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

#include "snctl-cpp/rk_event_wrapper.h"
#include <cassert>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <stdexcept>

inline void list_groups(rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
  rd_kafka_ListConsumerGroups(rk, nullptr, rkqu);

  try {
    auto event = RdKafkaEvent::poll(rkqu);
    const auto *result =
        rd_kafka_event_ListConsumerGroups_result(event.handle());
    assert(result != nullptr);

    size_t count;
    const auto *errors =
        rd_kafka_ListConsumerGroups_result_errors(result, &count);
    if (errors != nullptr) {
      for (size_t i = 0; i < count; ++i) {
        const auto *error = errors[i];
        std::cerr << i << " error: " << rd_kafka_error_string(error)
                  << std::endl;
      }
      return;
    }

    const auto *groups =
        rd_kafka_ListConsumerGroups_result_valid(result, &count);
    assert(groups != nullptr);
    std::cout << "There are " << count << " group" << (count == 1 ? "" : "s")
              << std::endl;
    for (size_t i = 0; i < count; i++) {
      const auto *group = groups[i];
      std::cout << "[" << i << "] "
                << rd_kafka_ConsumerGroupListing_group_id(group) << " "
                << rd_kafka_consumer_group_state_name(
                       rd_kafka_ConsumerGroupListing_state(group))
                << std::endl;
    }

  } catch (const std::runtime_error &e) {
    std::cerr << "Failed to list consumer groups: " << e.what() << std::endl;
  }
}
