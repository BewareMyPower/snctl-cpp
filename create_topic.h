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

#include "rk_event_wrapper.h"
#include <array>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

inline void create_topic(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                         const std::string &topic, int num_partitions) {
  std::array<char, 512> errstr;
  auto rk_topic = rd_kafka_NewTopic_new(topic.c_str(), num_partitions, 1,
                                        errstr.data(), errstr.size());
  if (rk_topic == nullptr) {
    throw std::runtime_error("Failed to create topic: " +
                             std::string(errstr.data()));
  }
  std::unique_ptr<std::remove_reference_t<decltype(*rk_topic)>,
                  decltype(&rd_kafka_NewTopic_destroy)>
      rk_topic_guard{rk_topic, &rd_kafka_NewTopic_destroy};

  rd_kafka_CreateTopics(rk, &rk_topic, 1, nullptr, rkqu);

  try {
    RdKafkaEvent::poll(rk, rkqu);
    std::cout << R"(Created topic ")" << topic << R"(" with )" << num_partitions
              << " partition" << (num_partitions == 1 ? "" : "s") << std::endl;
  } catch (const std::runtime_error &e) {
    std::cerr << "CreateTopics failed for " << topic << ": " << e.what();
    return;
  }
}
