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

#include <iostream>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

inline void delete_topic(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                         const std::string &topic) {
  auto rk_topic = rd_kafka_DeleteTopic_new(topic.c_str());
  std::unique_ptr<std::remove_reference_t<decltype(*rk_topic)>,
                  decltype(&rd_kafka_DeleteTopic_destroy)>
      rk_topic_guard{rk_topic, &rd_kafka_DeleteTopic_destroy};

  rd_kafka_DeleteTopics(rk, &rk_topic, 1, nullptr, rkqu);

  if (auto event = rd_kafka_queue_poll(rkqu, -1 /* infinite timeout */);
      rd_kafka_event_error(event)) {
    std::cerr << "DeleteTopics failed for " << topic << ": "
              << rd_kafka_event_error_string(event);
  } else {
    auto result = rd_kafka_event_DeleteTopics_result(event);
    size_t cntp;
    auto topics = rd_kafka_DeleteTopics_result_topics(result, &cntp);
    if (cntp != 1) {
      throw std::runtime_error("DeleteTopics response has " +
                               std::to_string(cntp) + " topics");
    }
    if (auto error = rd_kafka_topic_result_error_string(topics[0]);
        error == nullptr) {
      std::cout << R"(Deleted topic ")" << topic << R"(")" << std::endl;
    } else {
      std::cerr << R"(Failed to delete topic ")" << topic << R"(": )" << error
                << std::endl;
    }
  }
}
