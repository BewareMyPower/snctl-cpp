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
#include <limits.h>
#include <stdexcept>
#include <string>

inline void list_topics(rd_kafka_t *rk) {
  const struct rd_kafka_metadata *metadata;
  auto err = rd_kafka_metadata(rk, 1 /* all topics */, nullptr, &metadata,
                               INT_MAX /* timeout_ms */);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(
        "Failed to list topics (error code: " + std::to_string(err) + ")");
  }
  auto num_topics = metadata->topic_cnt;
  std::cout << "topic count: " << num_topics << std::endl;
  for (int i = 0; i < num_topics; i++) {
    const auto &topic = metadata->topics[i];
    std::cout << "[" << i << R"(] ")" << topic.topic << R"(" with )"
              << topic.partition_cnt << " partition"
              << (topic.partition_cnt == 1 ? "" : "s") << std::endl;
  }
}
