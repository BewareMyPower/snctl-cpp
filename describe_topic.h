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
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <stdexcept>
#include <string>

inline void describe_topic(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                           const std::string &topic) {
  const char *topics[] = {topic.c_str()};
  auto topic_names = rd_kafka_TopicCollection_of_topic_names(topics, 1);
  std::unique_ptr<std::remove_reference_t<decltype(*topic_names)>,
                  decltype(&rd_kafka_TopicCollection_destroy)>
      topic_names_guard{topic_names, &rd_kafka_TopicCollection_destroy};

  rd_kafka_DescribeTopics(rk, topic_names, nullptr, rkqu);

  try {
    auto event = RdKafkaEvent::poll(rk, rkqu);
    auto result = rd_kafka_event_DescribeTopics_result(event.handle());
    size_t result_topics_cnt;
    auto result_topics =
        rd_kafka_DescribeTopics_result_topics(result, &result_topics_cnt);
    for (size_t i = 0; i < result_topics_cnt; i++) {
      auto result_topic = result_topics[i];
      auto topic_name = rd_kafka_TopicDescription_name(result_topic);
      auto error = rd_kafka_TopicDescription_error(result_topic);
      if (rd_kafka_error_code(error)) {
        std::cout << "Topic: " << topic_name
                  << " has error: " << rd_kafka_error_string(error) << "\n";
        continue;
      }

      size_t partition_cnt;
      auto partitions =
          rd_kafka_TopicDescription_partitions(result_topic, &partition_cnt);
      for (size_t i = 0; i < partition_cnt; i++) {
        auto result_partition = partitions[i];
        auto id = rd_kafka_TopicPartitionInfo_partition(result_partition);
        auto leader = rd_kafka_TopicPartitionInfo_leader(result_partition);
        if (leader) {
          std::cout << "Partition[" << i << "] "
                    << R"(leader: {"id": )" << rd_kafka_Node_id(leader)
                    << R"(, url: ")" << rd_kafka_Node_host(leader) << ':'
                    << rd_kafka_Node_port(leader) << R"("})" << std::endl;
        } else {
          std::cout << "  has no leader" << std::endl;
        }
      }
    }
  } catch (const std::runtime_error &e) {
    std::cerr << "DescribeTopics failed for " << topic << ": " << e.what()
              << std::endl;
  }
}
