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
#include <cstddef>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <ostream>
#include <string>

static std::ostream &operator<<(std::ostream &os, const rd_kafka_Node_t *node) {
  os << rd_kafka_Node_host(node) << ":" << rd_kafka_Node_port(node);
  if (const auto *rack = rd_kafka_Node_rack(node); rack != nullptr) {
    os << " (rack: " << rack << ")";
  }
  return os;
}

static std::ostream &operator<<(std::ostream &os,
                                const rd_kafka_topic_partition_t &partition) {
  os << partition.topic << "-" << partition.partition;
  return os;
}

inline void describe_group(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                           const std::string &group) {
  const char *groups[1] = {group.c_str()};
  rd_kafka_DescribeConsumerGroups(rk, groups, 1, nullptr, rkqu);

  try {
    auto event = RdKafkaEvent::poll(rkqu);
    const auto *result =
        rd_kafka_event_DescribeConsumerGroups_result(event.handle());
    assert(result != nullptr);

    size_t group_count;
    const auto *groups =
        rd_kafka_DescribeConsumerGroups_result_groups(result, &group_count);
    assert(groups);
    if (group_count != 1) {
      std::cerr << "Expected exactly one group, but got " << group_count
                << std::endl;
      return;
    }

    const auto *group = groups[0];
    const auto *group_id = rd_kafka_ConsumerGroupDescription_group_id(group);
    const auto *error = rd_kafka_ConsumerGroupDescription_error(group);
    if (error != nullptr) {
      std::cerr << "Error describing group '" << group_id
                << "': " << rd_kafka_error_string(error) << std::endl;
      return;
    }

    const auto *assignor =
        rd_kafka_ConsumerGroupDescription_partition_assignor(group);
    const auto state = rd_kafka_ConsumerGroupDescription_state(group);
    const auto *state_name = rd_kafka_consumer_group_state_name(state);
    const auto *node = rd_kafka_ConsumerGroupDescription_coordinator(group);
    const auto type = rd_kafka_ConsumerGroupDescription_type(group);

    std::cout << "Group ID: " << group_id << std::endl;
    std::cout << "Assignor: " << assignor << std::endl;
    std::cout << "State: " << state_name << std::endl;
    std::cout << "Type: " << type << std::endl;
    std::cout << "Coordinator: " << node << std::endl;
    const auto member_count =
        rd_kafka_ConsumerGroupDescription_member_count(group);

    if (member_count > 0) {
      std::cout << "There are " << member_count << " members:" << std::endl;
      std::cout << "| index | client id | consumer id | host | assignments |"
                << std::endl;
    } else {
      std::cout << "No members" << std::endl;
    }
    for (size_t i = 0; i < member_count; i++) {
      const auto *member = rd_kafka_ConsumerGroupDescription_member(group, i);
      assert(member != nullptr);
      const auto *assignment = rd_kafka_MemberDescription_assignment(member);
      assert(assignment != nullptr);
      std::cout << "| " << i << " | "
                << rd_kafka_MemberDescription_client_id(member) << " | "
                << rd_kafka_MemberDescription_consumer_id(member) << " | "
                << rd_kafka_MemberDescription_host(member);
      const auto *partitions = rd_kafka_MemberAssignment_partitions(assignment);
      assert(partitions != nullptr);
      std::cout << " | [";
      for (int j = 0; j < partitions->cnt; j++) {
        if (j > 0) {
          std::cout << ", ";
        }
        std::cout << partitions->elems[j];
      }
      std::cout << "] |" << std::endl;
    }

  } catch (const std::runtime_error &e) {
    std::cerr << "Failed to describe group '" << group << "': " << e.what()
              << std::endl;
  }
}
