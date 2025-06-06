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

#include "snctl-cpp/raii_helper.h"
#include "snctl-cpp/rk_event_wrapper.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <map>
#include <ostream>
#include <sstream>
#include <stdexcept>
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

static std::ostream &operator<<(std::ostream &os,
                                const rd_kafka_topic_partition_t *partition) {
  return os << *partition;
}

// Return a map, whose key's format is "<topic>-<partition>" and  value is the
// committed offset of the topic-partition.
inline auto
query_committed_offsets(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                        const std::string &expected_group,
                        rd_kafka_topic_partition_list_t *rk_topic_partitions) {
  auto *group_offset = rd_kafka_ListConsumerGroupOffsets_new(
      expected_group.c_str(), rk_topic_partitions);
  GUARD(group_offset, rd_kafka_ListConsumerGroupOffsets_destroy);

  rd_kafka_ListConsumerGroupOffsets_t *group_offsets[1];
  group_offsets[0] = group_offset;

  rd_kafka_ListConsumerGroupOffsets(rk, &group_offsets[0], 1, nullptr, rkqu);
  auto event = RdKafkaEvent::poll(rkqu);
  const auto *result =
      rd_kafka_event_ListConsumerGroupOffsets_result(event.handle());
  assert(result != nullptr);

  size_t group_count;
  auto *group_result =
      rd_kafka_ListConsumerGroupOffsets_result_groups(result, &group_count);
  if (group_count != 1) {
    std::ostringstream oss;
    oss << "Expected exactly one group, but got " << group_count
        << " in ListConsumerGroupOffsets";
    throw std::runtime_error(oss.str());
  }

  if (expected_group !=
      std::string(rd_kafka_group_result_name(group_result[0]))) {
    std::ostringstream oss;
    oss << "Expected group '" << expected_group << "', but got '"
        << rd_kafka_group_result_name(group_result[0])
        << "' in ListConsumerGroupOffsets";
    throw std::runtime_error(oss.str());
  }

  std::map<std::string, int64_t> offsets;
  const auto *partitions = rd_kafka_group_result_partitions(group_result[0]);
  if (partitions == nullptr) {
    return offsets;
  }
  for (size_t i = 0; i < partitions->cnt; i++) {
    const auto &partition = partitions->elems[i];
    if (partition.offset == RD_KAFKA_OFFSET_INVALID) {
      continue; // Skip invalid offsets
    }
    std::ostringstream oss;
    oss << partition;
    offsets[oss.str()] = partition.offset;
  }
  return offsets;
}

// Return a map whose key is "<topic>-<partition>" and value is the end offset
inline auto
query_end_offsets(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                  rd_kafka_topic_partition_list_t *rk_topic_partitions) {
  for (int i = 0; i < rk_topic_partitions->cnt; i++) {
    rk_topic_partitions->elems[i].offset = RD_KAFKA_OFFSET_SPEC_LATEST;
  }

  rd_kafka_ListOffsets(rk, rk_topic_partitions, nullptr, rkqu);
  auto event = RdKafkaEvent::poll(rkqu);
  const auto *result = rd_kafka_event_ListOffsets_result(event.handle());
  assert(result != nullptr);

  size_t num_partitions;
  const auto *offsets_result =
      rd_kafka_ListOffsets_result_infos(result, &num_partitions);

  std::map<std::string, int64_t> offsets;
  for (size_t i = 0; i < num_partitions; i++) {
    const auto *rk_topic_partition =
        rd_kafka_ListOffsetsResultInfo_topic_partition(offsets_result[i]);
    std::ostringstream oss;
    oss << rk_topic_partition;
    offsets[oss.str()] = rk_topic_partition->offset;
  }
  return offsets;
}

inline void describe_group(rd_kafka_t *rk, rd_kafka_queue_t *rkqu,
                           const std::string &group, bool show_lag) {
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
      std::cout << "| index | client id | consumer id | host | assignments |";
      if (show_lag) {
        std::cout << " lag | end offset |";
      }
      std::cout << std::endl;
    } else {
      std::cout << "No members" << std::endl;
    }

    std::map<std::string, int> topic_to_partition_cnt;
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
        const auto partition = partitions->elems[j];
        std::cout << partition;
        if (auto it = topic_to_partition_cnt.find(partition.topic);
            it != topic_to_partition_cnt.end()) {
          if (it->second <= partition.partition) {
            it->second = partition.partition + 1;
          }
        } else {
          topic_to_partition_cnt[partition.topic] = partition.partition + 1;
        }
      }
      std::cout << "] |" << std::endl;
    }

    if (show_lag) {
      auto *rk_topic_partitions =
          rd_kafka_topic_partition_list_new(topic_to_partition_cnt.size());
      GUARD(rk_topic_partitions, rd_kafka_topic_partition_list_destroy);

      for (auto &&[topic, partition_cnt] : topic_to_partition_cnt) {
        for (int i = 0; i < partition_cnt; i++) {
          rd_kafka_topic_partition_list_add(rk_topic_partitions, topic.c_str(),
                                            i);
        }
      }

      const auto committed_offsets =
          query_committed_offsets(rk, rkqu, group_id, rk_topic_partitions);
      const auto end_offsets = query_end_offsets(rk, rkqu, rk_topic_partitions);
      std::cout << "Offsets info for group '" << group_id << "' with "
                << committed_offsets.size()
                << " topic-partitions:" << std::endl;
      std::cout << "| topic-partition | committed offset | end offset | lag |"
                << std::endl;
      for (auto &&[topic_partition, committed_offset] : committed_offsets) {
        auto end_offset_it = end_offsets.find(topic_partition);
        if (end_offset_it == end_offsets.cend()) {
          std::cout << "| " << topic_partition << " | " << committed_offset
                    << " | N/A | N/A |" << std::endl;
        } else {
          auto end_offset = end_offset_it->second;
          std::cout << "| " << topic_partition << " | " << committed_offset
                    << " | " << end_offset << " | "
                    << (end_offset - committed_offset) << " |" << std::endl;
        }
      }
    }
  } catch (const std::runtime_error &e) {
    std::cerr << "Failed to describe group '" << group << "': " << e.what()
              << std::endl;
  }
}
