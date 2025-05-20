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

#include "snctl-cpp/subcommand.h"
#include "snctl-cpp/topics/create_topic.h"
#include "snctl-cpp/topics/delete_topic.h"
#include "snctl-cpp/topics/describe_topic.h"
#include "snctl-cpp/topics/list_topics.h"
#include <argparse/argparse.hpp>
#include <librdkafka/rdkafka.h>
#include <stdexcept>

class Topics : public SubCommand {
public:
  explicit Topics(argparse::ArgumentParser &parent) : SubCommand("topics") {
    create_command_.add_description("Create a topic");
    create_command_.add_argument("topic").help("Topic to create").required();
    create_command_.add_argument("-p")
        .help("Number of partitions")
        .scan<'i', int>()
        .default_value(1);

    delete_command_.add_description("Delete a topic");
    delete_command_.add_argument("topic").help("Topic to delete").required();

    list_command_.add_description("List topics");

    describe_command_.add_description("Describe a topic");
    describe_command_.add_argument("topic")
        .help("Topic to describe")
        .required();

    add_child(create_command_);
    add_child(delete_command_);
    add_child(list_command_);
    add_child(describe_command_);

    attach_parent(parent);
  }

  void run(rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
    if (is_subcommand_used(create_command_)) {
      auto topic = create_command_.get("topic");
      auto partitions = create_command_.get<int>("-p");
      if (partitions < 0) {
        throw std::invalid_argument(
            "Number of partitions must be greater than or equal to 0");
      }
      create_topic(rk, rkqu, topic, partitions);
    } else if (is_subcommand_used(delete_command_)) {
      auto topic = delete_command_.get("topic");
      delete_topic(rk, rkqu, topic);
    } else if (is_subcommand_used(list_command_)) {
      list_topics(rk);
    } else if (is_subcommand_used(describe_command_)) {
      auto topic = describe_command_.get("topic");
      describe_topic(rk, rkqu, topic);
    } else {
      fail();
    }
  }

private:
  argparse::ArgumentParser create_command_{"create"};
  argparse::ArgumentParser delete_command_{"delete"};
  argparse::ArgumentParser list_command_{"list"};
  argparse::ArgumentParser describe_command_{"describe"};
};
