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

#include "snctl-cpp/groups/describe_group.h"
#include "snctl-cpp/groups/list_groups.h"
#include "snctl-cpp/subcommand.h"

#include <argparse/argparse.hpp>
#include <librdkafka/rdkafka.h>

class Groups : public SubCommand {
public:
  explicit Groups(argparse::ArgumentParser &parent) : SubCommand("groups") {
    list_command_.add_description("List all consumer groups");
    describe_command_.add_description("Describe a specific consumer group")
        .add_argument("group")
        .help("The group id")
        .required();
    describe_command_.add_argument("--lag")
        .default_value(false)
        .implicit_value(true)
        .help("Show the lag of the group");

    add_child(list_command_);
    add_child(describe_command_);

    attach_parent(parent);
  }

  void run(rd_kafka_t *rk, rd_kafka_queue_t *rkqu) {
    if (is_subcommand_used(list_command_)) {
      list_groups(rk, rkqu);
    } else if (is_subcommand_used(describe_command_)) {
      auto group = describe_command_.get("group");
      auto show_lag = describe_command_.get<bool>("lag");
      describe_group(rk, rkqu, group, show_lag);
    } else {
      fail();
    }
  }

private:
  argparse::ArgumentParser list_command_{"list"};
  argparse::ArgumentParser describe_command_{"describe"};
};
