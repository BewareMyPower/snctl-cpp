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
#include "common.h"
#include "create_topic.h"
#include "describe_topic.h"
#include "list_topics.h"
#include <argparse/argparse.hpp>
#include <array>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

int main(int argc, char *argv[]) {
  const auto version = "0.1.0";
  argparse::ArgumentParser program("snctl-cpp", version);
  program.add_argument("--config")
      .default_value("sncloud.ini")
      .help("Path to the config file");
  program.add_argument("--client-id").help("client id");

  argparse::ArgumentParser describe_command("describe");
  describe_command.add_description("Describe a topic");
  describe_command.add_argument("topic").help("Topic to describe").required();
  program.add_subparser(describe_command);

  argparse::ArgumentParser list_command("list");
  list_command.add_description("List topics");
  program.add_subparser(list_command);

  argparse::ArgumentParser create_command("create");
  create_command.add_description("Create a topic");
  create_command.add_argument("topic").help("Topic to create").required();
  create_command.add_argument("-p")
      .help("Number of partitions")
      .scan<'i', int>()
      .default_value(1);
  program.add_subparser(create_command);

  program.parse_args(argc, argv);

  auto rk_conf = rd_kafka_conf_new();

  std::array<char, 512> errstr;
  auto fail = [&errstr](const std::string &action) {
    throw std::runtime_error("Failed to " + action + ": " + errstr.data());
  };

  auto rk_conf_map = load_rdkafka_configs(program);
  for (auto &&[key, value] : rk_conf_map) {
    if (rd_kafka_conf_set(rk_conf, key.c_str(), value.c_str(), errstr.data(),
                          errstr.size()) != RD_KAFKA_CONF_OK) {
      fail("set " + key + " => " + value);
    }
  }

  auto rk =
      rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr.data(), errstr.size());
  if (!rk) {
    fail("create producer");
  }
  std::unique_ptr<std::remove_reference_t<decltype(*rk)>,
                  decltype(&rd_kafka_destroy)>
      rk_guard{rk, &rd_kafka_destroy};

  auto rkqu = rd_kafka_queue_new(rk);
  std::unique_ptr<std::remove_reference_t<decltype(*rkqu)>,
                  decltype(&rd_kafka_queue_destroy)>
      rkque_guard{rkqu, &rd_kafka_queue_destroy};

  if (program.is_subcommand_used(describe_command)) {
    DescribeTopicCommand command{rk, rkqu, describe_command};
    command.run();
  } else if (program.is_subcommand_used(list_command)) {
    list_topics(rk);
  } else if (program.is_subcommand_used(create_command)) {
    auto topic = create_command.get("topic");
    auto partitions = create_command.get<int>("-p");
    if (partitions < 0) {
      throw std::invalid_argument(
          "Number of partitions must be greater than or equal to 0");
    }
    create_topic(rk, rkqu, topic, partitions);
  } else {
    throw std::runtime_error("Only describe command is supported");
  }

  return 0;
}
