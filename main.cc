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
#include "SimpleIni.h"
#include "create_topic.h"
#include "delete_topic.h"
#include "describe_topic.h"
#include "list_topics.h"
#include <argparse/argparse.hpp>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

int main(int argc, char *argv[]) {
  std::vector<std::string> default_config_paths{
      std::filesystem::current_path() / "sncloud.ini",
      std::filesystem::path(std::getenv("HOME")) / ".snctl-cpp" /
          "sncloud.ini"};

  argparse::ArgumentParser program("snctl-cpp",
#ifdef VERSION_STR
                                   VERSION_STR
#else
                                   "unknown"
#endif
  );
  program.add_argument("--config")
      .default_value(default_config_paths)
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

  argparse::ArgumentParser delete_command("delete");
  delete_command.add_description("Delete a topic");
  delete_command.add_argument("topic").help("Topic to delete").required();
  program.add_subparser(delete_command);

  program.parse_args(argc, argv);

  auto rk_conf = rd_kafka_conf_new();

  std::array<char, 512> errstr;
  auto fail = [&errstr](const std::string &action) {
    throw std::runtime_error("Failed to " + action + ": " + errstr.data());
  };

  const auto config_files = program.get<std::vector<std::string>>("--config");
  CSimpleIni ini;
  bool loaded = false;
  for (auto &&config_file : config_files) {
    if (std::filesystem::exists(config_file)) {
      if (auto rc = ini.LoadFile(config_file.c_str()); rc < 0) {
        throw std::runtime_error("Error loading config file " + config_file);
      }
      loaded = true;
      break;
    }
  }
  if (!loaded) {
    throw std::runtime_error("No config file found");
  }

  auto get_value = [&ini](const auto &key, bool required) {
    auto value = ini.GetValue("kafka", key, "");
    if (strlen(value) == 0 && required) {
      throw std::runtime_error("Error: " + std::string(key) +
                               " not found in kafka section");
    }
    return std::string(value);
  };

  std::unordered_map<std::string, std::string> rk_conf_map{
      {"bootstrap.servers", get_value("bootstrap.servers", true)}};
  if (auto token = get_value("token", false); !token.empty()) {
    rk_conf_map["sasl.mechanism"] = "PLAIN";
    rk_conf_map["security.protocol"] = "SASL_SSL";
    rk_conf_map["sasl.username"] = "user";
    rk_conf_map["sasl.password"] = "token:" + token;
  }
  if (auto client_id = program.present("--client-id")) {
    rk_conf_map["client.id"] = client_id.value();
  }
  for (auto &&[key, value] : rk_conf_map) {
    if (rd_kafka_conf_set(rk_conf, key.c_str(), value.c_str(), errstr.data(),
                          errstr.size()) != RD_KAFKA_CONF_OK) {
      fail("set " + key + " => " + value);
    }
  }

  std::unique_ptr<FILE, decltype(&fclose)> file{nullptr, &fclose};
  if (auto log_enabled = ini.GetValue("log", "enabled", "false");
      std::string(log_enabled) == "false") {
    // Disable logging in rdkafka
    rd_kafka_conf_set_log_cb(
        rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                     const char *buf) {});
  } else {
    if (auto log_file = ini.GetValue("log", "path", "");
        strlen(log_file) == 0) {
      rd_kafka_conf_set_opaque(rk_conf, stdout);
    } else {
      file.reset(fopen(log_file, "a"));
      rd_kafka_conf_set_opaque(rk_conf, file.get());
      rd_kafka_conf_set_log_cb(
          rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                       const char *buf) {
            auto file = static_cast<FILE *>(rd_kafka_opaque(rk));
            fprintf(file, "[%d] %s: %s\n", level, fac, buf);
            fflush(file);
          });
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
    describe_topic(rk, rkqu, describe_command.get("topic"));
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
  } else if (program.is_subcommand_used(delete_command)) {
    auto topic = delete_command.get("topic");
    delete_topic(rk, rkqu, topic);
  } else {
    throw std::runtime_error("Only describe command is supported");
  }

  return 0;
}
