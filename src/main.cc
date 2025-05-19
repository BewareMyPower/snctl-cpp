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
#include <argparse/argparse.hpp>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "snctl-cpp/configs.h"
#include "snctl-cpp/raii_helper.h"
#include "snctl-cpp/topics.h"

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
  program.add_argument("--get-config")
      .default_value(false)
      .implicit_value(true)
      .help("Get the config file path");
  program.add_argument("--client-id").help("client id");

  Topics topics{program};
  Configs configs{program};
  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << "Failed to parse args: " << err.what() << "\n"
              << program << std::endl;
    return 1;
  }

  auto rk_conf = rd_kafka_conf_new();

  std::array<char, 512> errstr;
  auto fail = [&errstr](const std::string &action) {
    throw std::runtime_error("Failed to " + action + ": " + errstr.data());
  };

  configs.init(program);
  std::unordered_map<std::string, std::string> rk_conf_map{
      {"bootstrap.servers", configs.kafka_configs().bootstrap_servers}};

  if (const auto &token = configs.kafka_configs().token; !token.empty()) {
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
  if (configs.log_configs().enabled) {
    if (const auto &log_file = configs.log_configs().path; log_file.empty()) {
      rd_kafka_conf_set_opaque(rk_conf, stdout);
    } else {
      file.reset(fopen(log_file.c_str(), "a"));
      rd_kafka_conf_set_opaque(rk_conf, file.get());
      rd_kafka_conf_set_log_cb(
          rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                       const char *buf) {
            auto file = static_cast<FILE *>(rd_kafka_opaque(rk));
            fprintf(file, "[%d] %s: %s\n", level, fac, buf);
            fflush(file);
          });
    }
  } else { // Disable logging in rdkafka
    rd_kafka_conf_set_log_cb(
        rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                     const char *buf) {});
  }

  auto rk =
      rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr.data(), errstr.size());
  if (!rk) {
    fail("create producer");
  }
  GUARD(rk, rd_kafka_destroy);

  auto rkqu = rd_kafka_queue_new(rk);
  GUARD(rkqu, rd_kafka_queue_destroy);

  try {
    if (topics.used_by_parent(program)) {
      topics.run(rk, rkqu);
    } else if (configs.used_by_parent(program)) {
      configs.run();
    } else {
      if (program["--get-config"] == true) {
        if (auto config_file = configs.config_file(); !config_file.empty()) {
          std::cout << "config file: " << configs.config_file() << std::endl;
          return 0;
        } else {
          std::cerr << "Unexpected empty config file" << std::endl;
          return 2;
        }
      }
      std::cerr << "Invalid subcommand\n" << program << std::endl;
      return 1;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  return 0;
}
