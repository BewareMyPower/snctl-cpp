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
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <librdkafka/rdkafka.h>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "snctl-cpp/configs.h"
#include "snctl-cpp/consume.h"
#include "snctl-cpp/groups.h"
#include "snctl-cpp/kafka_client.h"
#include "snctl-cpp/produce.h"
#include "snctl-cpp/topics.h"

int main(int argc, char *argv[]) noexcept(false) {
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
  Groups groups{program};
  ProduceCommand produce{program};
  ConsumeCommand consume{program};
  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << "Failed to parse args: " << err.what() << "\n"
              << program << std::endl;
    return 1;
  }
  configs.init(program);
  std::unordered_map<std::string, std::string> rk_conf_map{
      {"bootstrap.servers", configs.kafka_configs().bootstrap_servers}};

  if (const auto &token = configs.kafka_configs().token; !token.empty()) {
    rk_conf_map["sasl.mechanism"] = "PLAIN";
    rk_conf_map["security.protocol"] = "SASL_SSL";
    rk_conf_map["sasl.username"] = "user";
    rk_conf_map["sasl.password"] = "token:" + token;
  }
  if (const auto &debug = configs.log_configs().debug; !debug.empty()) {
    rk_conf_map["debug"] = debug;
  }
  rk_conf_map["log_level"] = std::to_string(configs.log_configs().log_level);
  if (auto client_id = program.present("--client-id")) {
    rk_conf_map["client.id"] = client_id.value();
  }

  try {
    if (topics.used_by_parent(program)) {
      KafkaClient client(RD_KAFKA_CONSUMER, rk_conf_map, configs.log_configs(),
                         true);
      topics.run(client.rk(), client.queue());
    } else if (configs.used_by_parent(program)) {
      configs.run();
    } else if (groups.used_by_parent(program)) {
      KafkaClient client(RD_KAFKA_CONSUMER, rk_conf_map, configs.log_configs(),
                         true);
      groups.run(client.rk(), client.queue());
    } else if (produce.used_by_parent(program)) {
      produce.run(rk_conf_map, configs.log_configs(),
                  program.present("--client-id"));
    } else if (consume.used_by_parent(program)) {
      consume.run(rk_conf_map, configs.log_configs(),
                  program.present("--client-id"));
    } else {
      if (program["--get-config"] == true) {
        if (const auto &config_file = configs.config_file();
            config_file.empty()) {
          std::cerr << "Unexpected empty config file" << std::endl;
          return 2;
        }
        std::cout << "config file: " << configs.config_file() << std::endl;
        return 0;
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
