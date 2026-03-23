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
#include <SimpleIni.h>
#include <argparse/argparse.hpp>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

struct KafkaConfigs {
  std::string bootstrap_servers = "localhost:9092";
  std::string token;
  int fetch_message_max_bytes = 50 * 1024 * 1024;
  std::string isolation_level = "read_uncommitted";
};

struct LogConfigs {
  // Whether to enable logging for rdkafka
  bool enabled = true;
  // The file to store logs from rdkafka. If it's empty, the logs will be
  // written to the standard output.
  std::string path = "/tmp/rdkafka.log";
  // Comma-separated librdkafka debug contexts.
  std::string debug;
  // librdkafka log level, following syslog(3) levels.
  int log_level = 6;
};

class Configs : public SubCommand {
public:
  explicit Configs(argparse::ArgumentParser &parent) : SubCommand("configs") {
    update_command_.add_description("Update key-value from the INI section");
    update_command_.add_argument("--kafka-url")
        .help("The Kafka bootstrap.servers");
    update_command_.add_argument("--kafka-token").help("The Kafka token");
    update_command_.add_argument("--kafka-fetch-message-max-bytes")
        .help("The Kafka fetch.message.max.bytes")
        .scan<'i', int>();
    update_command_.add_argument("--kafka-isolation-level")
        .help("The Kafka isolation.level");
    update_command_.add_argument("--log-debug")
        .help("Comma-separated librdkafka debug contexts");
    update_command_.add_argument("--log-level")
        .help("librdkafka log level (0-7)");

    add_child(update_command_);
    attach_parent(parent);
  }

  // This method must be called after parent.parse_args() is called
  void init(argparse::ArgumentParser &parent) {
    for (auto &&file : parent.get<std::vector<std::string>>("config")) {
      if (!std::filesystem::exists(file)) {
        continue;
      }
      if (load_file(file)) {
        break;
      }
    }
    if (config_file_.empty()) {
      config_file_ = std::filesystem::current_path() / "sncloud.ini";
      std::cout << "No config file found. Creating " << config_file_
                << " with the default configs" << std::endl;
      save_file();
    }
  }

  void run() {
    if (is_subcommand_used(update_command_)) {
      bool updated = false;
      if (update_command_.present("--kafka-url")) {
        if (auto value = update_command_.get("--kafka-url");
            value != kafka_configs_.bootstrap_servers) {

          kafka_configs_.bootstrap_servers = value;
          std::cout << "Updated bootstrap.servers to " << value << std::endl;
          updated = true;
        } else {
          std::cout << "The provided bootstrap.servers is the same with the "
                       "config in "
                    << config_file_ << std::endl;
        }
      }
      if (update_command_.present("--kafka-token")) {
        auto value = update_command_.get("--kafka-token");
        if (value.empty()) {
          throw std::invalid_argument("The token cannot be empty");
        }
        if (value != kafka_configs_.token) {
          kafka_configs_.token = value;
          updated = true;
          std::cout << "Updated token" << std::endl;
        } else {
          std::cout << "The provided token is the same with the config in "
                    << config_file_ << std::endl;
        }
      }
      if (update_command_.present("--kafka-fetch-message-max-bytes")) {
        auto value = parse_fetch_message_max_bytes(
            update_command_.get<int>("--kafka-fetch-message-max-bytes"));
        if (value != kafka_configs_.fetch_message_max_bytes) {
          kafka_configs_.fetch_message_max_bytes = value;
          updated = true;
          std::cout << "Updated fetch.message.max.bytes to " << value
                    << std::endl;
        } else {
          std::cout << "The provided fetch.message.max.bytes is the same with "
                       "the config in "
                    << config_file_ << std::endl;
        }
      }
      if (update_command_.present("--kafka-isolation-level")) {
        auto value = parse_isolation_level(
            update_command_.get("--kafka-isolation-level"));
        if (value != kafka_configs_.isolation_level) {
          kafka_configs_.isolation_level = value;
          updated = true;
          std::cout << "Updated isolation.level to " << value << std::endl;
        } else {
          std::cout << "The provided isolation.level is the same with the "
                       "config in "
                    << config_file_ << std::endl;
        }
      }
      if (update_command_.present("--log-debug")) {
        auto value = update_command_.get("--log-debug");
        if (value != log_configs_.debug) {
          log_configs_.debug = value;
          updated = true;
          if (value.empty()) {
            std::cout << "Cleared librdkafka debug contexts" << std::endl;
          } else {
            std::cout << "Updated librdkafka debug contexts to " << value
                      << std::endl;
          }
        } else {
          std::cout << "The provided librdkafka debug contexts are the same "
                       "with the config in "
                    << config_file_ << std::endl;
        }
      }
      if (update_command_.present("--log-level")) {
        auto value = parse_log_level(update_command_.get("--log-level"));
        if (value != log_configs_.log_level) {
          log_configs_.log_level = value;
          updated = true;
          std::cout << "Updated librdkafka log level to " << value << std::endl;
        } else {
          std::cout << "The provided librdkafka log level is the same with "
                       "the config in "
                    << config_file_ << std::endl;
        }
      }
      if (updated) {
        save_file();
        std::cout << "Updated config file " << config_file_ << std::endl;
      } else {
        std::cout << "No config updated" << std::endl;
      }
    } else {
      fail();
    }
  }

  const auto &config_file() const noexcept { return config_file_; }

  const auto &kafka_configs() const noexcept { return kafka_configs_; }

  const auto &log_configs() const noexcept { return log_configs_; }

private:
  argparse::ArgumentParser update_command_{"update"};

  CSimpleIni ini_;
  std::string config_file_;
  KafkaConfigs kafka_configs_;
  LogConfigs log_configs_;

  std::optional<std::string> get_value(const std::string &section,
                                       const std::string &key) {
    const auto *value = ini_.GetValue(section.c_str(), key.c_str());
    if (value == nullptr) {
      return std::nullopt;
    }
    return std::optional(value);
  }

  bool load_file(const std::string &file) {
    if (auto rc = ini_.LoadFile(file.c_str()); rc != SI_OK) {
      std::cerr << "Failed to load existing file " << file << ": " << rc
                << std::endl;
      return false;
    }

    // reset configs
    kafka_configs_ = {};
    log_configs_ = {};

    // load configs from the INI file
    if (auto value = get_value("kafka", "bootstrap.servers"); value) {
      kafka_configs_.bootstrap_servers = *value;
    } else {
      std::cerr << "No bootstrap.servers found in the kafka section. Use the "
                   "default value: "
                << kafka_configs_.bootstrap_servers << std::endl;
    }
    if (auto value = get_value("kafka", "token"); value) {
      kafka_configs_.token = *value;
    }
    if (auto value = get_value("kafka", "fetch.message.max.bytes"); value) {
      try {
        kafka_configs_.fetch_message_max_bytes =
            parse_fetch_message_max_bytes(*value);
      } catch (const std::exception &) {
        std::cerr << "Invalid kafka.fetch.message.max.bytes '" << *value
                  << "'. Use the default value: "
                  << kafka_configs_.fetch_message_max_bytes << std::endl;
      }
    }
    if (auto value = get_value("kafka", "isolation.level"); value) {
      try {
        kafka_configs_.isolation_level = parse_isolation_level(*value);
      } catch (const std::exception &) {
        std::cerr << "Invalid kafka.isolation.level '" << *value
                  << "'. Use the default value: "
                  << kafka_configs_.isolation_level << std::endl;
      }
    }
    if (auto value = get_value("log", "enabled"); value) {
      log_configs_.enabled = std::string(*value) != "false";
    }
    if (auto value = get_value("log", "path"); value) {
      log_configs_.path = *value;
    }
    if (auto value = get_value("log", "debug"); value) {
      log_configs_.debug = *value;
    }
    if (auto value = get_value("log", "log_level"); value) {
      try {
        log_configs_.log_level = parse_log_level(*value);
      } catch (const std::exception &) {
        std::cerr << "Invalid log.log_level '" << *value
                  << "'. Use the default value: " << log_configs_.log_level
                  << std::endl;
      }
    }

    config_file_ = file;
    return true;
  }

  static int parse_log_level(const std::string &value) {
    size_t processed = 0;
    const auto log_level = std::stoi(value, &processed);
    if (processed != value.size() || log_level < 0 || log_level > 7) {
      throw std::invalid_argument(
          "The librdkafka log level must be between 0 and 7");
    }
    return log_level;
  }

  static int parse_fetch_message_max_bytes(const std::string &value) {
    size_t processed = 0;
    const auto bytes = std::stoi(value, &processed);
    if (processed != value.size() || bytes <= 0) {
      throw std::invalid_argument(
          "The Kafka fetch.message.max.bytes must be greater than 0");
    }
    return bytes;
  }

  static int parse_fetch_message_max_bytes(int value) {
    return parse_fetch_message_max_bytes(std::to_string(value));
  }

  static std::string parse_isolation_level(const std::string &value) {
    if (value == "read_uncommitted" || value == "read_committed") {
      return value;
    }
    throw std::invalid_argument(
        "The Kafka isolation.level must be read_uncommitted or "
        "read_committed");
  }

  void save_file() {
    ini_.SetValue("kafka", "bootstrap.servers",
                  kafka_configs_.bootstrap_servers.c_str());
    ini_.SetValue("kafka", "token", kafka_configs_.token.c_str());
    ini_.SetLongValue("kafka", "fetch.message.max.bytes",
                      kafka_configs_.fetch_message_max_bytes);
    ini_.SetValue("kafka", "isolation.level",
                  kafka_configs_.isolation_level.c_str());
    ini_.SetBoolValue("log", "enabled", log_configs_.enabled);
    ini_.SetValue("log", "path", log_configs_.path.c_str());
    ini_.SetValue("log", "debug", log_configs_.debug.c_str());
    ini_.SetLongValue("log", "log_level", log_configs_.log_level);
    ini_.SaveFile(config_file_.c_str());
  }
};
