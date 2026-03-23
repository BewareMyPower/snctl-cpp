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

#include "snctl-cpp/kafka_client.h"
#include "snctl-cpp/raii_helper.h"
#include "snctl-cpp/stop_signal.h"

#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class ConsumeCommand final {
public:
  explicit ConsumeCommand(argparse::ArgumentParser &parent) {
    command_.add_description("Create N consumers on a topic");
    command_.add_argument("topic").help("Topic to consume from").required();
    command_.add_argument("-n", "--consumers")
        .help("Number of consumers")
        .scan<'i', int>()
        .default_value(1);
    command_.add_argument("--group").help("Consumer group id");
    command_.add_argument("--offset-reset")
        .help("Offset reset policy for new groups: earliest or latest")
        .default_value(std::string("earliest"));
    command_.add_argument("--report-interval-ms")
        .help("Stats report interval in milliseconds")
        .scan<'i', int>()
        .default_value(1000);

    parent.add_subparser(command_);
  }

  bool used_by_parent(argparse::ArgumentParser &parent) const {
    return parent.is_subcommand_used(command_);
  }

  void run(const std::unordered_map<std::string, std::string> &base_configs,
           const LogConfigs &log_configs,
           const std::optional<std::string> &client_id_base) {
    const auto topic = command_.get("topic");
    const auto consumer_count = command_.get<int>("--consumers");
    const auto offset_reset = command_.get("--offset-reset");
    const auto report_interval_ms = command_.get<int>("--report-interval-ms");

    if (consumer_count <= 0) {
      throw std::invalid_argument(
          "The number of consumers must be greater than 0");
    }
    if (offset_reset != "earliest" && offset_reset != "latest") {
      throw std::invalid_argument(
          "The offset reset policy must be either earliest or latest");
    }
    if (report_interval_ms <= 0) {
      throw std::invalid_argument(
          "The report interval must be greater than 0 milliseconds");
    }

    const auto group_id =
        command_.present("--group").value_or(default_group_id(topic));

    std::cout << "Started " << consumer_count << " consumer"
              << (consumer_count == 1 ? "" : "s") << " on topic \"" << topic
              << "\" in group \"" << group_id << "\". Press Ctrl+C to stop."
              << std::endl;

    StopSignalGuard stop_signal_guard;
    std::atomic<uint64_t> consumed_messages = 0;
    std::atomic<uint64_t> consumed_bytes = 0;
    std::atomic<uint64_t> poll_errors = 0;
    std::vector<std::thread> threads;
    std::mutex errors_mu;
    std::vector<std::string> errors;

    auto add_error = [&errors_mu, &errors](std::string message) {
      std::lock_guard<std::mutex> lock(errors_mu);
      errors.emplace_back(std::move(message));
    };

    threads.reserve(consumer_count);
    for (int i = 0; i < consumer_count; i++) {
      threads.emplace_back([&, consumer_index = i]() {
        try {
          auto client_configs = base_configs;
          client_configs["group.id"] = group_id;
          client_configs["client.id"] =
              make_client_id(client_id_base, group_id, consumer_index);
          client_configs["auto.offset.reset"] = offset_reset;
          KafkaClient client(RD_KAFKA_CONSUMER, client_configs, log_configs);

          auto *subscription = rd_kafka_topic_partition_list_new(1);
          if (subscription == nullptr) {
            throw std::runtime_error("consumer[" +
                                     std::to_string(consumer_index) +
                                     "] failed to create subscription list");
          }
          GUARD(subscription, rd_kafka_topic_partition_list_destroy);
          rd_kafka_topic_partition_list_add(subscription, topic.c_str(),
                                            RD_KAFKA_PARTITION_UA);

          const auto subscribe_err =
              rd_kafka_subscribe(client.rk(), subscription);
          if (subscribe_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw std::runtime_error(
                "consumer[" + std::to_string(consumer_index) +
                "] failed to subscribe: " + rd_kafka_err2str(subscribe_err));
          }

          while (!StopSignalGuard::is_stop_requested()) {
            auto *message = rd_kafka_consumer_poll(client.rk(), 250);
            if (message == nullptr) {
              continue;
            }

            if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
              consumed_messages++;
              consumed_bytes += static_cast<uint64_t>(message->len);
            } else if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
              printf("consumer[%d] error: %s\n", consumer_index,
                     rd_kafka_message_errstr(message));
              poll_errors++;
            }
            rd_kafka_message_destroy(message);
          }

          const auto close_err = rd_kafka_consumer_close(client.rk());
          if (close_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw std::runtime_error(
                "consumer[" + std::to_string(consumer_index) +
                "] failed to close: " + rd_kafka_err2str(close_err));
          }
        } catch (const std::exception &e) {
          add_error(e.what());
          StopSignalGuard::request_stop();
        }
      });
    }

    const auto report_interval = std::chrono::milliseconds(report_interval_ms);
    uint64_t previous_consumed = 0;
    while (!StopSignalGuard::is_stop_requested()) {
      std::this_thread::sleep_for(report_interval);

      const auto current_consumed = consumed_messages.load();
      const auto current_bytes = consumed_bytes.load();
      const auto current_errors = poll_errors.load();
      const auto delta = current_consumed - previous_consumed;
      const auto rate = static_cast<double>(delta) * 1000.0 /
                        static_cast<double>(report_interval_ms);

      std::cout << "Consumed " << current_consumed << " messages (" << rate
                << " msg/s), bytes: " << current_bytes
                << ", poll errors: " << current_errors << std::endl;
      previous_consumed = current_consumed;

      {
        std::lock_guard<std::mutex> lock(errors_mu);
        if (!errors.empty()) {
          break;
        }
      }
    }

    for (auto &thread : threads) {
      thread.join();
    }

    std::cout << "Stopped consumers. Consumed " << consumed_messages.load()
              << " messages, bytes: " << consumed_bytes.load()
              << ", poll errors: " << poll_errors.load() << std::endl;

    if (!errors.empty()) {
      throw std::runtime_error(errors.front());
    }
  }

private:
  argparse::ArgumentParser command_{"consume"};

  static std::string default_group_id(const std::string &topic) {
    return "snctl-cpp-" + topic + "-" + std::to_string(std::time(nullptr));
  }

  static std::string
  make_client_id(const std::optional<std::string> &client_id_base,
                 const std::string &group_id, int consumer_index) {
    if (client_id_base.has_value() && !client_id_base->empty()) {
      return *client_id_base + "-consumer-" + std::to_string(consumer_index);
    }
    return group_id + "-consumer-" + std::to_string(consumer_index);
  }
};
