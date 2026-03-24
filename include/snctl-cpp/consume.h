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
#include "snctl-cpp/logging.h"
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
#include <sstream>
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
    command_.add_argument("--debug")
        .default_value(false)
        .implicit_value(true)
        .help("Print each consumed message's metadata");

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
    const auto debug = command_.get<bool>("debug");

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

    logging::out() << "Started " << consumer_count << " consumer"
                   << (consumer_count == 1 ? "" : "s") << " on topic \""
                   << topic << "\" in group \"" << group_id
                   << "\". Press Ctrl+C to stop.";

    StopSignalGuard stop_signal_guard;
    std::atomic<uint64_t> consumed_messages = 0;
    std::atomic<uint64_t> consumed_bytes = 0;
    std::atomic<uint64_t> poll_errors = 0;
    std::vector<std::thread> threads;
    std::mutex errors_mu;
    std::mutex output_mu;
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
          KafkaClient client(
              RD_KAFKA_CONSUMER, client_configs, log_configs, false,
              [&output_mu, consumer_index](
                  rd_kafka_t *rk, rd_kafka_resp_err_t err,
                  const rd_kafka_topic_partition_list_t *partitions) {
                std::lock_guard<std::mutex> lock(output_mu);
                if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ||
                    err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                  logging::out()
                      << "consumer[" << consumer_index << "] "
                      << rebalance_action(err)
                      << " partitions: " << format_partitions(partitions)
                      << " (current assignment: " << current_assignment(rk)
                      << ")";
                  return;
                }
                logging::err()
                    << "consumer[" << consumer_index
                    << "] rebalance error: " << rd_kafka_err2str(err)
                    << " (current assignment: " << current_assignment(rk)
                    << ")";
              });

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
              if (debug) {
                std::lock_guard<std::mutex> lock(output_mu);
                logging::out() << "consumer[" << consumer_index
                               << "] message topic=" << message_topic(message)
                               << " partition=" << message->partition
                               << " offset=" << message->offset
                               << " timestamp=" << message_timestamp(message);
              }
            } else if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
              std::lock_guard<std::mutex> lock(output_mu);
              logging::err() << "consumer[" << consumer_index
                             << "] error: " << rd_kafka_message_errstr(message);
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

      {
        std::lock_guard<std::mutex> lock(output_mu);
        logging::out() << "Consumed " << current_consumed << " messages ("
                       << rate << " msg/s), bytes: " << current_bytes
                       << ", poll errors: " << current_errors;
      }
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

    {
      std::lock_guard<std::mutex> lock(output_mu);
      logging::out() << "Stopped consumers. Consumed "
                     << consumed_messages.load()
                     << " messages, bytes: " << consumed_bytes.load()
                     << ", poll errors: " << poll_errors.load();
    }

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

  static const char *rebalance_action(rd_kafka_resp_err_t err) noexcept {
    switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
      return "assigned";
    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
      return "revoked";
    default:
      return "handled";
    }
  }

  static std::string
  format_partitions(const rd_kafka_topic_partition_list_t *partitions) {
    if (partitions == nullptr || partitions->cnt == 0) {
      return "(none)";
    }

    std::ostringstream oss;
    for (int i = 0; i < partitions->cnt; i++) {
      if (i > 0) {
        oss << ", ";
      }
      const auto &partition = partitions->elems[i];
      oss << partition.topic << "[" << partition.partition << "]";
    }
    return oss.str();
  }

  static std::string current_assignment(rd_kafka_t *rk) {
    rd_kafka_topic_partition_list_t *assignment = nullptr;
    const auto err = rd_kafka_assignment(rk, &assignment);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return std::string("<failed to query assignment: ") +
             rd_kafka_err2str(err) + ">";
    }
    GUARD(assignment, rd_kafka_topic_partition_list_destroy);
    return format_partitions(assignment);
  }

  static std::string message_topic(const rd_kafka_message_t *message) {
    if (message == nullptr || message->rkt == nullptr) {
      return "(unknown)";
    }
    return rd_kafka_topic_name(message->rkt);
  }

  static std::string message_timestamp(const rd_kafka_message_t *message) {
    if (message == nullptr) {
      return "(unknown)";
    }

    rd_kafka_timestamp_type_t timestamp_type = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
    const auto timestamp = rd_kafka_message_timestamp(message, &timestamp_type);
    if (timestamp < 0 || timestamp_type == RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
      return "not available";
    }

    const auto timestamp_time_point = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(timestamp));
    return logging::format_timestamp(timestamp_time_point) + " (" +
           timestamp_type_name(timestamp_type) + ")";
  }

  static std::string
  timestamp_type_name(rd_kafka_timestamp_type_t timestamp_type) {
    switch (timestamp_type) {
    case RD_KAFKA_TIMESTAMP_CREATE_TIME:
      return "create_time";
    case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME:
      return "log_append_time";
    case RD_KAFKA_TIMESTAMP_NOT_AVAILABLE:
      return "not_available";
    }
    return "unknown";
  }
};
