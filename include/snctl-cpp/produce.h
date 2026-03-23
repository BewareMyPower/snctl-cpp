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
#include "snctl-cpp/stop_signal.h"

#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class ProduceCommand final {
public:
  explicit ProduceCommand(argparse::ArgumentParser &parent) {
    command_.add_description("Create N producers on a topic with a configured "
                             "total rate");
    command_.add_argument("topic").help("Topic to produce to").required();
    command_.add_argument("-n", "--producers")
        .help("Number of producers")
        .scan<'i', int>()
        .default_value(1);
    command_.add_argument("--rate")
        .help("Total message rate in messages per second across all producers")
        .scan<'i', int>()
        .required();
    command_.add_argument("--message-size")
        .help("Message payload size in bytes")
        .scan<'i', int>()
        .default_value(1024);
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
    const auto producer_count = command_.get<int>("--producers");
    const auto total_rate = command_.get<int>("--rate");
    const auto message_size = command_.get<int>("--message-size");
    const auto report_interval_ms = command_.get<int>("--report-interval-ms");

    if (producer_count <= 0) {
      throw std::invalid_argument(
          "The number of producers must be greater than 0");
    }
    if (total_rate <= 0) {
      throw std::invalid_argument("The produce rate must be greater than 0");
    }
    if (message_size <= 0) {
      throw std::invalid_argument(
          "The message size must be greater than 0 bytes");
    }
    if (report_interval_ms <= 0) {
      throw std::invalid_argument(
          "The report interval must be greater than 0 milliseconds");
    }

    std::vector<int> producer_rates(producer_count,
                                    total_rate / producer_count);
    for (int i = 0; i < total_rate % producer_count; i++) {
      producer_rates[i]++;
    }

    logging::out() << "Started " << producer_count << " producer"
                   << (producer_count == 1 ? "" : "s") << " on topic \""
                   << topic << "\" with total rate " << total_rate
                   << " msg/s. Press Ctrl+C to stop.";

    StopSignalGuard stop_signal_guard;
    std::atomic<uint64_t> enqueued_messages = 0;
    std::atomic<uint64_t> enqueue_failures = 0;
    std::atomic<uint64_t> completed_messages = 0;
    std::atomic<uint64_t> delivered_messages = 0;
    std::atomic<uint64_t> delivery_failures = 0;
    std::vector<std::thread> threads;
    std::mutex errors_mu;
    std::vector<std::string> errors;

    auto add_error = [&errors_mu, &errors](std::string message) {
      std::lock_guard<std::mutex> lock(errors_mu);
      errors.emplace_back(std::move(message));
    };

    threads.reserve(producer_count);
    for (int i = 0; i < producer_count; i++) {
      threads.emplace_back([&, producer_index = i,
                            producer_rate = producer_rates[i]]() {
        try {
          auto client_configs = base_configs;
          client_configs["client.id"] =
              make_client_id(client_id_base, producer_index);
          KafkaClient client(
              RD_KAFKA_PRODUCER, client_configs, log_configs, false, {},
              [&completed_messages, &delivered_messages,
               &delivery_failures](const rd_kafka_message_t *message) {
                completed_messages++;
                if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                  delivered_messages++;
                } else {
                  delivery_failures++;
                }
              });
          const auto start = std::chrono::steady_clock::now();
          uint64_t sequence = 0;

          while (!StopSignalGuard::is_stop_requested()) {
            const auto now = std::chrono::steady_clock::now();
            const auto elapsed = std::chrono::duration<double>(now - start);
            const auto target_messages = static_cast<uint64_t>(
                elapsed.count() * static_cast<double>(producer_rate));

            while (sequence < target_messages &&
                   !StopSignalGuard::is_stop_requested()) {
              auto key = make_key(producer_index, sequence);
              auto payload =
                  make_payload(producer_index, sequence, message_size);
              const auto err = rd_kafka_producev(
                  client.rk(), RD_KAFKA_V_TOPIC(topic.c_str()),
                  RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                  RD_KAFKA_V_KEY(key.data(), key.size()),
                  RD_KAFKA_V_VALUE(payload.data(), payload.size()),
                  RD_KAFKA_V_END);
              if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                sequence++;
                enqueued_messages++;
                continue;
              }

              if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                rd_kafka_poll(client.rk(), 100);
                continue;
              }

              enqueue_failures++;
              throw std::runtime_error("producer[" +
                                       std::to_string(producer_index) +
                                       "] failed: " + rd_kafka_err2str(err));
            }

            rd_kafka_poll(client.rk(), 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          }

          rd_kafka_flush(client.rk(), 5000);
        } catch (const std::exception &e) {
          logging::err() << "producer[" << producer_index
                         << "] encountered an error: " << e.what();
          add_error(e.what());
          StopSignalGuard::request_stop();
        }
      });
    }

    const auto report_interval = std::chrono::milliseconds(report_interval_ms);
    uint64_t previous_enqueued = 0;
    uint64_t previous_completed = 0;
    while (!StopSignalGuard::is_stop_requested()) {
      std::this_thread::sleep_for(report_interval);

      const auto current_enqueued = enqueued_messages.load();
      const auto current_enqueue_failures = enqueue_failures.load();
      const auto current_completed = completed_messages.load();
      const auto current_delivered = delivered_messages.load();
      const auto current_delivery_failures = delivery_failures.load();
      const auto enqueued_delta = current_enqueued - previous_enqueued;
      const auto completed_delta = current_completed - previous_completed;
      const auto enqueued_rate = static_cast<double>(enqueued_delta) * 1000.0 /
                                 static_cast<double>(report_interval_ms);
      const auto completed_rate = static_cast<double>(completed_delta) *
                                  1000.0 /
                                  static_cast<double>(report_interval_ms);

      logging::out() << "Enqueued " << current_enqueued << " messages ("
                     << enqueued_rate << " msg/s), completed "
                     << current_completed << " messages (" << completed_rate
                     << " msg/s), delivered: " << current_delivered
                     << ", enqueue failures: " << current_enqueue_failures
                     << ", delivery failures: " << current_delivery_failures;
      previous_enqueued = current_enqueued;
      previous_completed = current_completed;

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

    logging::out() << "Stopped producers. Enqueued " << enqueued_messages.load()
                   << " messages, completed " << completed_messages.load()
                   << " messages, delivered: " << delivered_messages.load()
                   << ", enqueue failures: " << enqueue_failures.load()
                   << ", delivery failures: " << delivery_failures.load();

    if (!errors.empty()) {
      throw std::runtime_error(errors.front());
    }
  }

private:
  argparse::ArgumentParser command_{"produce"};

  static std::string
  make_client_id(const std::optional<std::string> &client_id_base,
                 int producer_index) {
    if (client_id_base.has_value() && !client_id_base->empty()) {
      return *client_id_base + "-producer-" + std::to_string(producer_index);
    }
    return "snctl-cpp-producer-" + std::to_string(producer_index);
  }

  static std::string make_payload(int producer_index, uint64_t sequence,
                                  size_t message_size) {
    std::ostringstream oss;
    oss << "producer=" << producer_index << " sequence=" << sequence;
    auto payload = oss.str();
    if (payload.size() < message_size) {
      payload.append(message_size - payload.size(), 'x');
    } else if (payload.size() > message_size) {
      payload.resize(message_size);
    }
    return payload;
  }

  static std::string make_key(int producer_index, uint64_t sequence) {
    std::ostringstream oss;
    oss << "producer=" << producer_index << " sequence=" << sequence;
    return oss.str();
  }
};
