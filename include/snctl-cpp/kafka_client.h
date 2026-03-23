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

#include "snctl-cpp/configs.h"
#include "snctl-cpp/logging.h"

#include <array>
#include <cstring>
#include <fstream>
#include <functional>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

class KafkaClient final {
public:
  using DeliveryReportCallback =
      std::function<void(const rd_kafka_message_t *message)>;
  using RebalanceCallback =
      std::function<void(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                         const rd_kafka_topic_partition_list_t *partitions)>;

  KafkaClient(rd_kafka_type_t type,
              const std::unordered_map<std::string, std::string> &configs,
              const LogConfigs &log_configs, bool with_queue = false,
              RebalanceCallback rebalance_callback = {},
              DeliveryReportCallback delivery_report_callback = {})
      : opaque_(std::make_unique<Opaque>()), rk_(nullptr, &rd_kafka_destroy),
        queue_(nullptr, &rd_kafka_queue_destroy) {
    std::array<char, 512> errstr;
    auto fail = [&errstr](const std::string &action) {
      std::string message = "Failed to ";
      message += action;
      message += ": ";
      message += errstr.data();
      throw std::runtime_error(message);
    };

    auto *rk_conf = rd_kafka_conf_new();
    for (auto &&[key, value] : configs) {
      if (rd_kafka_conf_set(rk_conf, key.c_str(), value.c_str(), errstr.data(),
                            errstr.size()) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(rk_conf);
        std::string message = "Failed to set ";
        message += key;
        message += " => ";
        message += value;
        message += ": ";
        message += errstr.data();
        throw std::runtime_error(message);
      }
    }

    opaque_->rebalance_callback = std::move(rebalance_callback);
    opaque_->delivery_report_callback = std::move(delivery_report_callback);
    rd_kafka_conf_set_opaque(rk_conf, opaque_.get());

    if (log_configs.enabled) {
      std::ostream *log_output = &std::cout;
      if (!log_configs.path.empty()) {
        log_file_ =
            std::make_unique<std::ofstream>(log_configs.path, std::ios::app);
        if (!log_file_->is_open()) {
          rd_kafka_conf_destroy(rk_conf);
          std::string message = "Failed to open log file: ";
          message += log_configs.path;
          throw std::runtime_error(message);
        }
        log_output = log_file_.get();
      }
      opaque_->log_output = log_output;
      rd_kafka_conf_set_log_cb(rk_conf, &KafkaClient::log_callback);
    } else {
      rd_kafka_conf_set_log_cb(rk_conf, &KafkaClient::noop_log_callback);
    }

    if (opaque_->rebalance_callback) {
      rd_kafka_conf_set_rebalance_cb(rk_conf, &KafkaClient::rebalance_callback);
    }
    if (opaque_->delivery_report_callback) {
      rd_kafka_conf_set_dr_msg_cb(rk_conf,
                                  &KafkaClient::delivery_report_callback);
    }

    auto *rk = rd_kafka_new(type, rk_conf, errstr.data(), errstr.size());
    if (rk == nullptr) {
      rd_kafka_conf_destroy(rk_conf);
      fail(type == RD_KAFKA_PRODUCER ? "create producer" : "create consumer");
    }
    rk_.reset(rk);

    if (with_queue) {
      auto *rkqu = rd_kafka_queue_new(rk_.get());
      if (rkqu == nullptr) {
        fail("create queue");
      }
      queue_.reset(rkqu);
    }
  }

  auto rk() const noexcept { return rk_.get(); }

  auto queue() const noexcept { return queue_.get(); }

private:
  struct Opaque {
    std::ostream *log_output = &std::cout;
    RebalanceCallback rebalance_callback;
    DeliveryReportCallback delivery_report_callback;
  };

  static Opaque *opaque(const rd_kafka_t *rk) noexcept {
    return static_cast<Opaque *>(rd_kafka_opaque(rk));
  }

  static void log_callback(const rd_kafka_t *rk, int level, const char *fac,
                           const char *buf) {
    auto *context = opaque(rk);
    auto *output = context != nullptr && context->log_output != nullptr
                       ? context->log_output
                       : &std::cout;
    std::ostringstream oss;
    oss << '[' << level << "] " << fac << ": " << buf;
    logging::write_line(*output, oss.str());
  }

  static void noop_log_callback(const rd_kafka_t *rk, int level,
                                const char *fac, const char *buf) {}

  static void delivery_report_callback(rd_kafka_t *rk,
                                       const rd_kafka_message_t *message,
                                       void *opaque_ptr) {
    auto *context = static_cast<Opaque *>(opaque_ptr);
    if (context != nullptr && context->delivery_report_callback) {
      context->delivery_report_callback(message);
    }
  }

  static void rebalance_callback(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                                 rd_kafka_topic_partition_list_t *partitions,
                                 void *opaque_ptr) {
    sync_rebalance_state(rk, err, partitions);
    auto *context = static_cast<Opaque *>(opaque_ptr);
    if (context != nullptr && context->rebalance_callback) {
      context->rebalance_callback(rk, err, partitions);
    }
  }

  static bool use_cooperative_rebalancing(rd_kafka_t *rk) noexcept {
    const auto *protocol = rd_kafka_rebalance_protocol(rk);
    return protocol != nullptr && std::strcmp(protocol, "COOPERATIVE") == 0;
  }

  static void
  sync_rebalance_state(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                       const rd_kafka_topic_partition_list_t *partitions) {
    switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
      if (use_cooperative_rebalancing(rk)) {
        report_rebalance_error("incrementally assign partitions",
                               rd_kafka_incremental_assign(rk, partitions));
      } else {
        report_rebalance_error("assign partitions",
                               rd_kafka_assign(rk, partitions));
      }
      break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
      if (use_cooperative_rebalancing(rk)) {
        report_rebalance_error("incrementally unassign partitions",
                               rd_kafka_incremental_unassign(rk, partitions));
      } else {
        report_rebalance_error("clear assignment",
                               rd_kafka_assign(rk, nullptr));
      }
      break;

    default:
      report_rebalance_error("clear assignment", rd_kafka_assign(rk, nullptr));
      break;
    }
  }

  static void report_rebalance_error(const char *action,
                                     rd_kafka_resp_err_t err) {
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
      return;
    }
    logging::err() << "Failed to " << action
                   << " during rebalance: " << rd_kafka_err2str(err);
  }

  static void report_rebalance_error(const char *action,
                                     rd_kafka_error_t *error) {
    if (error == nullptr) {
      return;
    }
    logging::err() << "Failed to " << action
                   << " during rebalance: " << rd_kafka_error_string(error);
    rd_kafka_error_destroy(error);
  }

  std::unique_ptr<Opaque> opaque_;
  std::unique_ptr<std::ofstream> log_file_;
  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> rk_;
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> queue_;
};
