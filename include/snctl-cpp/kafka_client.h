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

#include <array>
#include <cstdio>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

class KafkaClient final {
public:
  KafkaClient(rd_kafka_type_t type,
              const std::unordered_map<std::string, std::string> &configs,
              const LogConfigs &log_configs, bool with_queue = false)
      : log_file_(nullptr, &fclose), rk_(nullptr, &rd_kafka_destroy),
        queue_(nullptr, &rd_kafka_queue_destroy) {
    std::array<char, 512> errstr;
    auto fail = [&errstr](const std::string &action) {
      throw std::runtime_error("Failed to " + action + ": " + errstr.data());
    };

    auto *rk_conf = rd_kafka_conf_new();
    for (auto &&[key, value] : configs) {
      if (rd_kafka_conf_set(rk_conf, key.c_str(), value.c_str(), errstr.data(),
                            errstr.size()) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(rk_conf);
        throw std::runtime_error("Failed to set " + key + " => " + value +
                                 ": " + errstr.data());
      }
    }

    if (log_configs.enabled) {
      FILE *log_output = stdout;
      if (!log_configs.path.empty()) {
        log_file_.reset(fopen(log_configs.path.c_str(), "a"));
        if (log_file_ == nullptr) {
          rd_kafka_conf_destroy(rk_conf);
          throw std::runtime_error("Failed to open log file: " +
                                   log_configs.path);
        }
        log_output = log_file_.get();
      }
      rd_kafka_conf_set_opaque(rk_conf, log_output);
      rd_kafka_conf_set_log_cb(
          rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                       const char *buf) {
            auto *file = static_cast<FILE *>(rd_kafka_opaque(rk));
            fprintf(file, "[%d] %s: %s\n", level, fac, buf);
            fflush(file);
          });
    } else {
      rd_kafka_conf_set_log_cb(
          rk_conf, +[](const rd_kafka_t *rk, int level, const char *fac,
                       const char *buf) {});
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
  std::unique_ptr<FILE, decltype(&fclose)> log_file_;
  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> rk_;
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> queue_;
};
