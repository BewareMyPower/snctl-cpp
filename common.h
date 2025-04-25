#pragma once

#include "SimpleIni.h"
#include "argparse/argparse.hpp"
#include <unordered_map>

inline std::unordered_map<std::string, std::string>
load_rdkafka_configs(const argparse::ArgumentParser &program) {
  const auto config_file = program.get("--config");
  CSimpleIni ini;
  if (auto rc = ini.LoadFile(config_file.c_str()); rc < 0) {
    throw std::runtime_error("Error loading config file " + config_file);
  }
  auto get_value = [&ini](const auto &key) {
    auto value = ini.GetValue("kafka", key, "");
    if (strlen(value) == 0) {
      throw std::runtime_error("Error: " + std::string(key) +
                               " not found in kafka section");
    }
    return std::string(value);
  };
  std::unordered_map<std::string, std::string> rk_conf_map{
      {"bootstrap.servers", get_value("bootstrap.servers")},
      {"sasl.mechanism", "PLAIN"},
      {"security.protocol", "SASL_SSL"},
      {"sasl.username", "user"},
      {"sasl.password", "token:" + get_value("token")}};
  if (auto client_id = program.present("--client-id")) {
    rk_conf_map["client.id"] = client_id.value();
  }
  return rk_conf_map;
}
