#include "SimpleIni.h"
#include "librdkafka/rdkafka.h"
#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>

int main(int argc, char *argv[]) {
  if (argc < 2) {
    throw std::runtime_error("Usage: " + std::string(argv[0]) + " <topic>");
  }
  const auto topic = argv[1];

  CSimpleIni ini;
  if (auto rc = ini.LoadFile("sncloud.ini"); rc < 0) {
    throw std::runtime_error("Error loading config file");
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
  auto rk_conf = rd_kafka_conf_new();

  std::array<char, 512> errstr;
  auto fail = [&errstr](const std::string &action) {
    throw std::runtime_error("Failed to " + action + ": " + errstr.data());
  };

  for (auto &&[key, value] : rk_conf_map) {
    if (rd_kafka_conf_set(rk_conf, key.c_str(), value.c_str(), errstr.data(),
                          errstr.size()) != RD_KAFKA_CONF_OK) {
      fail("set " + key + " => " + value);
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

  const char *topics[] = {topic};
  auto topic_names = rd_kafka_TopicCollection_of_topic_names(topics, 1);
  std::unique_ptr<std::remove_reference_t<decltype(*topic_names)>,
                  decltype(&rd_kafka_TopicCollection_destroy)>
      topic_names_guard{topic_names, &rd_kafka_TopicCollection_destroy};

  rd_kafka_DescribeTopics(rk, topic_names, nullptr, rkqu);

  // wait infinitely
  if (auto event = rd_kafka_queue_poll(rkqu, -1); rd_kafka_event_error(event)) {
    std::cerr << "DescribeTopics failed for " << topic << ": "
              << rd_kafka_event_error_string(event);
  } else {
    auto result = rd_kafka_event_DescribeTopics_result(event);
    size_t result_topics_cnt;
    auto result_topics =
        rd_kafka_DescribeTopics_result_topics(result, &result_topics_cnt);
    for (size_t i = 0; i < result_topics_cnt; i++) {
      auto result_topic = result_topics[i];
      auto topic_name = rd_kafka_TopicDescription_name(result_topic);
      auto error = rd_kafka_TopicDescription_error(result_topic);
      if (rd_kafka_error_code(error)) {
        std::cout << "Topic: " << topic_name
                  << " has error: " << rd_kafka_error_string(error) << "\n";
        continue;
      }

      size_t partition_cnt;
      auto partitions =
          rd_kafka_TopicDescription_partitions(result_topic, &partition_cnt);
      for (size_t i = 0; i < partition_cnt; i++) {
        auto result_partition = partitions[i];
        std::cout << "  partition: " << i << std::endl;
        auto id = rd_kafka_TopicPartitionInfo_partition(result_partition);
        auto leader = rd_kafka_TopicPartitionInfo_leader(result_partition);
        if (leader) {
          std::cout << "  leader id: " << rd_kafka_Node_id(leader)
                    << ", host: " << rd_kafka_Node_host(leader) << ":"
                    << rd_kafka_Node_port(leader) << std::endl;
        } else {
          std::cout << "  leader: <none>" << std::endl;
        }
      }
    }
  }

  return 0;
}
