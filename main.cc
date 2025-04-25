#include "common.h"
#include "describe_topic.h"
#include <argparse/argparse.hpp>
#include <array>
#include <librdkafka/rdkafka.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("snctl-cpp");
  program.add_argument("--config")
      .default_value("sncloud.ini")
      .help("Path to the config file");
  program.add_argument("--client-id").help("client id");

  argparse::ArgumentParser describe_command("describe");
  describe_command.add_description("Describe a topic");
  describe_command.add_argument("topic").help("Topic to describe").required();

  program.add_subparser(describe_command);
  program.parse_args(argc, argv);

  auto rk_conf = rd_kafka_conf_new();

  std::array<char, 512> errstr;
  auto fail = [&errstr](const std::string &action) {
    throw std::runtime_error("Failed to " + action + ": " + errstr.data());
  };

  auto rk_conf_map = load_rdkafka_configs(program);
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

  const auto topic = describe_command.get("topic");
  const char *topics[] = {topic.c_str()};
  auto topic_names = rd_kafka_TopicCollection_of_topic_names(topics, 1);
  std::unique_ptr<std::remove_reference_t<decltype(*topic_names)>,
                  decltype(&rd_kafka_TopicCollection_destroy)>
      topic_names_guard{topic_names, &rd_kafka_TopicCollection_destroy};

  if (program.is_subcommand_used(describe_command)) {
    DescribeTopicCommand command{rk, rkqu, describe_command};
    command.run();
  } else {
    throw std::runtime_error("Only describe command is supported");
  }

  return 0;
}
