#pragma once

#include "MDSNotificationMessage.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>
#include <boost/functional/hash.hpp>
#include <librdkafka/rdkafka.h>
#include <string>

class MDSKafka;

struct connection_t {
  std::string broker;
  bool use_ssl;
  std::string user;
  std::string password;
  std::optional<std::string> ca_location;
  std::optional<std::string> mechanism;
  uint64_t hash_key;
  connection_t() = default;
  connection_t(const std::string &broker, bool use_ssl, const std::string &user,
               const std::string &password,
               const std::optional<std::string> &ca_location,
               const std::optional<std::string> &mechanism);
  void combine_hash();
  std::string to_string() const { return broker + ":" + user; }
};

class MDSKafkaManager {
public:
  MDSKafkaManager(CephContext *cct);
  int add_topic(const std::string &topic_name, const connection_t &connection);
  int remove_topic(const std::string &topic_name);
  int send(const std::shared_ptr<MDSNotificationMessage> &message);
  CephContext *cct;

private:
  void run();
  uint64_t publish(const std::shared_ptr<MDSNotificationMessage> &message);
  uint64_t polling(int read_timeout);
  static const size_t MAX_CONNECTIONS_DEFAULT = 32;
  static const size_t MAX_QUEUE_DEFAULT = 32768;
  static const unsigned IDLE_TIME_MS = 100;
  static const int READ_TIMEOUT_MS_DEFAULT = 100;
  std::shared_mutex endpoint_mutex;
  std::unordered_map<uint64_t, std::shared_ptr<MDSKafka>> endpoints;
  std::mutex queue_mutex;
  std::queue<std::shared_ptr<MDSNotificationMessage>> message_queue;
  std::thread worker;
  bool stopped = false;
};

class MDSKafkaTopic {
public:
  MDSKafkaTopic() = delete;
  MDSKafkaTopic(const std::string &topic_name,
                const std::shared_ptr<MDSKafka> &kafka_endpoint);
  int push_unack_event();
  void acknowledge_event(int idx);
  void drop_last_event();
  static std::shared_ptr<MDSKafkaTopic>
  create(CephContext *_cct, const std::string &topic_name,
         const std::shared_ptr<MDSKafka> &kafka_endpoint);
  static void kafka_topic_deleter(rd_kafka_topic_t *topic_ptr);
  std::unique_ptr<rd_kafka_topic_t, decltype(&kafka_topic_deleter)>
      kafka_topic_ptr{nullptr, kafka_topic_deleter};
  friend class MDSKafka;

private:
  std::string topic_name;
  static CephContext *cct;
  std::vector<bool> delivery_ring;
  std::mutex ring_mutex;
  int head, tail, inflight_count;
  static const size_t MAX_INFLIGHT_DEFAULT = 32768;
  std::weak_ptr<MDSKafka> kafka_endpoint;
};

class MDSKafka {
public:
  MDSKafka() = delete;
  MDSKafka(const connection_t &connection);
  static std::shared_ptr<MDSKafka> create(CephContext *_cct,
                                          const connection_t &connection);
  uint64_t
  publish_internal(const std::shared_ptr<MDSNotificationMessage> &message);
  uint64_t poll(int read_timeout);
  void add_topic(const std::string &topic_name,
                 const std::shared_ptr<MDSKafkaTopic> &topic);
  bool has_topic(const std::string &topic_name);
  void remove_topic(const std::string &topic_name);
  static void kafka_producer_deleter(rd_kafka_t *producer_ptr);
  friend class MDSKafkaManager;
  friend class MDSKafkaTopic;

private:
  std::unique_ptr<rd_kafka_t, decltype(&kafka_producer_deleter)> producer{
      nullptr, kafka_producer_deleter};
  std::shared_mutex topic_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSKafkaTopic>> topics;
  static CephContext *cct;
  connection_t connection;
  static void message_callback(rd_kafka_t *rk,
                               const rd_kafka_message_t *rkmessage,
                               void *opaque);
  static void log_callback(const rd_kafka_t *rk, int level, const char *fac,
                           const char *buf);
  static void poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                void *opaque);
};
