
#include "MDSKafka.h"

#define dout_subsys ceph_subsys_mds

CephContext *MDSKafka::cct = nullptr;
CephContext *MDSKafkaTopic::cct = nullptr;

connection_t::connection_t(const std::string &broker, bool use_ssl,
                           const std::string &user, const std::string &password,
                           const std::optional<std::string> &ca_location,
                           const std::optional<std::string> &mechanism)
    : broker(broker), use_ssl(use_ssl), user(user), password(password),
      ca_location(ca_location), mechanism(mechanism) {
  combine_hash();
}

MDSKafkaManager::MDSKafkaManager(CephContext *cct)
    : cct(cct), stopped(false), worker(&MDSKafkaManager::run, this) {}

int MDSKafkaManager::remove_topic(const std::string &topic_name) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  std::shared_ptr<MDSKafka> kafka_from;
  for (auto &[hash_key, endpoint] : endpoints) {
    if (endpoint->has_topic(topic_name)) {
      kafka_from = endpoint;
      break;
    }
  }
  if (kafka_from) {
    kafka_from->remove_topic(topic_name);
    if (kafka_from->topics.size() == 0) {
      endpoints.erase(kafka_from->connection.hash_key);
    }
    ldout(cct, 1) << "Kafka topic with topic name '" << topic_name
                  << "' is removed successfully" << dendl;
    return 0;
  }
  ldout(cct, 1) << "No kafka topic exist with topic name '" << topic_name << "'"
                << dendl;
  return -EINVAL;
}

int MDSKafkaManager::add_topic(const std::string &topic_name,
                               const connection_t &connection) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  std::shared_ptr<MDSKafka> kafka_from, kafka_to;
  for (auto &[hash_key, endpoint] : endpoints) {
    if (endpoint->has_topic(topic_name)) {
      kafka_from = endpoint;
      break;
    }
  }
  auto it = endpoints.find(connection.hash_key);
  if (it != endpoints.end()) {
    kafka_to = it->second;
  }
  if (kafka_from && kafka_from == kafka_to) {
    ldout(cct, 1) << "Kafka topic with topic name '" << topic_name
                  << "' is added successfully" << dendl;
    return 0;
  }
  bool created = false;
  if (!kafka_to) {
    if (endpoints.size() >= MAX_CONNECTIONS_DEFAULT) {
      ldout(cct, 1) << "Kafka connect: max connections exceeded" << dendl;
      return -ENOMEM;
    }
    kafka_to = MDSKafka::create(cct, connection);
    if (!kafka_to) {
      return -ECONNREFUSED;
    }
    created = true;
  }
  std::shared_ptr<MDSKafkaTopic> topic =
      MDSKafkaTopic::create(cct, topic_name, kafka_to);
  if (!topic) {
    return -ECONNREFUSED;
  }
  kafka_to->add_topic(topic_name, topic);
  if (created) {
    endpoints[connection.hash_key] = kafka_to;
  }
  if (kafka_from) {
    kafka_from->remove_topic(topic_name);
    if (kafka_from->topics.size() == 0) {
      endpoints.erase(kafka_from->connection.hash_key);
    }
  }
  ldout(cct, 1) << "Kafka topic with topic name '" << topic_name
                << "' is added successfully" << dendl;
  return 0;
}

int MDSKafkaManager::send(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  std::unique_lock<std::mutex> lock(queue_mutex);
  if (message_queue.size() >= MAX_QUEUE_DEFAULT) {
    ldout(cct, 1) << "Notification message for kafka with seq_id="
                  << message->seq_id << " is dropped as queue is full" << dendl;
    return -EBUSY;
  }
  message_queue.push(message);
  return 0;
}

uint64_t MDSKafkaManager::publish(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  std::shared_lock<std::shared_mutex> lock(endpoint_mutex);
  uint64_t reply_count = 0;
  for (auto &[key, endpoint] : endpoints) {
    reply_count += endpoint->publish_internal(message);
  }
  return reply_count;
}

uint64_t MDSKafkaManager::polling(int read_timeout) {
  std::shared_lock<std::shared_mutex> lock(endpoint_mutex);
  uint64_t reply_count = 0;
  for (auto &[key, endpoint] : endpoints) {
    reply_count += endpoint->poll(read_timeout);
  }
  return reply_count;
}

void MDSKafkaManager::run() {
  while (!stopped) {
    int send_count = 0, reply_count = 0;
    while (true) {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (message_queue.empty()) {
        break;
      }
      std::shared_ptr<MDSNotificationMessage> message = message_queue.front();
      message_queue.pop();
      ++send_count;
      lock.unlock();
      reply_count += publish(message);
    }
    reply_count += polling(READ_TIMEOUT_MS_DEFAULT);
  }
}

void connection_t::combine_hash() {
  hash_key = 0;
  boost::hash_combine(hash_key, broker);
  boost::hash_combine(hash_key, use_ssl);
  boost::hash_combine(hash_key, user);
  boost::hash_combine(hash_key, password);
  if (ca_location.has_value()) {
    boost::hash_combine(hash_key, ca_location.value());
  }
  if (mechanism.has_value()) {
    boost::hash_combine(hash_key, mechanism.value());
  }
}

void MDSKafkaTopic::kafka_topic_deleter(rd_kafka_topic_t *topic_ptr) {
  if (topic_ptr) {
    rd_kafka_topic_destroy(topic_ptr);
  }
}

MDSKafkaTopic::MDSKafkaTopic(const std::string &topic_name,
                             const std::shared_ptr<MDSKafka> &kafka_endpoint)
    : topic_name(topic_name), kafka_endpoint(kafka_endpoint), head(0), tail(0),
      inflight_count(0) {}

std::shared_ptr<MDSKafkaTopic>
MDSKafkaTopic::create(CephContext *_cct, const std::string &topic_name,
                      const std::shared_ptr<MDSKafka> &kafka_endpoint) {
  try {
    if (!MDSKafkaTopic::cct && _cct) {
      MDSKafkaTopic::cct = _cct;
    }

    std::shared_ptr<MDSKafkaTopic> topic_ptr =
        std::make_shared<MDSKafkaTopic>(topic_name, kafka_endpoint);
    topic_ptr->kafka_topic_ptr.reset(rd_kafka_topic_new(
        kafka_endpoint->producer.get(), topic_name.c_str(), nullptr));
    if (!topic_ptr->kafka_topic_ptr) {
      return nullptr;
    }
    topic_ptr->delivery_ring = std::vector<bool>(MAX_INFLIGHT_DEFAULT, false);
    return topic_ptr;
  } catch (...) {
  }
  return nullptr;
}

int MDSKafkaTopic::push_unack_event() {
  std::unique_lock<std::mutex> lock(ring_mutex);
  if (inflight_count >= (int)MAX_INFLIGHT_DEFAULT) {
    return -1;
  }
  delivery_ring[tail] = true;
  int idx = tail;
  tail = (tail + 1) % MAX_INFLIGHT_DEFAULT;
  ++inflight_count;
  return idx;
}

void MDSKafkaTopic::acknowledge_event(int idx) {
  if (!(idx >= 0 && idx < (int)MAX_INFLIGHT_DEFAULT)) {
    ldout(cct, 10) << "Kafka run: unsolicited n/ack received with tag=" << idx
                   << dendl;
    return;
  }
  std::unique_lock<std::mutex> lock(ring_mutex);
  delivery_ring[idx] = false;
  while (inflight_count > 0 && !delivery_ring[head]) {
    head = (head + 1) % MAX_INFLIGHT_DEFAULT;
    --inflight_count;
  }
}

void MDSKafkaTopic::drop_last_event() {
  std::unique_lock<std::mutex> lock(ring_mutex);
  delivery_ring[tail] = false;
  tail = (tail - 1 + MAX_INFLIGHT_DEFAULT) % MAX_INFLIGHT_DEFAULT;
  --inflight_count;
}

void MDSKafka::kafka_producer_deleter(rd_kafka_t *producer_ptr) {
  if (producer_ptr) {
    rd_kafka_flush(producer_ptr,
                   10 * 1000);      // Wait for max 10 seconds to flush.
    rd_kafka_destroy(producer_ptr); // Destroy producer instance.
  }
}

MDSKafka::MDSKafka(const connection_t &connection) : connection(connection) {}

std::shared_ptr<MDSKafka> MDSKafka::create(CephContext *_cct,
                                           const connection_t &connection) {
  try {
    if (!MDSKafka::cct && _cct) {
      MDSKafka::cct = _cct;
    }
    // validation before creating kafka interface
    if (connection.user.empty() != connection.password.empty()) {
      return nullptr;
    }
    if (!connection.user.empty() && !connection.use_ssl &&
        !g_conf().get_val<bool>(
            "mds_allow_notification_secrets_in_cleartext")) {
      ldout(cct, 1) << "Kafka connect: user/password are only allowed over "
                       "secure connection"
                    << dendl;
      return nullptr;
    }
    std::shared_ptr<MDSKafka> kafka_ptr =
        std::make_shared<MDSKafka>(connection);
    char errstr[512] = {0};
    auto kafka_conf_deleter = [](rd_kafka_conf_t *conf) {
      rd_kafka_conf_destroy(conf);
    };
    std::unique_ptr<rd_kafka_conf_t, decltype(kafka_conf_deleter)> conf(
        rd_kafka_conf_new(), kafka_conf_deleter);
    if (!conf) {
      ldout(cct, 1) << "Kafka connect: failed to allocate configuration"
                    << dendl;
      return nullptr;
    }
    constexpr std::uint64_t min_message_timeout = 1;
    const auto message_timeout =
        std::max(min_message_timeout,
                 cct->_conf.get_val<uint64_t>("mds_kafka_message_timeout"));
    if (rd_kafka_conf_set(conf.get(), "message.timeout.ms",
                          std::to_string(message_timeout).c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      goto conf_error;
    }
    if (rd_kafka_conf_set(conf.get(), "bootstrap.servers",
                          connection.broker.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      goto conf_error;
    }

    if (connection.use_ssl) {
      if (!connection.user.empty()) {
        // use SSL+SASL
        if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_SSL",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf.get(), "sasl.username",
                              connection.user.c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf.get(), "sasl.password",
                              connection.password.c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20)
            << "Kafka connect: successfully configured SSL+SASL security"
            << dendl;

        if (connection.mechanism) {
          if (rd_kafka_conf_set(conf.get(), "sasl.mechanism",
                                connection.mechanism->c_str(), errstr,
                                sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            goto conf_error;
          }
          ldout(cct, 20)
              << "Kafka connect: successfully configured SASL mechanism"
              << dendl;
        } else {
          if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr,
                                sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            goto conf_error;
          }
          ldout(cct, 20) << "Kafka connect: using default SASL mechanism"
                         << dendl;
        }
      } else {
        // use only SSL
        if (rd_kafka_conf_set(conf.get(), "security.protocol", "SSL", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: successfully configured SSL security"
                       << dendl;
      }
      if (connection.ca_location) {
        if (rd_kafka_conf_set(conf.get(), "ssl.ca.location",
                              connection.ca_location->c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: successfully configured CA location"
                       << dendl;
      } else {
        ldout(cct, 20) << "Kafka connect: using default CA location" << dendl;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured security"
                     << dendl;
    } else if (!connection.user.empty()) {
      // use SASL+PLAINTEXT
      if (rd_kafka_conf_set(conf.get(), "security.protocol", "SASL_PLAINTEXT",
                            errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
          rd_kafka_conf_set(conf.get(), "sasl.username",
                            connection.user.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK ||
          rd_kafka_conf_set(conf.get(), "sasl.password",
                            connection.password.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        goto conf_error;
      }
      ldout(cct, 20) << "Kafka connect: successfully configured SASL_PLAINTEXT"
                     << dendl;

      if (connection.mechanism) {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism",
                              connection.mechanism->c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20)
            << "Kafka connect: successfully configured SASL mechanism" << dendl;
      } else {
        if (rd_kafka_conf_set(conf.get(), "sasl.mechanism", "PLAIN", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
          goto conf_error;
        }
        ldout(cct, 20) << "Kafka connect: using default SASL mechanism"
                       << dendl;
      }
    }
    rd_kafka_conf_set_dr_msg_cb(conf.get(), message_callback);
    rd_kafka_conf_set_opaque(conf.get(), kafka_ptr.get());
    rd_kafka_conf_set_log_cb(conf.get(), log_callback);
    rd_kafka_conf_set_error_cb(conf.get(), poll_err_callback);
    {
      rd_kafka_t *prod = rd_kafka_new(RD_KAFKA_PRODUCER, conf.release(), errstr,
                                      sizeof(errstr));
      if (!prod) {
        ldout(cct, 1) << "Kafka connect: failed to create producer: " << errstr
                      << dendl;
        return nullptr;
      }
      kafka_ptr->producer.reset(prod);
    }
    ldout(cct, 0) << "Kafka connect: successfully created new producer"
                  << dendl;
    {
      const auto log_level = cct->_conf->subsys.get_log_level(ceph_subsys_mds);
      if (log_level <= 1) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 3);
      } else if (log_level <= 2) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      } else if (log_level <= 10) {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      } else {
        rd_kafka_set_log_level(kafka_ptr->producer.get(), 5);
      }
    }
    return kafka_ptr;

  conf_error:
    ldout(cct, 1) << "Kafka connect: configuration failed: " << errstr << dendl;
    return nullptr;
  } catch (...) {
  }
  return nullptr;
}

bool MDSKafka::has_topic(const std::string &topic_name) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  return (topics.find(topic_name) != topics.end());
}

void MDSKafka::add_topic(const std::string &topic_name,
                         const std::shared_ptr<MDSKafkaTopic> &topic) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  topics[topic_name] = topic;
}

void MDSKafka::remove_topic(const std::string &topic_name) {
  std::unique_lock<std::shared_mutex> lock(topic_mutex);
  auto it = topics.find(topic_name);
  if (it != topics.end()) {
    topics.erase(it);
  }
}

void MDSKafka::log_callback(const rd_kafka_t *rk, int level, const char *fac,
                            const char *buf) {
  if (!cct) {
    return;
  }
  if (level <= 3) {
    ldout(cct, 1) << "RDKAFKA-" << level << "-" << fac << ": "
                  << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 5) {
    ldout(cct, 2) << "RDKAFKA-" << level << "-" << fac << ": "
                  << rd_kafka_name(rk) << ": " << buf << dendl;
  } else if (level <= 6) {
    ldout(cct, 10) << "RDKAFKA-" << level << "-" << fac << ": "
                   << rd_kafka_name(rk) << ": " << buf << dendl;
  } else {
    ldout(cct, 20) << "RDKAFKA-" << level << "-" << fac << ": "
                   << rd_kafka_name(rk) << ": " << buf << dendl;
  }
}

void MDSKafka::poll_err_callback(rd_kafka_t *rk, int err, const char *reason,
                                 void *opaque) {
  if (!cct) {
    return;
  }
  ldout(cct, 10) << "Kafka run: poll error(" << err << "): " << reason << dendl;
}

uint64_t MDSKafka::publish_internal(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  uint64_t reply_count = 0;
  std::shared_lock<std::shared_mutex> lock(topic_mutex);
  uint64_t read_timeout =
      cct->_conf.get_val<uint64_t>("mds_kafka_sleep_timeout");
  for (auto [topic_name, topic_ptr] : topics) {
    int idx = topic_ptr->push_unack_event();
    if (idx == -1) {
      ldout(cct, 1) << "Kafka publish (with callback): failed with error: "
                       "callback queue full, trying to poll again"
                    << dendl;
      reply_count += rd_kafka_poll(producer.get(), 3 * read_timeout);
      idx = topic_ptr->push_unack_event();
      if (idx == -1) {
        ldout(cct, 1)
            << "Kafka publish (with callback): failed with error: "
               "message dropped, callback queue full event after polling for "
            << 3 * read_timeout << "ms" << dendl;
        continue;
      }
    }
    int *tag = new int(idx);
    // RdKafka::ErrorCode response = producer->produce(
    //     topic_name, RdKafka::Topic::PARTITION_UA,
    //     RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(message->c_str()),
    //     message->length(), nullptr, 0, 0, tag);
    const auto response = rd_kafka_produce(
        topic_ptr->kafka_topic_ptr.get(), RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY, const_cast<char *>(message->message.c_str()),
        message->message.length(), nullptr, 0, tag);
    if (response == -1) {
      const auto err = rd_kafka_last_error();
      ldout(cct, 1) << "Kafka publish: failed to produce for topic: "
                    << topic_name << ". with error: " << rd_kafka_err2str(err)
                    << dendl;

      delete tag;
      topic_ptr->drop_last_event();
      continue;
    }
    reply_count += rd_kafka_poll(producer.get(), 0);
  }
  return reply_count;
}

uint64_t MDSKafka::poll(int read_timeout) {
  return rd_kafka_poll(producer.get(), read_timeout);
}

void MDSKafka::message_callback(rd_kafka_t *rk,
                                const rd_kafka_message_t *rkmessage,
                                void *opaque) {
  const auto kafka_ptr = reinterpret_cast<MDSKafka *>(opaque);
  const auto result = rkmessage->err;
  if (result == 0) {
    ldout(cct, 20) << "Kafka run: ack received with result="
                   << rd_kafka_err2str(result) << dendl;
  } else {
    ldout(cct, 1) << "Kafka run: nack received with result="
                  << rd_kafka_err2str(result)
                  << " for broker: " << kafka_ptr->connection.broker << dendl;
  }
  if (!rkmessage->_private) {
    ldout(cct, 20) << "Kafka run: n/ack received without a callback" << dendl;
    return;
  }
  int *tag = reinterpret_cast<int *>(rkmessage->_private);
  std::string topic_name = std::string(rd_kafka_topic_name(rkmessage->rkt));
  std::shared_lock<std::shared_mutex> lock(kafka_ptr->topic_mutex);
  if (kafka_ptr->topics.find(topic_name) == kafka_ptr->topics.end()) {
    ldout(cct, 20) << "Kafka run: topic=" << topic_name
                   << " is removed before ack" << dendl;
    delete tag;
    return;
  }
  std::shared_ptr<MDSKafkaTopic> topic_ptr = kafka_ptr->topics[topic_name];
  lock.unlock();
  topic_ptr->acknowledge_event(*tag);
  ldout(cct, 0) << "kafka--> message delivered, broker="
                << kafka_ptr->connection.broker << ", tag=" << *tag << dendl;
  delete tag;
}