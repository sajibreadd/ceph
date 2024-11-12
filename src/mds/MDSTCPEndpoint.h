#pragma once

#include "MDSNotificationMessage.h"
#include "MDSRank.h"
#include <bits/stdc++.h>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

class MDSTCPEndpoint;

struct MDSTCPConnection {
  std::string ip;
  int port;
  MDSTCPConnection() = default;
  MDSTCPConnection(const std::string &ip, int port);
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MDSTCPConnection *> &o);
};
WRITE_CLASS_ENCODER(MDSTCPConnection)

class MDSTCPManager {
public:
  MDSTCPManager(MDSRank *mds);
  ~MDSTCPManager();
  int init();
  void activate();
  void pause();
  int send(const std::shared_ptr<MDSNotificationMessage> &message);
  int add_endpoint(const std::string &name, const MDSTCPConnection &connection,
                   bool write_into_disk);
  int remove_endpoint(const std::string &name, bool write_into_disk);

private:
  int load_data(std::map<std::string, bufferlist> &mp);
  int add_endpoint_into_disk(const std::string &name,
                             const MDSTCPConnection &connection);
  int remove_endpoint_from_disk(const std::string &name);
  int update_omap(const std::map<std::string, bufferlist> &mp);
  int remove_keys(const std::set<std::string> &st);
  void sync_endpoints();
  CephContext *cct;
  std::shared_mutex endpoint_mutex;
  std::mutex task_counter_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSTCPEndpoint>>
      candidate_endpoints, effective_endpoints;
  static const size_t MAX_CONNECTIONS_DEFAULT = 256;
  static const size_t MAX_TASK_IN_FLIGHT = 32768;
  MDSRank *mds;
  boost::thread worker;
  std::string object_name;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_guard;
  std::atomic<int> task_counter = 0;
  std::atomic<uint64_t> endpoints_epoch = 0;
  std::atomic<bool> paused;
  uint64_t prev_endpoints_epoch = 0;
};

class MDSTCPEndpoint {
public:
  MDSTCPEndpoint() = delete;
  MDSTCPEndpoint(CephContext *cct, boost::asio::io_context &io_context,
                 const std::string &name, const MDSTCPConnection &connection);
  int publish_internal(std::vector<boost::asio::const_buffer> &buf,
                       uint64_t seq_id, std::function<void()> callback);
  static std::shared_ptr<MDSTCPEndpoint>
  create(CephContext *cct, boost::asio::io_context &io_context,
         const std::string &name, const MDSTCPConnection &connection);
  friend class MDSTCPManager;

private:
  void do_resolve();
  void do_connect(boost::asio::ip::tcp::resolver::results_type &results);
  void do_retry();
  static const uint64_t RETRY_DELAY = 10;
  std::string name;
  MDSTCPConnection connection;
  boost::asio::ip::tcp::socket socket;
  boost::asio::ip::tcp::resolver resolver;
  boost::asio::steady_timer retry_timer;
  std::atomic<bool> connected;
  CephContext *cct;
};