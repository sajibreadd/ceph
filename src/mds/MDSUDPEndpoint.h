#include "MDSNotificationMessage.h"
#include <bits/stdc++.h>
#include <boost/asio.hpp>
#include <boost/functional/hash.hpp>

class MDSUDPEndpoint;

class MDSUDPManager {
public:
  MDSUDPManager(CephContext *cct) : cct(cct) {}
  int send(const std::shared_ptr<MDSNotificationMessage> &message);
  int add_endpoint(const std::string &name, const std::string &ip, int port);
  int remove_endpoint(const std::string &name);

private:
  CephContext *cct;
  std::shared_mutex endpoint_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSUDPEndpoint>> endpoints;
  static const size_t MAX_CONNECTIONS_DEFAULT = 256;
};

class MDSUDPEndpoint {
public:
  MDSUDPEndpoint() = delete;
  MDSUDPEndpoint(CephContext *cct, const std::string &name,
                 const std::string &ip, int port);
  int publish_internal(std::vector<boost::asio::const_buffer> &buf,
                       uint64_t seq_id);
  static std::shared_ptr<MDSUDPEndpoint> create(CephContext *cct,
                                                const std::string &name,
                                                const std::string &ip,
                                                int port);
  friend class MDSUDPManager;

private:
  std::string name;
  std::string ip;
  int port;
  boost::asio::io_context io_context;
  boost::asio::ip::udp::socket socket;
  boost::asio::ip::udp::endpoint endpoint;
  CephContext *cct;
};