#include "MDSUDPEndpoint.h"
#define dout_subsys ceph_subsys_mds

int MDSUDPManager::send(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  std::shared_lock<std::shared_mutex> lock(endpoint_mutex);
  std::vector<boost::asio::const_buffer> buf(2);
  for (auto &[key, endpoint] : endpoints) {
    uint64_t len = message->message.length();
    buf[0] = boost::asio::buffer(&len, sizeof(len));
    buf[1] = boost::asio::buffer(message->message.c_str(),
                                 message->message.length());
    endpoint->publish_internal(buf, message->seq_id);
  }
  return 0;
}

int MDSUDPManager::add_endpoint(const std::string &name, const std::string &ip,
                                int port) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  auto it = endpoints.find(name);
  if (it == endpoints.end() && endpoints.size() >= MAX_CONNECTIONS_DEFAULT) {
    ldout(cct, 1) << "UDP connect: max connections exceeded" << dendl;
    return -ENOMEM;
  }
  std::shared_ptr<MDSUDPEndpoint> new_endpoint =
      MDSUDPEndpoint::create(cct, name, ip, port);
  if (!new_endpoint) {
    ldout(cct, 1) << "UDP connect: udp endpoint creation failed" << dendl;
    return -ECONNREFUSED;
  }
  endpoints[name] = new_endpoint;
  ldout(cct, 1) << "UDP endpoint with entity name '" << name
                  << "' is added successfully" << dendl;
  return 0;
}

int MDSUDPManager::remove_endpoint(const std::string &name) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  auto it = endpoints.find(name);
  if (it != endpoints.end()) {
    endpoints.erase(it);
    ldout(cct, 1) << "UDP endpoint with entity name '" << name
                  << "' is removed successfully" << dendl;
    return 0;
  }
  ldout(cct, 1) << "No UDP endpoint exist with entity name '" << name << "'"
                << dendl;
  return -EINVAL;
}

MDSUDPEndpoint::MDSUDPEndpoint(CephContext *cct, const std::string &name,
                               const std::string &ip, int port)
    : cct(cct), name(name), socket(io_context), ip(ip), port(port),
      endpoint(boost::asio::ip::address::from_string(ip), port) {
  try {
    boost::system::error_code ec;
    socket.open(boost::asio::ip::udp::v4(), ec);
    if (ec) {
      throw std::runtime_error(ec.message());
    }
  } catch (const std::exception &e) {
    lderr(cct) << "Error occurred while opening UDP socket with error:"
               << e.what() << dendl;
    throw;
  }
}

std::shared_ptr<MDSUDPEndpoint> MDSUDPEndpoint::create(CephContext *cct,
                                                       const std::string &name,
                                                       const std::string &ip,
                                                       int port) {
  try {
    std::shared_ptr<MDSUDPEndpoint> endpoint =
        std::make_shared<MDSUDPEndpoint>(cct, name, ip, port);
    return endpoint;
  } catch (...) {
  }
  return nullptr;
}

int MDSUDPEndpoint::publish_internal(
    std::vector<boost::asio::const_buffer> &buf, uint64_t seq_id) {
  boost::system::error_code ec;
  socket.send_to(buf, endpoint, 0, ec);
  if (ec) {
    ldout(cct, 0) << "Error occurred while sending notification having seq_id="
                  << seq_id << ":" << ec.message() << dendl;
    return -ec.value();
  } else {
    ldout(cct, 20) << "Notification having seq_id=" << seq_id << " delivered"
                   << dendl;
  }
  return 0;
}
