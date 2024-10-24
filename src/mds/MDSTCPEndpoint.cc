#include "MDSTCPEndpoint.h"
#include "include/fs_types.h"

#define dout_subsys ceph_subsys_mds

MDSTCPConnection::MDSTCPConnection(const std::string &ip, int port)
    : ip(ip), port(port) {}

void MDSTCPConnection::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(ip, bl);
  encode(port, bl);
  ENCODE_FINISH(bl);
}

void MDSTCPConnection::dump(ceph::Formatter *f) const {
  f->dump_string("ip", ip);
  f->dump_bool("port", port);
}

void MDSTCPConnection::generate_test_instances(
    std::list<MDSTCPConnection *> &o) {
  o.push_back(new MDSTCPConnection);
}

void MDSTCPConnection::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(ip, iter);
  decode(port, iter);
  DECODE_FINISH(iter);
}

int MDSTCPManager::load_data(std::map<std::string, bufferlist> &mp) {
  int r = update_omap(std::map<std::string, bufferlist>());
  if (r < 0) {
    return r;
  }
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_get_vals("", "", UINT_MAX, &mp, NULL, NULL);
  mds->objecter->read(object_t(object_name),
                      object_locator_t(mds->get_metadata_pool()), op,
                      CEPH_NOSNAP, NULL, 0, &sync_finisher);
  r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error reading omap values from object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSTCPManager::update_omap(const std::map<std::string, bufferlist> &mp) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_set(mp);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error updating omap of object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSTCPManager::remove_keys(const std::set<std::string> &st) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_rm_keys(st);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error removing keys from omap of object '"
                    << object_name << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSTCPManager::add_endpoint_into_disk(const std::string &name,
                                          const MDSTCPConnection &connection) {
  std::map<std::string, bufferlist> mp;
  bufferlist bl;
  encode(connection, bl);
  mp[name] = std::move(bl);
  int r = update_omap(mp);
  return r;
}

int MDSTCPManager::remove_endpoint_from_disk(const std::string &name) {
  std::set<std::string> st;
  st.insert(name);
  int r = remove_keys(st);
  return r;
}

MDSTCPManager::MDSTCPManager(MDSRank *mds)
    : mds(mds), cct(mds->cct), paused(true), object_name("mds_tcp_endpoints"),
      endpoints_epoch(0), prev_endpoints_epoch(0), task_counter(0),
      work_guard(io_context.get_executor()) {}

int MDSTCPManager::init() {
  std::map<std::string, bufferlist> mp;
  int r = load_data(mp);
  if (r < 0) {
    lderr(cct) << "Error occurred while initilizing TCP endpoints" << dendl;
    return r;
  }
  for (auto &[key, val] : mp) {
    try {
      MDSTCPConnection connection;
      auto iter = val.cbegin();
      decode(connection, iter);
      add_endpoint(key, connection, false);
      endpoints_epoch++;
    } catch (const ceph::buffer::error &e) {
      ldout(cct, 1)
          << "No value exist in the omap of object 'mds_tcp_endpoints' "
             "for tcp entity name '"
          << key << "'" << dendl;
    }
  }
  return r;
}

void MDSTCPManager::pause() {
  if (paused) {
    return;
  }
  work_guard.reset();
  io_context.stop();
  paused = true;
  if (worker.joinable()) {
    worker.join();
  }
}

void MDSTCPManager::activate() {
  if (!paused) {
    return;
  }
  work_guard.reset();
  io_context.restart();
  paused = false;
  worker =
      boost::thread(boost::bind(&boost::asio::io_context::run, &io_context));
}

void MDSTCPManager::sync_endpoints() {
  uint64_t current_epoch = endpoints_epoch.load();
  if (prev_endpoints_epoch != current_epoch) {
    effective_endpoints = candidate_endpoints;
    prev_endpoints_epoch = current_epoch;
  }
}

int MDSTCPManager::send(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  if (paused) {
    return -CEPHFS_ECANCELED;
  }
  sync_endpoints();
  std::unique_lock<std::mutex> lock(task_counter_mutex);
  int r = 0;
  std::vector<boost::asio::const_buffer> buf(2);
  for (auto &[key, endpoint] : effective_endpoints) {
    if (task_counter.load() >= MAX_TASK_IN_FLIGHT) {
      ldout(cct, 20) << "Notification message with seq_id=" << message->seq_id
                     << " for TCP connection named '" << key
                     << "' is dropped as context queue is full" << dendl;
      r = -CEPHFS_EBUSY;
    }
    uint64_t len = message->message.length();
    buf[0] = boost::asio::buffer(&len, sizeof(len));
    buf[1] = boost::asio::buffer(message->message.c_str(),
                                 message->message.length());
    ++task_counter;
    endpoint->publish_internal(buf, message->seq_id,
                               [this]() { --task_counter; });
  }
  return r;
}

int MDSTCPManager::add_endpoint(const std::string &name,
                                const MDSTCPConnection &connection,
                                bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  std::shared_ptr<MDSTCPEndpoint> new_endpoint;
  auto it = candidate_endpoints.find(name);
  int r = 0;
  if (it == candidate_endpoints.end() &&
      candidate_endpoints.size() >= MAX_CONNECTIONS_DEFAULT) {
    ldout(cct, 1) << "TCP connect: max connections exceeded" << dendl;
    r = -CEPHFS_ENOMEM;
    goto error_occurred;
  }
  new_endpoint = MDSTCPEndpoint::create(cct, io_context, name, connection);
  if (!new_endpoint) {
    ldout(cct, 1) << "TCP connect: tcp endpoint creation failed" << dendl;
    r = -CEPHFS_ECANCELED;
    goto error_occurred;
  }
  if (write_into_disk) {
    r = add_endpoint_into_disk(name, connection);
    if (r < 0) {
      goto error_occurred;
    }
  }
  candidate_endpoints[name] = new_endpoint;
  endpoints_epoch++;
  ldout(cct, 1) << "TCP endpoint with entity name '" << name
                << "' is added successfully" << dendl;
  activate();
  return r;
error_occurred:
  lderr(cct) << "TCP endpoint with entity name '" << name
             << "' can not be added, failed with an error:" << cpp_strerror(r)
             << dendl;
  return r;
}

int MDSTCPManager::remove_endpoint(const std::string &name,
                                   bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  int r = 0;
  auto it = candidate_endpoints.find(name);
  if (it != candidate_endpoints.end()) {
    if (write_into_disk) {
      r = remove_endpoint_from_disk(name);
    }
    if (r == 0) {
      candidate_endpoints.erase(it);
      endpoints_epoch++;
      ldout(cct, 1) << "TCP endpoint with entity name '" << name
                    << "' is removed successfully" << dendl;
      if (candidate_endpoints.empty()) {
        pause();
      }
    } else {
      lderr(cct) << "TCP endpoint '" << name
                 << "' can not be removed, failed with an error:"
                 << cpp_strerror(r) << dendl;
    }
    return r;
  }
  ldout(cct, 1) << "No TCP endpoint exist with entity name '" << name << "'"
                << dendl;
  return -CEPHFS_EINVAL;
}

MDSTCPEndpoint::MDSTCPEndpoint(CephContext *cct,
                               boost::asio::io_context &io_context,
                               const std::string &name,
                               const MDSTCPConnection &connection)
    : cct(cct), name(name), resolver(io_context), socket(io_context),
      retry_timer(io_context, std::chrono::seconds(RETRY_DELAY)),
      connection(connection), connected(false) {
  do_resolve();
}

void MDSTCPEndpoint::do_resolve() {
  boost::asio::ip::tcp::resolver::query query(connection.ip,
                                              std::to_string(connection.port));
  resolver.async_resolve(
      query, [this](const boost::system::error_code &ec,
                    boost::asio::ip::tcp::resolver::results_type results) {
        if (!ec) {
          do_connect(results);
        } else {
          ldout(cct, 20) << "Resolving endpoints for TCP connection named '"
                         << name << "' failed with an error:" << ec.what()
                         << ", trying again" << dendl;
          do_retry();
        }
      });
}

void MDSTCPEndpoint::do_connect(
    boost::asio::ip::tcp::resolver::results_type &results) {
  boost::asio::async_connect(socket, results,
                             [this](const boost::system::error_code &ec,
                                    const boost::asio::ip::tcp::endpoint &) {
                               if (!ec) {
                                 connected = true;
                                 ldout(cct, 1)
                                     << "TCP connection named '" << name
                                     << "' connected succesfully" << dendl;
                               } else {
                                 ldout(cct, 20)
                                     << "Connecting TCP connection named '"
                                     << name
                                     << "' failed with an error:" << ec.what()
                                     << ", trying to reconnect again" << dendl;
                                 do_retry();
                               }
                             });
}

void MDSTCPEndpoint::do_retry() {
  retry_timer.expires_after(std::chrono::seconds(RETRY_DELAY));
  retry_timer.async_wait([this](const boost::system::error_code &ec) {
    if (!ec) {
      do_resolve();
    }
  });
}

std::shared_ptr<MDSTCPEndpoint>
MDSTCPEndpoint::create(CephContext *cct, boost::asio::io_context &io_context,
                       const std::string &name,
                       const MDSTCPConnection &connection) {
  try {
    std::shared_ptr<MDSTCPEndpoint> endpoint =
        std::make_shared<MDSTCPEndpoint>(cct, io_context, name, connection);
    return endpoint;
  } catch (...) {
  }
  return nullptr;
}

int MDSTCPEndpoint::publish_internal(
    std::vector<boost::asio::const_buffer> &buf, uint64_t seq_id,
    std::function<void()> callback) {
  if (!connected) {
    return -CEPHFS_ECANCELED;
  }
  boost::system::error_code ec;
  boost::asio::async_write(
      socket, buf,
      [this, seq_id, &callback](const boost::system::error_code &ec,
                                std::size_t bytes_sent) {
        if (ec) {
          ldout(cct, 20)
              << "Error occurred while sending notification having seq_id="
              << seq_id << " to TCP connection named '" << name
              << "':" << ec.message() << dendl;
          if (ec == boost::asio::error::connection_reset ||
              ec == boost::asio::error::broken_pipe ||
              ec == boost::asio::error::eof) {
            connected = false;
            do_resolve();
          }
        } else {
          ldout(cct, 20) << "Notification having seq_id=" << seq_id
                         << " delivered to TCP connecion named '" << name << "'"
                         << dendl;
        }
        callback();
      });
  return 0;
}