// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stack>
#include <fcntl.h>
#include <algorithm>
#include <sys/time.h>
#include <sys/file.h>
#include <boost/scope_exit.hpp>
#include "include/uuid.h"

#include "common/admin_socket.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "common/perf_counters_key.h"
#include "include/stringify.h"
#include "FSMirror.h"
#include "PeerReplayer.h"
#include "Utils.h"
#include "ServiceDaemon.h"

#include "json_spirit/json_spirit.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::PeerReplayer("   \
                           << m_peer.uuid << ") " << __func__

using namespace std;

// Performance Counters
enum {
  l_cephfs_mirror_peer_replayer_first = 6000,
  l_cephfs_mirror_peer_replayer_snaps_synced,
  l_cephfs_mirror_peer_replayer_snaps_deleted,
  l_cephfs_mirror_peer_replayer_snaps_renamed,
  l_cephfs_mirror_peer_replayer_snap_sync_failures,
  l_cephfs_mirror_peer_replayer_avg_sync_time,
  l_cephfs_mirror_peer_replayer_sync_bytes,
  l_cephfs_mirror_peer_replayer_last_synced_start,
  l_cephfs_mirror_peer_replayer_last_synced_end,
  l_cephfs_mirror_peer_replayer_last_synced_duration,
  l_cephfs_mirror_peer_replayer_last_synced_bytes,
  l_cephfs_mirror_peer_replayer_last,
};

namespace cephfs {
namespace mirror {

namespace {

const std::string PEER_CONFIG_KEY_PREFIX = "cephfs/mirror/peer";

std::string snapshot_dir_path(CephContext *cct, const std::string &path) {
  return path + "/" + cct->_conf->client_snapdir;
}

std::string snapshot_path(const std::string &snap_dir, const std::string &snap_name) {
  return snap_dir + "/" + snap_name;
}

std::string snapshot_path(CephContext *cct, const std::string &path, const std::string &snap_name) {
  return path + "/" + cct->_conf->client_snapdir + "/" + snap_name;
}

std::string entry_path(const std::string &dir, const std::string &name) {
  return dir + "/" + name;
}

std::map<std::string, std::string> decode_snap_metadata(snap_metadata *snap_metadata,
                                                        size_t nr_snap_metadata) {
  std::map<std::string, std::string> metadata;
  for (size_t i = 0; i < nr_snap_metadata; ++i) {
    metadata.emplace(snap_metadata[i].key, snap_metadata[i].value);
  }

  return metadata;
}

std::string peer_config_key(const std::string &fs_name, const std::string &uuid) {
  return PEER_CONFIG_KEY_PREFIX + "/" + fs_name + "/" + uuid;
}

class PeerAdminSocketCommand {
public:
  virtual ~PeerAdminSocketCommand() {
  }
  virtual int call(Formatter *f) = 0;
};

class StatusCommand : public PeerAdminSocketCommand {
public:
  explicit StatusCommand(PeerReplayer *peer_replayer)
    : peer_replayer(peer_replayer) {
  }

  int call(Formatter *f) override {
    peer_replayer->peer_status(f);
    return 0;
  }

private:
  PeerReplayer *peer_replayer;
};

// helper to open a directory relative to a file descriptor
int opendirat(MountRef mnt, int dirfd, const std::string &relpath, int flags,
              ceph_dir_result **dirp) {
  int r = ceph_openat(mnt, dirfd, relpath.c_str(), flags | O_DIRECTORY, 0);
  if (r < 0) {
    return r;
  }

  int fd = r;
  r = ceph_fdopendir(mnt, fd, dirp);
  if (r < 0) {
    ceph_close(mnt, fd);
  }
  return r;
}

} // anonymous namespace

class PeerReplayerAdminSocketHook : public AdminSocketHook {
public:
  PeerReplayerAdminSocketHook(CephContext *cct, const Filesystem &filesystem,
                              const Peer &peer, PeerReplayer *peer_replayer)
    : admin_socket(cct->get_admin_socket()) {
    int r;
    std::string cmd;

    // mirror peer status format is name@id uuid
    cmd = "fs mirror peer status "
          + stringify(filesystem.fs_name) + "@" + stringify(filesystem.fscid)
          + " "
          + stringify(peer.uuid);
    r = admin_socket->register_command(
      cmd, this, "get peer mirror status");
    if (r == 0) {
      commands[cmd] = new StatusCommand(peer_replayer);
    }
  }

  ~PeerReplayerAdminSocketHook() override {
    admin_socket->unregister_commands(this);
    for (auto &[command, cmdptr] : commands) {
      delete cmdptr;
    }
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
           const bufferlist&,
           Formatter *f, std::ostream &errss, bufferlist &out) override {
    auto p = commands.at(std::string(command));
    return p->call(f);
  }

private:
  typedef std::map<std::string, PeerAdminSocketCommand*, std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

PeerReplayer::PeerReplayer(
    CephContext *cct, FSMirror *fs_mirror, RadosRef local_cluster,
    const Filesystem &filesystem, const Peer &peer,
    const std::set<std::string, std::less<>> &directories, MountRef mount,
    ServiceDaemon *service_daemon, FileMirrorPool &file_mirror_pool)
    : m_cct(cct), m_fs_mirror(fs_mirror), m_local_cluster(local_cluster),
      m_filesystem(filesystem), m_peer(peer),
      m_directories(directories.begin(), directories.end()),
      m_local_mount(mount), m_service_daemon(service_daemon),
      m_asok_hook(new PeerReplayerAdminSocketHook(cct, filesystem, peer, this)),
      m_lock(ceph::make_mutex("cephfs::mirror::PeerReplayer::" +
                              stringify(peer.uuid))),
      file_mirror_pool(file_mirror_pool) {
  g_conf().add_observer(this);
  // reset sync stats sent via service daemon
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_FAILED_DIR_COUNT_KEY, (uint64_t)0);
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY, (uint64_t)0);

  std::string labels = ceph::perf_counters::key_create("cephfs_mirror_peers",
						       {{"source_fscid", stringify(m_filesystem.fscid)},
							{"source_filesystem", m_filesystem.fs_name},
							{"peer_cluster_name", m_peer.remote.cluster_name},
							{"peer_cluster_filesystem", m_peer.remote.fs_name}});
  PerfCountersBuilder plb(m_cct, labels, l_cephfs_mirror_peer_replayer_first,
			  l_cephfs_mirror_peer_replayer_last);
  auto prio = m_cct->_conf.get_val<int64_t>("cephfs_mirror_perf_stats_prio");
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_synced,
		      "snaps_synced", "Snapshots Synchronized", "sync", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_deleted,
		      "snaps_deleted", "Snapshots Deleted", "del", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_renamed,
		      "snaps_renamed", "Snapshots Renamed", "ren", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snap_sync_failures,
		      "sync_failures", "Snapshot Sync Failures", "fail", prio);
  plb.add_time_avg(l_cephfs_mirror_peer_replayer_avg_sync_time,
		   "avg_sync_time", "Average Sync Time", "asyn", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_sync_bytes,
		      "sync_bytes", "Sync Bytes", "sbye", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_start,
	       "last_synced_start", "Last Synced Start", "lsst", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_end,
	       "last_synced_end", "Last Synced End", "lsen", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_duration,
	       "last_synced_duration", "Last Synced Duration", "lsdn", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_last_synced_bytes,
		      "last_synced_bytes", "Last Synced Bytes", "lsbt", prio);
  m_perf_counters = plb.create_perf_counters();
  m_cct->get_perfcounters_collection()->add(m_perf_counters);
  dir_scanning_thread = g_ceph_context->_conf.get_val<uint64_t>(
      "cephfs_mirror_dir_scanning_thread");
  thread_pool_queue_size = g_ceph_context->_conf.get_val<uint64_t>(
      "cephfs_mirror_thread_pool_queue_size");
  max_element_in_cache_per_thread = g_ceph_context->_conf.get_val<uint64_t>(
      "cephfs_mirror_max_element_in_cache_per_thread");
  sync_latest_snapshot = g_ceph_context->_conf.get_val<bool>(
      "cephfs_mirror_sync_latest_snapshot");
  remote_diff_base_upon_start = g_ceph_context->_conf.get_val<bool>(
      "cephfs_mirror_remote_diff_base_upon_start");
  start_syncing =
      g_ceph_context->_conf.get_val<bool>("cephfs_mirror_start_syncing");
}

PeerReplayer::~PeerReplayer() {
  delete m_asok_hook;
  PerfCounters *perf_counters = nullptr;
  std::swap(perf_counters, m_perf_counters);
  if (perf_counters != nullptr) {
    m_cct->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
  }
}

void PeerReplayer::_inc_failed_count(const std::string &dir_root) {
  auto max_failures = g_ceph_context->_conf.get_val<uint64_t>(
  "cephfs_mirror_max_consecutive_failures_per_directory");
  auto &sync_stat = m_snap_sync_stats.at(dir_root);
  sync_stat->last_failed = clock::now();
  if (++sync_stat->nr_failures >= max_failures && !sync_stat->failed) {
    sync_stat->failed = true;
    ++m_service_daemon_stats.failed_dir_count;
    m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                   SERVICE_DAEMON_FAILED_DIR_COUNT_KEY,
                                                   m_service_daemon_stats.failed_dir_count);
  }
}

void PeerReplayer::_reset_failed_count(const std::string &dir_root) {
  auto &sync_stat = m_snap_sync_stats.at(dir_root);
  if (sync_stat->failed) {
    ++m_service_daemon_stats.recovered_dir_count;
    m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                   SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY,
                                                   m_service_daemon_stats.recovered_dir_count);
  }
  sync_stat->nr_failures.store(0);
  sync_stat->failed = false;
  sync_stat->last_failed = boost::none;
  sync_stat->last_failed_reason = boost::none;
}

bool PeerReplayer::should_backoff(DirRegistry* registry, int *retval) {

  if (is_stopping()) {
    // ceph defines EBLOCKLISTED to ESHUTDOWN (108). so use
    // EINPROGRESS to identify shutdown.
    *retval = -EINPROGRESS;
    return true;
  }
  if (registry->failed || registry->canceled) {
    *retval = -ECANCELED;
    return true;
  }

  if (m_fs_mirror->is_blocklisted()) {
    *retval = -EBLOCKLISTED;
    return true;
  }

  *retval = 0;
  return false;
}

void PeerReplayer::handle_conf_change(const ConfigProxy &conf,
                                      const std::set<std::string> &changed) {
  std::scoped_lock locker(m_lock);
  if (changed.count("cephfs_mirror_dir_scanning_thread")) {
    dir_scanning_thread = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_dir_scanning_thread");
    for (auto& [dir, registry]: m_registered) {
      if (registry->sync_pool) {
        registry->sync_pool->update_state(dir_scanning_thread);
      }
    }
  }
  if (changed.count("cephfs_mirror_thread_pool_queue_size")) {
    thread_pool_queue_size = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_thread_pool_queue_size");
    for (auto &[dir, registry] : m_registered) {
      if (registry->sync_pool) {
        registry->sync_pool->update_qlimit(thread_pool_queue_size);
      }
    }
  }
  if (changed.count("cephfs_mirror_max_element_in_cache_per_thread")) {
    max_element_in_cache_per_thread = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_max_element_in_cache_per_thread");
  }
  if (changed.count("cephfs_mirror_sync_latest_snapshot")) {
    sync_latest_snapshot = g_ceph_context->_conf.get_val<bool>(
        "cephfs_mirror_sync_latest_snapshot");
  }
  if (changed.count("cephfs_mirror_remote_diff_base_upon_start")) {
    remote_diff_base_upon_start = g_ceph_context->_conf.get_val<bool>(
        "cephfs_mirror_remote_diff_base_upon_start");
  }
  if (changed.count("cephfs_mirror_start_syncing")) {
    start_syncing =
        g_ceph_context->_conf.get_val<bool>("cephfs_mirror_start_syncing");
  }
  if (changed.count("cephfs_mirror_max_concurrent_directory_syncs")) {
    int new_val = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_max_concurrent_directory_syncs");
    nr_replayers += (new_val - max_concurrent_directory_syncs);
    max_concurrent_directory_syncs = new_val;
  }
  if (changed.count("cephfs_mirror_use_snapdiff_api")) {
    use_snapdiff_api =
        g_ceph_context->_conf.get_val<bool>("cephfs_mirror_use_snapdiff_api");
  }
  if (changed.count("cephfs_mirror_lock_directory_before_sync")) {
    lock_directory_before_sync = g_ceph_context->_conf.get_val<bool>(
        "cephfs_mirror_lock_directory_before_sync");
  }
  if (changed.count("cephfs_mirror_remote_timeout")) {
    remote_timeout =
        g_ceph_context->_conf.get_val<uint64_t>("cephfs_mirror_remote_timeout");
    ceph_set_session_timeout(m_remote_mount, remote_timeout);
  }
  if (changed.count("cephfs_mirror_local_timeout")) {
    local_timeout =
        g_ceph_context->_conf.get_val<uint64_t>("cephfs_mirror_local_timeout");
    ceph_set_session_timeout(m_local_mount, local_timeout);
  }
  if (changed.count("cephfs_mirror_max_consecutive_failures_before_remote_sync")) {
    max_consecutive_failures_before_remote_sync = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_max_consecutive_failures_before_remote_sync");
  }

  m_cond.notify_all();
  dout(0)
      << "cephfs_mirror_file_sync_thread="
      << g_ceph_context->_conf.get_val<uint64_t>(
             "cephfs_mirror_file_sync_thread")
      << ", cephfs_mirror_dir_scanning_thread=" << dir_scanning_thread
      << ", cephfs_mirror_remote_diff_base_upon_start="
      << remote_diff_base_upon_start
      << ", cephfs_mirror_sync_latest_snapshot=" << sync_latest_snapshot
      << ", cephfs_mirror_thread_pool_queue_size=" << thread_pool_queue_size
      << ", cephfs_mirror_max_element_in_cache_per_thread="
      << max_element_in_cache_per_thread
      << ", cephfs_mirror_start_syncing=" << start_syncing
      << ", cephfs_mirror_max_concurrent_directory_syncs="
      << max_concurrent_directory_syncs
      << ", cephfs_mirror_use_snapdiff_api=" << use_snapdiff_api
      << ", cephfs_mirror_lock_directory_before_sync="
      << lock_directory_before_sync
      << ", cephfs_mirror_remote_timeout=" << remote_timeout
      << ", cephfs_mirror_local_timeout=" << local_timeout
      << ", mds_session_blocklist_on_timeout="
      << g_ceph_context->_conf.get_val<bool>("mds_session_blocklist_on_timeout")
      << ", mds_session_blocklist_on_evict="
      << g_ceph_context->_conf.get_val<bool>("mds_session_blocklist_on_evict")
      << ", client_reconnect_stale="
      << g_ceph_context->_conf.get_val<bool>("client_reconnect_stale")
      << ", client_tick_interval="
      << g_ceph_context->_conf.get_val<std::chrono::seconds>(
             "client_tick_interval")
      << ", client_cache_size=" << g_ceph_context->_conf->client_cache_size
      << ", cephfs_mirror_max_consecutive_failures_before_remote_sync="
      << max_consecutive_failures_before_remote_sync << dendl;
}

const char **PeerReplayer::get_tracked_conf_keys() const {
  static const char *KEYS[] = {"cephfs_mirror_dir_scanning_thread",
                               "cephfs_mirror_thread_pool_queue_size",
                               "cephfs_mirror_max_element_in_cache_per_thread",
                               "cephfs_mirror_sync_latest_snapshot",
                               "cephfs_mirror_remote_diff_base_upon_start",
                               "cephfs_mirror_start_syncing",
                               "cephfs_mirror_max_concurrent_directory_syncs",
                               "cephfs_mirror_use_snapdiff_api",
                               "cephfs_mirror_lock_directory_before_sync",
                               "cephfs_mirror_remote_timeout",
                               "cephfs_mirror_local_timeout",
                               NULL};
  return KEYS;
}

int PeerReplayer::init() {
  dout(20) << ": initial dir list=[" << m_directories << "]" << dendl;
  for (auto &dir_root : m_directories) {
    m_snap_sync_stats.emplace(dir_root, std::make_shared<SnapSyncStat>());
  }
  // m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
  //   SERVICE_DAEMON_FAILED_DIR_COUNT_KEY, (uint64_t)0);
  m_service_daemon->update_peer_info(m_filesystem.fscid, m_peer,
                                     m_snap_sync_stats);

  auto &remote_client = m_peer.remote.client_name;
  auto &remote_cluster = m_peer.remote.cluster_name;
  auto remote_filesystem = Filesystem{0, m_peer.remote.fs_name};

  std::string key = peer_config_key(m_filesystem.fs_name, m_peer.uuid);
  std::string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist in_bl;
  bufferlist out_bl;

  int r = m_local_cluster->mon_command(cmd, in_bl, &out_bl, nullptr);
  dout(5) << ": mon command r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  std::string mon_host;
  std::string cephx_key;
  if (!r) {
    json_spirit::mValue root;
    if (!json_spirit::read(out_bl.to_str(), root)) {
      derr << ": invalid config-key JSON" << dendl;
      return -EBADMSG;
    }
    try {
      auto &root_obj = root.get_obj();
      mon_host = root_obj.at("mon_host").get_str();
      cephx_key = root_obj.at("key").get_str();
      dout(0) << ": remote monitor host=" << mon_host << dendl;
    } catch (std::runtime_error&) {
      derr << ": unexpected JSON received" << dendl;
      return -EBADMSG;
    }
  }

  r = connect(remote_client, remote_cluster, &m_remote_cluster, mon_host, cephx_key);
  if (r < 0) {
    derr << ": error connecting to remote cluster: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = mount(m_remote_cluster, remote_filesystem, false, &m_remote_mount);
  if (r < 0) {
    m_remote_cluster.reset();
    derr << ": error mounting remote filesystem=" << remote_filesystem << dendl;
    return r;
  }

  std::scoped_lock locker(m_lock);
  nr_replayers = max_concurrent_directory_syncs =
      g_ceph_context->_conf.get_val<uint64_t>(
          "cephfs_mirror_max_concurrent_directory_syncs");
  scanner_thread = std::make_unique<std::thread>(&PeerReplayer::run_scan, this);
  return 0;
}

void PeerReplayer::shutdown() {
  dout(20) << dendl;

  std::unique_lock locker(m_lock);
  ceph_assert(!m_stopping);
  m_stopping = true;
  for (auto &[dir_root, registry] : m_registered) {
    int sync_idx = registry->get_file_sync_queue_idx();
    if (sync_idx >= 0) {
      file_mirror_pool.drain_queue(sync_idx);
    }
    if (registry->sync_pool) {
      registry->sync_pool->deactivate();
    }
    if (sync_idx >= 0) {
      file_mirror_pool.sync_finish(sync_idx, dir_root);
    }
  }
  dout(0) << ": Directory scanning thread pools deactivated" << dendl;
  locker.unlock();

  {
    std::scoped_lock thread_map_locker(thread_map_lock);
    for (auto &[dir_root, thread_ptr] : thread_map) {
      thread_ptr->join();
    }
    thread_map.clear();
  }

  locker.lock();
  m_cond.notify_all();
  locker.unlock();

  scanner_thread->join();

  ceph_unmount(m_remote_mount);
  ceph_release(m_remote_mount);
  m_remote_mount = nullptr;
  m_remote_cluster.reset();
}

void PeerReplayer::add_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  std::scoped_lock locker(m_lock);
  m_directories.emplace_back(dir_root);
  m_snap_sync_stats.emplace(dir_root, std::make_shared <SnapSyncStat> ());
  m_service_daemon->update_peer_info(m_filesystem.fscid, m_peer,
                                     m_snap_sync_stats);
  m_cond.notify_all();
}

void PeerReplayer::remove_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  auto _dir_root = std::string(dir_root);
  std::unique_lock locker(m_lock);
  auto it = std::find(m_directories.begin(), m_directories.end(), _dir_root);
  if (it != m_directories.end()) {
    m_directories.erase(it);
  }

  auto it1 = m_registered.find(_dir_root);
  if (it1 == m_registered.end()) {
    m_snap_sync_stats.erase(_dir_root);
    m_service_daemon->update_peer_info(m_filesystem.fscid, m_peer,
                                        m_snap_sync_stats);
  } else {
    it1->second->canceled.store(true);
    int sync_idx = it1->second->get_file_sync_queue_idx();
    if (sync_idx >= 0) {
      file_mirror_pool.drain_queue(sync_idx);
    }
    if (it1->second->sync_pool) {
      it1->second->sync_pool->drain_queue();
    }
  }
  m_deleted_directories.push_back(_dir_root);
  m_cond.notify_all();
}

int PeerReplayer::register_directory(const std::string &dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  ceph_assert(m_registered.find(dir_root) == m_registered.end());

  DirRegistry* registry = new DirRegistry();
  int r = try_lock_directory(dir_root, registry);
  if (r < 0) {
    return r;
  }

  dout(5) << ": dir_root=" << dir_root << " registered" << dendl;
  m_registered.emplace(dir_root, registry);
  return 0;
}

void PeerReplayer::unregister_directory(const std::string &dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  auto it = m_registered.find(dir_root);
  ceph_assert(it != m_registered.end());

  unlock_directory(it->first, it->second);
  m_registered.erase(it);
  if (std::find(m_directories.begin(), m_directories.end(), dir_root) == m_directories.end()) {
    m_snap_sync_stats.erase(dir_root);
    m_service_daemon->update_peer_info(m_filesystem.fscid, m_peer,
                                       m_snap_sync_stats);
  }
}

int PeerReplayer::try_lock_directory(const std::string &dir_root,
                                     DirRegistry *registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  int r = ceph_open(m_remote_mount, dir_root.c_str(), O_RDONLY | O_DIRECTORY, 0);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to open remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (r == -ENOENT) {
    // we snap under dir_root, so mode does not matter much
    r = ceph_mkdirs(m_remote_mount, dir_root.c_str(), 0755);
    if (r < 0) {
      derr << ": failed to create remote directory=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    r = ceph_open(m_remote_mount, dir_root.c_str(), O_RDONLY | O_DIRECTORY, 0);
    if (r < 0) {
      derr << ": failed to open remote dir_root=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  int fd = r;

  if (lock_directory_before_sync) {
    uint64_t key = m_snap_sync_stats.at(dir_root)->dir_lock_key;
    r = ceph_flock(m_remote_mount, fd, LOCK_EX | LOCK_NB, (uint64_t)key);
    if (r != 0) {
      if (r == -EWOULDBLOCK) {
        dout(5) << ": dir_root=" << dir_root << " is locked by cephfs-mirror, "
                << "will retry again" << dendl;
      } else {
        derr << ": failed to lock dir_root=" << dir_root << ": "
             << cpp_strerror(r) << dendl;
      }

      r = ceph_close(m_remote_mount, fd);
      if (r < 0) {
        derr << ": failed to close (cleanup) remote dir_root=" << dir_root
             << ": " << cpp_strerror(r) << dendl;
      }
      return r;
    }

    dout(10) << ": dir_root=" << dir_root << " locked" << dendl;
  }

  registry->fd = fd;
  return 0;
}

void PeerReplayer::unlock_directory(const std::string &dir_root, DirRegistry* registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  int r = 0;
  if (lock_directory_before_sync) {
    r = ceph_flock(m_remote_mount, registry->fd, LOCK_UN,
                   (uint64_t)m_snap_sync_stats.at(dir_root)->dir_lock_key);
    if (r < 0) {
      derr << ": failed to unlock remote dir_root=" << dir_root << ": "
           << cpp_strerror(r) << dendl;
      return;
    }
  }

  r = ceph_close(m_remote_mount, registry->fd);
  if (r < 0) {
    derr << ": failed to close remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
  }

  dout(10) << ": dir_root=" << dir_root << " unlocked" << dendl;
}

int PeerReplayer::build_snap_map(const std::string &dir_root,
                                 std::map<uint64_t, std::string> *snap_map, bool is_remote) {
  auto snap_dir = snapshot_dir_path(m_cct, dir_root);
  dout(20) << ": dir_root=" << dir_root << ", snap_dir=" << snap_dir
           << ", is_remote=" << is_remote << dendl;

  auto lr_str = is_remote ? "remote" : "local";
  auto mnt = is_remote ? m_remote_mount : m_local_mount;

  ceph_dir_result *dirp = nullptr;
  int r = ceph_opendir(mnt, snap_dir.c_str(), &dirp);
  if (r < 0) {
    if (is_remote && r == -ENOENT) {
      return 0;
    }
    derr << ": failed to open " << lr_str << " snap directory=" << snap_dir
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  std::set<std::string> snaps;
  auto entry = ceph_readdir(mnt, dirp);
  while (entry != NULL) {
    auto d_name = std::string(entry->d_name);
    dout(20) << ": entry=" << d_name << dendl;
    if (d_name != "." && d_name != ".." && d_name.rfind("_", 0) != 0) {
      snaps.emplace(d_name);
    }

    entry = ceph_readdir(mnt, dirp);
  }

  int rv = 0;
  for (auto &snap : snaps) {
    snap_info info;
    auto snap_path = snapshot_path(snap_dir, snap);
    r = ceph_get_snap_info(mnt, snap_path.c_str(), &info);
    if (r < 0) {
      derr << ": failed to fetch " << lr_str << " snap info for snap_path=" << snap_path
           << ": " << cpp_strerror(r) << dendl;
      rv = r;
      break;
    }

    uint64_t snap_id;
    if (is_remote) {
      if (!info.nr_snap_metadata) {
        std::string failed_reason = "snapshot '" + snap  + "' has invalid metadata";
        derr << ": " << failed_reason << dendl;
        {
          std::scoped_lock locker(m_lock);
          m_snap_sync_stats.at(dir_root)->last_failed_reason = failed_reason;
        }
        rv = -EINVAL;
      } else {
        auto metadata = decode_snap_metadata(info.snap_metadata, info.nr_snap_metadata);
        dout(20) << ": snap_path=" << snap_path << ", metadata=" << metadata << dendl;
        auto it = metadata.find(PRIMARY_SNAP_ID_KEY);
        if (it == metadata.end()) {
          derr << ": snap_path=" << snap_path << " has missing \"" << PRIMARY_SNAP_ID_KEY
               << "\" in metadata" << dendl;
          rv = -EINVAL;
        } else {
          snap_id = std::stoull(it->second);
        }
        ceph_free_snap_info_buffer(&info);
      }
    } else {
      snap_id = info.id;
    }

    if (rv != 0) {
      break;
    }
    snap_map->emplace(snap_id, snap);
  }

  r = ceph_closedir(mnt, dirp);
  if (r < 0) {
    derr << ": failed to close " << lr_str << " snap directory=" << snap_dir
         << ": " << cpp_strerror(r) << dendl;
  }

  dout(10) << ": " << lr_str << " snap_map=" << *snap_map << dendl;
  return rv;
}

int PeerReplayer::propagate_snap_deletes(
    const std::string &dir_root, const std::map<uint64_t, std::string> &snaps) {
  dout(5) << ": dir_root=" << dir_root << ", deleted snapshots=" << snaps << dendl;

  for (auto &[_, snap] : snaps) {
    dout(20) << ": deleting dir_root=" << dir_root << ", snapshot=" << snap
             << dendl;
    int r = ceph_rmsnap(m_remote_mount, dir_root.c_str(), snap.c_str());
    if (r < 0) {
      derr << ": failed to delete remote snap dir_root=" << dir_root
           << ", snapshot=" << snaps << ": " << cpp_strerror(r) << dendl;
      return r;
    }
    inc_deleted_snap(dir_root);
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_deleted);
    }
  }

  return 0;
}

int PeerReplayer::propagate_snap_renames(
    const std::string &dir_root,
    const std::map<uint64_t, std::pair<std::string, std::string>> &snaps) {
  dout(10) << ": dir_root=" << dir_root << ", renamed snapshots=" << snaps << dendl;

  for (auto &[_, snapp] : snaps) {
    auto from = snapshot_path(m_cct, dir_root, snapp.first);
    auto to = snapshot_path(m_cct, dir_root, snapp.second);
    dout(20) << ": renaming dir_root=" << dir_root << ", snapshot from="
             << from << ", to=" << to << dendl;
    int r = ceph_rename(m_remote_mount, from.c_str(), to.c_str());
    if (r < 0) {
      derr << ": failed to rename remote snap dir_root=" << dir_root
           << ", snapshot from =" << from << ", to=" << to << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    inc_renamed_snap(dir_root);
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_renamed);
    }
  }

  return 0;
}

int PeerReplayer::sync_attributes(std::shared_ptr<SyncEntry> &cur_entry,
                                  const FHandles &fh) {
  int r = 0;
  if ((cur_entry->change_mask & CEPH_STATX_UID) ||
      (cur_entry->change_mask & CEPH_STATX_GID)) {
    r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                     cur_entry->stx.stx_uid, cur_entry->stx.stx_gid,
                     AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chown remote directory=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if ((cur_entry->change_mask & CEPH_STATX_MODE)) {
    r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                     cur_entry->stx.stx_mode & ~S_IFMT, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chmod remote directory=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (!cur_entry->is_directory() &&
      (cur_entry->change_mask & CEPH_STATX_MTIME)) {
    struct timespec times[] = {
        {cur_entry->stx.stx_atime.tv_sec, cur_entry->stx.stx_atime.tv_nsec},
        {cur_entry->stx.stx_mtime.tv_sec, cur_entry->stx.stx_mtime.tv_nsec}};
    r = ceph_utimensat(m_remote_mount, fh.r_fd_dir_root,
                       cur_entry->epath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to change [am]time on remote directory="
           << cur_entry->epath << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return 0;
}

int PeerReplayer::_remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry,
                                const FHandles &fh,
                                std::shared_ptr<SnapSyncStat> &sync_stat) {
  int r =
      ceph_mkdirat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                   cur_entry->stx.stx_mode & ~S_IFDIR);
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  sync_stat->current_stat.inc_dir_created_count();
  return 0;
}

int PeerReplayer::remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry,
                               const FHandles &fh,
                               std::shared_ptr<SnapSyncStat> &sync_stat) {
  dout(10) << ": remote epath=" << cur_entry->epath << dendl;
  int r = 0;
  /*
    We know that if it is not create fresh and purge remote
    then remote already has the dir entry, we just need to
    sync_attributes, thus saving a ceph_mkdirat call.
  */
  if (cur_entry->create_fresh() || cur_entry->purge_remote()) {
    r = _remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
  }

  r = sync_attributes(cur_entry, fh);
  return r;
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::copy_to_remote(std::shared_ptr<SyncEntry> &cur_entry,
                                 DirRegistry *registry, const FHandles &fh,
                                 FileMirrorPool::FileWorker *file_worker) {
  dout(10) << ": dir_root=" << registry->dir_root
           << ", epath=" << cur_entry->epath << dendl;
  uint64_t total_read = 0, total_wrote = 0;
  int r = 0;


  file_worker->state = FileMirrorPool::FileWorker::ThreadState::BACKOFF_CHECK1;
  if (should_backoff(registry, &r)) {
    dout(0) << ": backing off r=" << r << dendl;
    return r;
  }

  int l_fd;
  int r_fd;
  void *ptr;
  struct iovec iov[NR_IOVECS];

  file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_OPEN_LOCAL;
  r = ceph_openat(m_local_mount, fh.c_fd, cur_entry->epath.c_str(),
                      O_RDONLY | O_NOFOLLOW, 0);
  if (r < 0) {
    derr << ": failed to open local file path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  l_fd = r;
  file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_OPEN_REMOTE;
  r = ceph_openat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                  O_CREAT | O_TRUNC | O_WRONLY | O_NOFOLLOW,
                  cur_entry->stx.stx_mode);
  if (r < 0) {
    derr << ": failed to create remote file path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    goto close_local_fd;
  }

  r_fd = r;
  ptr = malloc(NR_IOVECS * IOVEC_SIZE);
  if (!ptr) {
    r = -ENOMEM;
    derr << ": failed to allocate memory" << dendl;
    goto close_remote_fd;
  }

  while (true) {
    file_worker->state = FileMirrorPool::FileWorker::ThreadState::BACKOFF_CHECK2;
    if (should_backoff(registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    for (int i = 0; i < NR_IOVECS; ++i) {
      iov[i].iov_base = (char *)ptr + IOVEC_SIZE * i;
      iov[i].iov_len = IOVEC_SIZE;
    }

    file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_READ;

    r = ceph_preadv(m_local_mount, l_fd, iov, NR_IOVECS, -1);
    if (r < 0) {
      derr << ": failed to read local file path=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
    if (r == 0) {
      break;
    }
    total_read += r;
    dout(20) << ": successfully read " << total_read << " bytes from local"
             << cur_entry->epath << dendl;

    int iovs = (int)(r / IOVEC_SIZE);
    int t = r % IOVEC_SIZE;
    if (t) {
      iov[iovs].iov_len = t;
      ++iovs;
    }

    file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_WRITE;

    r = ceph_pwritev(m_remote_mount, r_fd, iov, iovs, -1);
    if (r < 0) {
      derr << ": failed to write remote file path=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
    total_wrote += r;
    file_worker->bytes_synced = total_wrote;
    dout(20) << ": successfully wrote " << total_wrote << " bytes to remote "
             << cur_entry->epath << dendl;
  }

  if (r == 0) {
    file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_FSYNC;
    r = ceph_fsync(m_remote_mount, r_fd, 0);
    if (r < 0) {
      derr << ": failed to sync data for file path=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
    }
  }
  file_worker->state = FileMirrorPool::FileWorker::ThreadState::FREE_BUFFER;
  free(ptr);

close_remote_fd:
  file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_CLOSE_REMOTE;
  if (ceph_close(m_remote_mount, r_fd) < 0) {
    derr << ": failed to close remote fd path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    r = -EINVAL;
  }

close_local_fd:
  file_worker->state = FileMirrorPool::FileWorker::ThreadState::FILE_CLOSE_LOCAL;
  if (ceph_close(m_local_mount, l_fd) < 0) {
    derr << ": failed to close local fd path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    r = -EINVAL;
  }
  // dout(0) << ": file transfer finished-->" << cur_entry->epath << dendl;
  return r == 0 ? 0 : r;
}

void PeerReplayer::enqueue_file_transfer(
    FileSyncMechanism *syncm, DirRegistry *registry,
    std::shared_ptr<SnapSyncStat> &sync_stat) {
  sync_stat->current_stat.inc_file_in_flight_count();
  registry->inc_sync_indicator();
  file_mirror_pool.sync_file_data(syncm, registry->get_file_sync_queue_idx());
}

int PeerReplayer::remote_file_op(std::shared_ptr<SyncEntry> &cur_entry,
                                 DirRegistry *registry,
                                 std::shared_ptr<SnapSyncStat> &sync_stat,
                                 const FHandles &fh) {
  bool need_data_sync = cur_entry->create_fresh() ||
                        cur_entry->purge_remote() ||
                        ((cur_entry->change_mask & CEPH_STATX_SIZE) > 0) ||
                        (((cur_entry->change_mask & CEPH_STATX_MTIME) > 0));

  dout(10) << ": dir_root=" << registry->dir_root
           << ", epath=" << cur_entry->epath
           << ", need_data_sync=" << need_data_sync
           << ", stat_change_mask=" << cur_entry->change_mask << dendl;

  int r;
  if (need_data_sync) {
    if (S_ISREG(cur_entry->stx.stx_mode)) {
      FileSyncMechanism *syncm = new FileSyncMechanism(
          m_local_mount, std::move(cur_entry), registry, sync_stat, fh, this);
      enqueue_file_transfer(syncm, registry, sync_stat);
      return 0;
    } else if (S_ISLNK(cur_entry->stx.stx_mode)) {
      // free the remote link before relinking
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root,
                        cur_entry->epath.c_str(), 0);
      if (r < 0 && r != -ENOENT) {
        derr << ": failed to remove remote symlink=" << cur_entry->epath << ": "
             << cpp_strerror(r) << dendl;
        return r;
      }
      char *target = (char *)alloca(cur_entry->stx.stx_size + 1);
      r = ceph_readlinkat(m_local_mount, fh.c_fd, cur_entry->epath.c_str(),
                          target, cur_entry->stx.stx_size);
      if (r < 0) {
        derr << ": failed to readlink local path=" << cur_entry->epath << ": "
             << cpp_strerror(r) << dendl;
        return r;
      }

      target[cur_entry->stx.stx_size] = '\0';
      r = ceph_symlinkat(m_remote_mount, target, fh.r_fd_dir_root,
                         cur_entry->epath.c_str());
      if (r < 0 && r != -EEXIST) {
        derr << ": failed to symlink remote path=" << cur_entry->epath
             << " to target=" << target << ": " << cpp_strerror(r) << dendl;
        return r;
      }
      cur_entry->change_mask =
          (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID | CEPH_STATX_GID |
           CEPH_STATX_MTIME);
    } else {
      dout(5) << ": skipping entry=" << cur_entry->epath
              << ": unsupported mode=" << cur_entry->stx.stx_mode << dendl;
      return 0;
    }
  }

  unsigned int all_change =
      (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID | CEPH_STATX_GID |
       CEPH_STATX_MTIME);
  r = sync_attributes(cur_entry, fh);
  if (r < 0) {
    return r;
  }
  if (cur_entry->change_mask & all_change) {
    sync_stat->current_stat.inc_files_synced(false, cur_entry->stx.stx_size);
  } else {
    sync_stat->current_stat.inc_file_skipped_count();
  }
  
  return 0;
}

int PeerReplayer::cleanup_remote_entry(const std::string &epath,
                                       DirRegistry *registry,
                                       const FHandles &fh,
                                       std::shared_ptr<SnapSyncStat> &sync_stat,
                                       int not_dir) {
  dout(20) << ": dir_root=" << registry->dir_root << ", epath=" << epath
           << dendl;
  int r = 0;
  /*
    not_dir is a hint whether in the base state it's directory or not.
    This hint works always.
  */
  if (not_dir == 1) {
    r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
    if (r < 0 && r != -ENOENT && r != -EISDIR) {
      derr << ": failed to remove remote entry=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    if (r == -ENOENT) {
      dout(10) << ": entry already removed=" << epath << dendl;
      return r;
    }
    if (r == 0) {
      sync_stat->current_stat.inc_file_del_count();
      return r;
    }
  }

  r = 0;
  struct ceph_statx tstx;
  r = ceph_statxat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), &tstx,
                   CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                       CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                   AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r == -ENOENT) {
    dout(10) << ": entry already removed=" << epath << dendl;
    return r;
  }
  if (r < 0) {
    derr << ": failed to remove remote entry=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  /*
    if the hint didn't work(it should work always but for the sanity check), 
    that means we thought it as a directory in base state, our goal is to 
    just clean it up. No matter it's file or directory.
  */
  if (!S_ISDIR(tstx.stx_mode)) {
    r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
    if (r < 0) {
      derr << ": failed to remove remote entry=" << epath << ": "
           << cpp_strerror(r) << dendl;
    }
    sync_stat->current_stat.inc_file_del_count();
    return r;
  }

  ceph_dir_result *tdirp;
  r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath, AT_SYMLINK_NOFOLLOW,
                &tdirp);
  if (r < 0) {
    derr << ": failed to open remote directory=" << epath << ": "
        << cpp_strerror(r) << dendl;
    return r;
  }

  std::stack<SyncEntry> rm_stack;
  rm_stack.emplace(SyncEntry(epath, tdirp, tstx));
  while (!rm_stack.empty()) {
    if (should_backoff(registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    dout(20) << ": " << rm_stack.size() << " entries in stack" << dendl;
    std::string e_name;
    auto &entry = rm_stack.top();
    dout(20) << ": top of stack path=" << entry.epath << dendl;
    if (entry.is_directory()) {
      struct ceph_statx stx;
      struct dirent de;
      while (true) {
        r = ceph_readdirplus_r(m_remote_mount, entry.dirp, &de, &stx,
                              CEPH_STATX_MODE,
                              AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
        if (r < 0) {
          derr << ": failed to read remote directory=" << entry.epath << dendl;
          break;
        }
        if (r == 0) {
          break;
        }

        auto d_name = std::string(de.d_name);
        if (d_name != "." && d_name != "..") {
          e_name = d_name;
          break;
        }
      }

      if (r == 0) {
        r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(),
                          AT_REMOVEDIR);
        if (r < 0) {
          derr << ": failed to remove remote directory=" << entry.epath << ": "
              << cpp_strerror(r) << dendl;
          break;
        }
        sync_stat->current_stat.inc_dir_deleted_count();

        dout(10) << ": done for remote directory=" << entry.epath << dendl;
        if (entry.dirp && ceph_closedir(m_remote_mount, entry.dirp) < 0) {
          derr << ": failed to close remote directory=" << entry.epath << dendl;
        }
        rm_stack.pop();
        continue;
      }
      if (r < 0) {
        break;
      }

      auto epath = entry_path(entry.epath, e_name);
      if (S_ISDIR(stx.stx_mode)) {
        ceph_dir_result *dirp;
        r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath,
                      AT_SYMLINK_NOFOLLOW, &dirp);
        if (r < 0) {
          derr << ": failed to open remote directory=" << epath << ": "
              << cpp_strerror(r) << dendl;
          break;
        }
        rm_stack.emplace(SyncEntry(epath, dirp, stx));
      } else {
        rm_stack.emplace(SyncEntry(epath, stx));
      }
    } else {
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(),
                        0);
      if (r < 0) {
        derr << ": failed to remove remote directory=" << entry.epath << ": "
            << cpp_strerror(r) << dendl;
        break;
      }
      dout(10) << ": done for remote file=" << entry.epath << dendl;
      sync_stat->current_stat.inc_file_del_count();
      rm_stack.pop();
    }
  }

  while (!rm_stack.empty()) {
    auto &entry = rm_stack.top();
    if (entry.is_directory()) {
      dout(20) << ": closing remote directory=" << entry.epath << dendl;
      if (entry.dirp && ceph_closedir(m_remote_mount, entry.dirp) < 0) {
        derr << ": failed to close remote directory=" << entry.epath << dendl;
      }
    }

    rm_stack.pop();
  }

  return r;
}

int PeerReplayer::should_sync_entry(const std::string &epath, const struct ceph_statx &cstx,
                                    const FHandles &fh, bool *need_data_sync, bool *need_attr_sync) {
  dout(10) << ": epath=" << epath << dendl;

  *need_data_sync = false;
  *need_attr_sync = false;
  struct ceph_statx pstx;
  int r = ceph_statxat(fh.p_mnt, fh.p_fd, epath.c_str(), &pstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                       CEPH_STATX_SIZE | CEPH_STATX_CTIME | CEPH_STATX_MTIME,
                       AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0 && r != -ENOENT && r != -ENOTDIR) {
    derr << ": failed to stat prev entry= " << epath << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (r < 0) {
    // inode does not exist in prev snapshot or file type has changed
    // (file was S_IFREG earlier, S_IFDIR now).
    dout(5) << ": entry=" << epath << ", r=" << r << dendl;
    *need_data_sync = true;
    *need_attr_sync = true;
    return 0;
  }

  dout(10) << ": local cur statx: mode=" << cstx.stx_mode << ", uid=" << cstx.stx_uid
           << ", gid=" << cstx.stx_gid << ", size=" << cstx.stx_size << ", ctime="
           << cstx.stx_ctime << ", mtime=" << cstx.stx_mtime << dendl;
  dout(10) << ": local prev statx: mode=" << pstx.stx_mode << ", uid=" << pstx.stx_uid
           << ", gid=" << pstx.stx_gid << ", size=" << pstx.stx_size << ", ctime="
           << pstx.stx_ctime << ", mtime=" << pstx.stx_mtime << dendl;
  if ((cstx.stx_mode & S_IFMT) != (pstx.stx_mode & S_IFMT)) {
    dout(5) << ": entry=" << epath << " has mode mismatch" << dendl;
    *need_data_sync = true;
    *need_attr_sync = true;
  } else {
    *need_data_sync = (cstx.stx_size != pstx.stx_size) || (cstx.stx_mtime != pstx.stx_mtime);
    *need_attr_sync = (cstx.stx_ctime != pstx.stx_ctime);
  }

  return 0;
}

int PeerReplayer::propagate_deleted_entries(
    const std::string &epath, DirRegistry *registry,
    std::shared_ptr<SnapSyncStat> &sync_stat, const FHandles &fh,
    std::unordered_map<std::string, unsigned int> &change_mask_map,
    int &change_mask_map_size) {
  dout(10) << ": dir_root=" << registry->dir_root << ", epath=" << epath
           << dendl;
  ceph_dir_result *dirp;
  int r = opendirat(fh.p_mnt, fh.p_fd, epath, AT_SYMLINK_NOFOLLOW, &dirp);
  if (r < 0) {
    if (r == -ELOOP) {
      dout(5) << ": epath=" << epath << " is a symbolic link -- mode sync"
              << " done when traversing parent" << dendl;
      return 0;
    }
    if (r == -ENOTDIR) {
      dout(5) << ": epath=" << epath << " is not a directory -- mode sync"
              << " done when traversing parent" << dendl;
      return 0;
    }
    if (r == -ENOENT) {
      dout(5) << ": epath=" << epath
              << " missing in previous-snap/remote dir-root" << dendl;
    }
    return r;
  }
  struct ceph_statx pstx;
  struct dirent de;
  while (true) {
    if (should_backoff(registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }
    unsigned int extra_flags =
        (change_mask_map_size < max_element_in_cache_per_thread)
            ? (CEPH_STATX_UID | CEPH_STATX_GID | CEPH_STATX_SIZE |
               CEPH_STATX_MTIME)
            : 0;
    r = ceph_readdirplus_r(fh.p_mnt, dirp, &de, &pstx,
                           CEPH_STATX_MODE | extra_flags,
                           AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);

    if (r < 0) {
      derr << ": failed to read directory entries: " << cpp_strerror(r)
           << dendl;
      // flip errno to signal that we got an err (possible the
      // snapshot getting deleted in midst).
      if (r == -ENOENT) {
        r = -EINVAL;
      }
      break;
    }
    if (r == 0) {
      dout(10) << ": reached EOD" << dendl;
      break;
    }
    std::string d_name = std::string(de.d_name);
    if (d_name == "." || d_name == "..") {
      continue;
    }
    auto dpath = entry_path(epath, d_name);

    struct ceph_statx cstx;
    r = ceph_statxat(m_local_mount, fh.c_fd, dpath.c_str(), &cstx,
                     CEPH_STATX_MODE | extra_flags,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to stat local (cur) directory=" << dpath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }

    bool purge_remote = true;
    bool entry_present = false;
    if (r == 0) {
      // directory entry present in both snapshots -- check inode
      // type
      entry_present = true;
      if ((pstx.stx_mode & S_IFMT) == (cstx.stx_mode & S_IFMT)) {
        dout(5) << ": mode matches for entry=" << d_name << dendl;
        purge_remote = false;
      } else {
        dout(5) << ": mode mismatch for entry=" << d_name << dendl;
      }
    } else {
      dout(5) << ": entry=" << d_name << " missing in current snapshot"
              << dendl;
    }

    if (purge_remote && !entry_present) {
      dout(5) << ": purging remote entry=" << dpath << dendl;
      SyncMechanism *syncm = new DeleteMechanism(
          m_local_mount, std::move(std::make_shared<SyncEntry>(dpath)),
          registry, sync_stat, fh, this, !S_ISDIR(pstx.stx_mode));
      registry->sync_pool->sync_anyway(syncm);
    } else if (extra_flags) {
      unsigned int change_mask = 0;
      build_change_mask(pstx, cstx, false, purge_remote, change_mask);
      /*
        saving some of change_mask, such that we can avoid doing
        stat in previous snapshot/remote if it is found in the chage_mask map.
        This map is strategically maintained such that we can best use of
        max_element_in_cache_per_thread threshold.
      */
      change_mask_map[d_name] = change_mask;
      ++change_mask_map_size;
    }
  }
  if (dirp) {
    ceph_closedir(fh.p_mnt, dirp);
  }
  return r;
}

int PeerReplayer::open_dir(MountRef mnt, const std::string &dir_path,
                           boost::optional<uint64_t> snap_id) {
  dout(20) << ": dir_path=" << dir_path << dendl;
  if (snap_id) {
    dout(20) << ": expected snapshot id=" << *snap_id << dendl;
  }

  int fd = ceph_open(mnt, dir_path.c_str(), O_DIRECTORY | O_RDONLY, 0);
  if (fd < 0) {
    derr << ": cannot open dir_path=" << dir_path << ": " << cpp_strerror(fd)
         << dendl;
    return fd;
  }

  if (!snap_id) {
    return fd;
  }

  snap_info info;
  int r = ceph_get_snap_info(mnt, dir_path.c_str(), &info);
  if (r < 0) {
    derr << ": failed to fetch snap_info for path=" << dir_path
         << ": " << cpp_strerror(r) << dendl;
    ceph_close(mnt, fd);
    return r;
  }

  if (info.id != *snap_id) {
    dout(5) << ": got mismatching snapshot id for path=" << dir_path << " (" << info.id
            << " vs " << *snap_id << ") -- possible recreate" << dendl;
    ceph_close(mnt, fd);
    return -EINVAL;
  }

  return fd;
}

int PeerReplayer::pre_sync_check_and_open_handles(
    const std::string &dir_root,
    const Snapshot &current, boost::optional<Snapshot> prev,
    FHandles *snapdiff_fh, FHandles* remotediff_fh) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  auto cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  auto fd = open_dir(m_local_mount, cur_snap_path, current.second);
  if (fd < 0) {
    return fd;
  }
  snapdiff_fh->c_fd = snapdiff_fh->p_fd = remotediff_fh->c_fd =
      remotediff_fh->p_fd = -1;

  // current snapshot file descriptor
  snapdiff_fh->m_current = current;
  snapdiff_fh->m_prev = prev;
  snapdiff_fh->c_fd = fd;
  remotediff_fh->c_fd = fd;

  if (prev) {
    auto prev_snap_path = snapshot_path(m_cct, dir_root, (*prev).first);
    snapdiff_fh->p_fd = open_dir(m_local_mount, prev_snap_path, (*prev).second);
    snapdiff_fh->p_mnt = m_local_mount;
  }
  remotediff_fh->p_fd = open_dir(m_remote_mount, dir_root, boost::none);
  remotediff_fh->p_mnt = m_remote_mount;

  if (remotediff_fh->p_fd < 0) {
    ceph_close(m_local_mount, remotediff_fh->c_fd);
    if (snapdiff_fh->p_fd >= 0) {
      ceph_close(m_local_mount, snapdiff_fh->p_fd);
    }
    return remotediff_fh->p_fd;
  }

  {
    std::scoped_lock locker(m_lock);
    auto it = m_registered.find(dir_root);
    ceph_assert(it != m_registered.end());
    snapdiff_fh->r_fd_dir_root = it->second->fd;
    remotediff_fh->r_fd_dir_root = it->second->fd;
  }

  dout(0) << ": using "
          << ((snapdiff_fh->p_fd >= 0)
                  ? ("local (previous) snapshot " + (*prev).first)
                  : "remote dir_root")
          << " for incremental transfer" << dendl;
  return 0;
}

// sync the mode of the remote dir_root with that of the local dir_root
int PeerReplayer::sync_perms(const std::string& path) {
  int r = 0;
  struct ceph_statx tstx;

  r = ceph_statx(m_local_mount, path.c_str(), &tstx, CEPH_STATX_MODE,
		 AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to fetch stat for local path: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  r = ceph_chmod(m_remote_mount, path.c_str(), tstx.stx_mode);
  if (r < 0) {
    derr << ": failed to set mode for remote path: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void PeerReplayer::DirSyncPool::activate() {
  {
    std::scoped_lock lock(mtx);
    active = true;
    qlimit = 1 << 15;
  }
  for (int i = 0; i < num_threads; ++i) {
    dir_scanners.emplace_back(new DirScanner());
    dir_scanners[i]->stop_called = false;
    dir_scanners[i]->active = true;
    dir_scanners[i]->worker = std::thread(&DirSyncPool::run, this, dir_scanners[i]);
  }
}

void PeerReplayer::DirSyncPool::drain_queue() {
  {
    std::scoped_lock lock(mtx);
    while (!sync_queue.empty()) {
      auto &task = sync_queue.front();
      task->complete(-1);
      sync_queue.pop();
      queued--;
    }
  }
  pick_cv.notify_all();
}

void PeerReplayer::DirSyncPool::deactivate() {
  std::unique_lock<std::mutex> lock(mtx);
  active = false;
  for (auto& dir_scanner : dir_scanners) {
    dir_scanner->stop_called = true;
  }

  lock.unlock();
  drain_queue();

  for (auto& dir_scanner : dir_scanners) {
    dir_scanner->join();
  }

  lock.lock();

  for (int i = 0; i < dir_scanners.size(); ++i) {
    if (dir_scanners[i]) {
      delete dir_scanners[i];
      dir_scanners[i] = nullptr;
    }
  }
  dir_scanners.clear();
}

void PeerReplayer::DirSyncPool::run(DirScanner *dir_scanner) {
  while (true) {
    SyncMechanism *task;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_cv.wait(lock, [this, &dir_scanner] {
        return (dir_scanner->stop_called || !sync_queue.empty());
      });
      if (dir_scanner->stop_called) {
        dir_scanner->active = false;
        break;
      }
      task = sync_queue.front();
      sync_queue.pop();
      queued--;
    }
    task->complete(0);
  }
}

bool PeerReplayer::DirSyncPool::_try_sync(SyncMechanism *task) {
  {
    std::unique_lock<std::mutex> lock(mtx);
    if (sync_queue.size() >= qlimit) {
      return false;
    }
    task->sync_in_flight();
    sync_queue.emplace(task);
    queued++;
  }
  pick_cv.notify_one();
  return true;
}

bool PeerReplayer::DirSyncPool::try_sync(SyncMechanism *task) {
  bool success = false;
  if (sync_queue.size() < qlimit) {
    success = _try_sync(task);
  }
  return success;
}

void PeerReplayer::DirSyncPool::sync_anyway(SyncMechanism *task) {
  if (!try_sync(task)) {
    sync_direct(task);
  }
}

void PeerReplayer::DirSyncPool::sync_direct(SyncMechanism *task) {
  task->sync_in_flight();
  task->complete(1);
}

void PeerReplayer::DirSyncPool::update_state(int thread_count) {
  std::unique_lock<std::mutex> lock(mtx);
  if (!active) {
    return;
  }
  if (thread_count == num_threads) {
    return;
  }

  if (thread_count < num_threads) {
    for (int i = thread_count; i < num_threads; ++i) {
      dir_scanners[i]->stop_called = true;
      dout(0) << ": Lazy shutdown of thread no " << i
              << " from directory scanning threadpool for " << epath << dendl;
    }
    num_threads = thread_count;
    lock.unlock();
    pick_cv.notify_all();
  } else {
    int i;
    for (i = num_threads; i < dir_scanners.size() && num_threads < thread_count;
         ++i) {
      if (dir_scanners[i]->active) {
        swap(dir_scanners[i], dir_scanners[num_threads]);
        dir_scanners[num_threads++]->stop_called = false;
        dout(0) << ": Reactivating thread no " << i
                << " of directory scanning threadpool for " << epath << dendl;
      }
    }
    for (i = (int)dir_scanners.size() - 1; i >= num_threads; --i) {
      if (num_threads == thread_count && dir_scanners[i]->active) {
        break;
      }
    }
    lock.unlock();
    pick_cv.notify_all();
    if (i < num_threads) {
      for (i = (int)dir_scanners.size() - 1; i >= num_threads; --i) {
        dir_scanners[i]->join();
        dout(0) << ": Force shutdown of already lazy shut thread having "
                   "thread no "
                << i << " from directory scanning threadpool for " << epath
                << dendl;
        delete dir_scanners[i];
        dir_scanners[i] = nullptr;
        dir_scanners.pop_back();
      }
    }

    for (i = num_threads; i < thread_count; ++i) {
      dir_scanners.emplace_back(new DirScanner());
      dir_scanners[i]->stop_called = false;
      dir_scanners[i]->active = true;
      dir_scanners[i]->worker =
          std::thread(&DirSyncPool::run, this, dir_scanners[i]);
      dout(0) << ": Creating thread no " << i
              << " in directory scanning threadpool for " << epath << dendl;
    }
    num_threads = thread_count;
  }
  dout(0) << ": updating number of threads in directory scanner threadpool for "
             "directory="
          << epath << " to" << thread_count << dendl;
}

void PeerReplayer::DirSyncPool::update_qlimit(int _qlimit) {
  std::unique_lock<std::mutex> lock(mtx);
  qlimit = _qlimit;
}

int SyncMechanism::populate_current_stat(const PeerReplayer::FHandles &fh) {
  int r = 0;
  if (!cur_entry->stat_known) { // stat populated
    r = ceph_statxat(m_local, fh.c_fd, cur_entry->epath.c_str(),
                     &cur_entry->stx,
                     CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                         CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to stat cur entry= " << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      derr << ": failed to stat cur entry= " << cur_entry->epath << ", "
           << cur_entry->sync_is_snapdiff() << dendl;
      return r;
    }
    cur_entry->set_stat_known();
  }
  return r;
}

int SyncMechanism::populate_change_mask(const PeerReplayer::FHandles &fh) {
  int r = 0;
  bool purge_remote = cur_entry->purge_remote();
  bool create_fresh = cur_entry->create_fresh();
  struct ceph_statx pstx;
  if (cur_entry->change_mask_populated()) {
    return 0;
  }
  if (!create_fresh && !purge_remote) {
    int pstat_r =
        ceph_statxat(fh.p_mnt, fh.p_fd, cur_entry->epath.c_str(), &pstx,
                     CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                         CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (pstat_r < 0 && pstat_r != -ENOENT && pstat_r != -ENOTDIR) {
      r = pstat_r;
      derr << ": failed to stat prev entry= " << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    purge_remote = pstat_r == 0 && ((cur_entry->stx.stx_mode & S_IFMT) !=
                                    (pstx.stx_mode & S_IFMT));
    create_fresh = (pstat_r < 0);
  }
  replayer->build_change_mask(pstx, cur_entry->stx, create_fresh, purge_remote,
                              cur_entry->change_mask);
  return 0;
}

void DeleteMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = replayer->cleanup_remote_entry(cur_entry->epath, registry, fh, sync_stat,
                                     not_dir);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to cleanup remote entry=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    registry->set_failed(r);
  }
notify_finish:
  registry->dec_sync_indicator();
}

void FileSyncMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = sync_file();
  if (r < 0) {
    registry->set_failed(r);
  }
notify_finish:
  sync_stat->current_stat.dec_file_in_flight_count();
  registry->dec_sync_indicator();
}

int FileSyncMechanism::sync_file() {
  dout(20) << ": epath=" << cur_entry->epath << dendl;
  int r = replayer->copy_to_remote(cur_entry, registry, fh, file_worker);
  if (r < 0) {
    derr << ": failed to copy path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  r = replayer->sync_attributes(cur_entry, fh);
  sync_stat->current_stat.inc_files_synced(true, cur_entry->stx.stx_size);
  return r;
}

void PeerReplayer::build_change_mask(const struct ceph_statx &pstx,
                                     const struct ceph_statx &cstx,
                                     bool create_fresh, bool purge_remote,
                                     unsigned int &change_mask) {
  change_mask |= SyncEntry::CHANGE_MASK_POPULATED;
  if (!S_ISDIR(pstx.stx_mode)) {
    change_mask |= SyncEntry::WASNT_DIR_IN_PREV_SNAPSHOT;
  }
  if (create_fresh || purge_remote) {
    change_mask |= (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID |
                    CEPH_STATX_GID | CEPH_STATX_MTIME);
    if (create_fresh) {
      change_mask |= SyncEntry::CREATE_FRESH;
    }
    if (purge_remote) {
      change_mask |= SyncEntry::PURGE_REMOTE;
    }
    return;
  }

  if ((cstx.stx_mode & ~S_IFMT) != (pstx.stx_mode & ~S_IFMT)) {
    change_mask = change_mask | CEPH_STATX_MODE;
  }
  if (cstx.stx_size != pstx.stx_size) {
    change_mask = change_mask | CEPH_STATX_SIZE;
  }
  if (cstx.stx_uid != pstx.stx_uid) {
    change_mask = change_mask | CEPH_STATX_UID;
  }
  if (cstx.stx_gid != pstx.stx_gid) {
    change_mask = change_mask | CEPH_STATX_GID;
  }
  if (cstx.stx_mtime != pstx.stx_mtime) {
    change_mask = change_mask | CEPH_STATX_MTIME;
  }
}

void DirSyncMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = sync_tree();
  if (r < 0) {
    registry->set_failed(r);
  }
notify_finish:
  registry->dec_sync_indicator();
}

int DirSyncMechanism::sync_tree() {

  int r = 0;
  while (cur_entry && !replayer->should_backoff(registry, &r)) {
    if (cur_entry->needs_remote_sync()) {
      // **--> sync remote
      r = sync_current_entry();
      if (r < 0) {
        break;
      }
    }
    r = go_next();
    if (r < 0) {
      break;
    }
  }
finish:
  finish_sync();
  return r;
}

bool DirSyncMechanism::try_spawning(SyncMechanism *syncm) {
  if (registry->sync_pool->try_sync(syncm)) {
    cur_entry = nullptr;
    return true;
  }
  cur_entry = syncm->move_cur_entry();
  delete syncm;
  return false;
}

int DirSnapDiffSync::go_next() {
  int r = 0;
  while (!m_sync_stack.empty()) {
    std::shared_ptr<PeerReplayer::SyncEntry> &entry = m_sync_stack.top();
    dout(20) << ": top of stack path=" << entry->epath << dendl;
    std::string e_name;
    ceph_snapdiff_entry_t sd_entry;
    while (true) {
      if (replayer->should_backoff(registry, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        return r;
      }
      r = ceph_readdir_snapdiff(&entry->info, &sd_entry);
      if (r < 0) {
        derr << ": failed to read directory=" << entry->epath << dendl;
        return r;
      }
      if (r == 0) {
        break;
      }
      std::string d_name = sd_entry.dir_entry.d_name;
      if (d_name == "." || d_name == "..") {
        continue;
      }
      e_name = d_name;
      break;
    }

    std::string epath = entry_path(entry->epath, e_name);
    if (entry->deleted_sibling_exist &&
        entry->deleted_sibling.snapid == (*fh.m_prev).second) {
      std::string deleted_sibling_d_name =
          entry->deleted_sibling.dir_entry.d_name;
      if (r == 0 || sd_entry.snapid == (*fh.m_prev).second ||
          e_name != deleted_sibling_d_name) {
        // prev sibling is a deleted entry, we need to delete it now.
        std::shared_ptr<PeerReplayer::SyncEntry> deleted_entry =
            std::make_shared<PeerReplayer::SyncEntry>(
                entry_path(entry->epath, deleted_sibling_d_name));
        SyncMechanism *syncm = new DeleteMechanism(
            m_local, std::move(deleted_entry), registry, sync_stat, fh,
            replayer, (DT_DIR != entry->deleted_sibling.dir_entry.d_type));
        registry->sync_pool->sync_anyway(syncm);
        entry->deleted_sibling_exist = false;
      }
    }

    if (r == 0) {
      dout(10) << ": done for directory=" << entry->epath << dendl;
      if (!(m_sync_stack.size() == 1 && entry->epath == ".")) {
        if (ceph_close_snapdiff(&entry->info) < 0) {
          derr << ": failed to close directory=" << entry->epath << dendl;
        }
      }
      m_sync_stack.pop();
      continue;
    }

    if (sd_entry.snapid == (*fh.m_prev).second) {
      entry->deleted_sibling = sd_entry;
      entry->deleted_sibling_exist = true;
      continue;
    }

    struct ceph_statx cstx;
    cur_entry = std::make_shared<PeerReplayer::SyncEntry>(
        epath, ceph_snapdiff_info(), cstx, 0);
    if (entry->deleted_sibling_exist &&
        entry->deleted_sibling.snapid == (*fh.m_prev).second &&
        sd_entry.snapid == fh.m_current.second &&
        std::string(entry->deleted_sibling.dir_entry.d_name) == e_name) {
      // this is an entry where previous need to be purged
      cur_entry->change_mask |= PeerReplayer::SyncEntry::PURGE_REMOTE;
      entry->deleted_sibling_exist = false;
    }

    SyncMechanism *syncm = new DirSnapDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat, fh, replayer);
    if (try_spawning(syncm)) {
      continue;
    }
    r = 0;
    break;
  }
  return r;
}

int DirSnapDiffSync::sync_current_entry() {
  int r = 0;
  bool failed_prev =
      (sync_stat->synced_snap_count == 0 && sync_stat->nr_failures > 0);
  /*
  Our goal here, when this is a fresh syncing we will go for DirSnapDiffSync.
  When we already know that previous sync failed, then our goal is to, for the
  unchanged part(same as previous snapshot) we will scan through DirSnapDiffSync
  for other parts we will scan through DirBruteDiffSync comparing with the
  remote state(more specifically keeping remote as diff base). With this
  strategy we will get rid of transferring already transferred file for a failed
  scenario. Also, we are traversing the unchanged part(between previous and
  current snapshot) of the tree using snapdiff.

  Scenario when previous sync failed:
  1. For file, we completely skipped file blockdiff strategy and based on
    remote we synced the file. Which will eventually save us from already
    synced file.
  2. For directory, we used local previous snapshot as diff base for
    change mask building to find out whether is a newly created entry
    (or previous snapshot has same entry with different type). Some cases are
    give below,
    * If directory doesn't exist in prev snapshot, it might be possible
      that remote state has some patial subtree under that directory. So
      we let DirBruteDiffSync class to handle by using remote diff base
      by sending registry->remotediff_fh.
    * Same case for when entry type is different in previous snapshot
    * If both is not the case, then we went for snapdiff optimized strategy.

  Scenario when previous sync didn't fail:
    For every new entry or entry need to be purged in remote before creating
    We let DirBruteDiffSync class handle this matter as we know snapdiff
    has no impact here. And as remote state is not corrupted, we let
  DirBruteDiffSync class to use local previous snapshot as diff base.
  */

  r = populate_current_stat(fh);
  if (r < 0) {
    return r;
  }

  r = populate_change_mask(fh);
  if (r < 0) {
    return r;
  }

  if (cur_entry->create_fresh() || cur_entry->purge_remote()) {
    SyncMechanism *syncm = nullptr;
    if (failed_prev) {
      cur_entry->reset_change_mask();
      cur_entry->set_is_snapdiff(false);
    }
    syncm = new DirBruteDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat,
        failed_prev ? registry->remotediff_fh : fh, replayer);
    registry->sync_pool->sync_direct(syncm);
    return 0;
  }

  if (S_ISDIR(cur_entry->stx.stx_mode)) { // is a directory
    r = replayer->remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
    sync_stat->current_stat.inc_dir_scanned_count();
  } else {
    r = replayer->remote_file_op(cur_entry, registry, sync_stat, fh);
    cur_entry = nullptr;
    return r;
  }
  r = ceph_open_snapdiff(fh.p_mnt, registry->dir_root.c_str(),
                         cur_entry->epath.c_str(), (*fh.m_prev).first.c_str(),
                         fh.m_current.first.c_str(), &cur_entry->info);
  if (r != 0) {
    derr << ": failed to open snapdiff, entry=" << cur_entry->epath
         << ", r=" << r << dendl;
    return r;
  }
  cur_entry->set_remote_synced();
  m_sync_stack.emplace(std::move(cur_entry));
  cur_entry = nullptr;
  return 0;
}

void DirSnapDiffSync::finish_sync() {
  dout(20) << dendl;

  while (!m_sync_stack.empty()) {
    auto &entry = m_sync_stack.top();
    if (entry->is_directory() &&
        !(m_sync_stack.size() == 1 && entry->epath == ".")) {
      dout(20) << ": closing local directory=" << entry->epath << dendl;
      if (ceph_close_snapdiff(&(entry->info)) < 0) {
        derr << ": failed to close snapdiff directory=" << entry->epath
             << dendl;
      }
    }

    m_sync_stack.pop();
  }
}

int DirBruteDiffSync::go_next() {
  int r = 0;
  while (!m_sync_stack.empty()) {
    std::shared_ptr<PeerReplayer::SyncEntry> &entry = m_sync_stack.top();
    dout(20) << ": top of stack path=" << entry->epath << dendl;
    std::string e_name;
    struct dirent de;
    struct ceph_statx stx;
    while (true) {
      if (replayer->should_backoff(registry, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        return r;
      }
      r = ceph_readdirplus_r(m_local, entry->dirp, &de, &stx,
                             CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                                 CEPH_STATX_SIZE | CEPH_STATX_ATIME |
                                 CEPH_STATX_MTIME,
                             AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
      if (r < 0) {
        derr << ": failed to local read directory=" << entry->epath << dendl;
        return r;
      }
      if (r == 0) {
        break;
      }
      auto d_name = std::string(de.d_name);
      if (d_name != "." && d_name != "..") {
        e_name = d_name;
        break;
      }
    }
    if (r == 0) {
      dout(10) << ": done for directory=" << entry->epath << dendl;
      if (!(m_sync_stack.size() == 1 && entry->epath == ".")) {
        if (entry->dirp && ceph_closedir(m_local, entry->dirp) < 0) {
          derr << ": failed to close local directory=" << entry->epath << dendl;
        }
      }
      m_sync_stack.pop();
      change_mask_map_size -= (int)m_change_mask_map_stack.top().size();
      m_change_mask_map_stack.pop();
      continue;
    }
    std::shared_ptr<PeerReplayer::SyncEntry> &par_entry = m_sync_stack.top();
    auto &par_change_mask_map = m_change_mask_map_stack.top();
    uint64_t change_mask = 0;
    bool create_fresh = par_entry->create_fresh() || par_entry->purge_remote();

    if (!create_fresh) {
      auto it = par_change_mask_map.find(e_name);
      /*
        found in the map so, we don't need to do extra stat in prev
        snapshot or remote.
      */
      if (it != par_change_mask_map.end()) {
        change_mask = it->second;
        // we already take the change mask and deleting it from memory, to
        // insert more entry
        par_change_mask_map.erase(it);
        change_mask_map_size--;
        sync_stat->current_stat.inc_cache_hit();
      }
    } else {
      change_mask |= PeerReplayer::SyncEntry::CREATE_FRESH;
    }
    cur_entry = std::make_shared<PeerReplayer::SyncEntry>(
        entry_path(par_entry->epath, e_name), nullptr, stx, change_mask,
        par_entry->sync_is_snapdiff());

    // this will help us to avoid doing stat again in current snapshot
    // in populate_current_stat function.
    cur_entry->set_stat_known();
    SyncMechanism *syncm = new DirBruteDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat, fh, replayer);
    if (try_spawning(syncm)) {
      continue;
    }
    r = 0;
    break;
  }
  return r;
}

int DirBruteDiffSync::sync_current_entry() {
  bool failed_prev =
      (sync_stat->synced_snap_count == 0 || sync_stat->nr_failures > 0);
  int r = 0, rem_r = 0;
  r = populate_current_stat(fh);
  if (r < 0) {
    return r;
  }

  r = populate_change_mask(fh);
  if (r < 0) {
    return r;
  }

  if (failed_prev && fh.p_mnt == m_local &&
      (cur_entry->create_fresh() || cur_entry->purge_remote())) {
    // dout(0) << ": remote fix-->" << cur_entry->epath << dendl;
    cur_entry->reset_change_mask();
    cur_entry->set_is_snapdiff(false);
    SyncMechanism *syncm =
        new DirBruteDiffSync(m_local, std::move(cur_entry), registry, sync_stat,
                             registry->remotediff_fh, replayer);
    registry->sync_pool->sync_direct(syncm);
    return 0;
  }

  if (cur_entry->purge_remote()) {
    rem_r = replayer->cleanup_remote_entry(
        cur_entry->epath, registry, fh, sync_stat,
        cur_entry->wasnt_dir_in_prev_snapshot());
    if (rem_r < 0 && rem_r != -ENOENT) {
      derr << ": failed to cleanup remote entry=" << cur_entry->epath << ": "
           << cpp_strerror(rem_r) << dendl;
      r = rem_r;
      return r;
    }
  }
  if (S_ISDIR(cur_entry->stx.stx_mode)) {
    r = replayer->remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
    sync_stat->current_stat.inc_dir_scanned_count();
  } else {
    r = replayer->remote_file_op(cur_entry, registry, sync_stat, fh);
    cur_entry = nullptr;
    return r;
  }
  r = opendirat(m_local, fh.c_fd, cur_entry->epath, AT_SYMLINK_NOFOLLOW,
                &cur_entry->dirp);
  if (r < 0) {
    derr << ": failed to open local directory=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  std::unordered_map<std::string, unsigned int> change_mask_map;

  /*
    when cur_entry is fresh or entry in previous snapshot need to
    be purged, then we don't need unnecessary propagate_deleted_entries
    Saving some api calls.
  */

  if (!cur_entry->create_fresh() && !cur_entry->purge_remote()) {
    r = replayer->propagate_deleted_entries(cur_entry->epath, registry,
                                            sync_stat, fh, change_mask_map,
                                            change_mask_map_size);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to propagate missing dirs: " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  cur_entry->set_remote_synced();
  m_sync_stack.emplace(std::move(cur_entry));
  m_change_mask_map_stack.emplace(std::move(change_mask_map));
  return 0;
}

void DirBruteDiffSync::finish_sync() {
  dout(20) << dendl;

  while (!m_sync_stack.empty()) {
    auto &entry = m_sync_stack.top();
    if (entry && entry->is_directory() &&
        !(m_sync_stack.size() == 1 && entry->epath == ".")) {
      dout(20) << ": closing local directory=" << entry->epath << dendl;
      if (ceph_closedir(m_local, entry->dirp) < 0) {
        derr << ": failed to close local directory=" << entry->epath << dendl;
      }
    }

    m_sync_stack.pop();
  }
}

int PeerReplayer::do_synchronize(const std::string &dir_root, const Snapshot &current,
                                 boost::optional<Snapshot> prev) {
  dout(0)
      << "cephfs_mirror_file_sync_thread="
      << g_ceph_context->_conf.get_val<uint64_t>(
             "cephfs_mirror_file_sync_thread")
      << ", cephfs_mirror_dir_scanning_thread=" << dir_scanning_thread
      << ", cephfs_mirror_remote_diff_base_upon_start="
      << remote_diff_base_upon_start
      << ", cephfs_mirror_sync_latest_snapshot=" << sync_latest_snapshot
      << ", cephfs_mirror_thread_pool_queue_size=" << thread_pool_queue_size
      << ", cephfs_mirror_max_element_in_cache_per_thread="
      << max_element_in_cache_per_thread
      << ", cephfs_mirror_start_syncing=" << start_syncing
      << ", cephfs_mirror_max_concurrent_directory_syncs="
      << max_concurrent_directory_syncs
      << ", cephfs_mirror_use_snapdiff_api=" << use_snapdiff_api
      << ", mds_session_blocklist_on_timeout="
      << g_ceph_context->_conf.get_val<bool>("mds_session_blocklist_on_timeout")
      << ", mds_session_blocklist_on_evict="
      << g_ceph_context->_conf.get_val<bool>("mds_session_blocklist_on_evict")
      << ", client_reconnect_stale="
      << g_ceph_context->_conf.get_val<bool>("client_reconnect_stale")
      << ", client_tick_interval="
      << g_ceph_context->_conf.get_val<std::chrono::seconds>(
             "client_tick_interval")
      << ", client_cache_size=" << g_ceph_context->_conf->client_cache_size
      << ", cephfs_mirror_max_consecutive_failures_before_remote_sync= "
      << max_consecutive_failures_before_remote_sync << dendl;

  dout(0) << ": sync start-->" << dir_root << ",cur_snap= " << current.first
          << ", diff_base=" << (prev ? (*prev).first : "remote") << dendl;
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  std::unique_lock<ceph::mutex> lock(m_lock);
  DirRegistry *registry = m_registered.at(dir_root);
  auto sync_stat = m_snap_sync_stats.at(dir_root);
  lock.unlock();
  int r = pre_sync_check_and_open_handles(dir_root, current, prev,
                                          &registry->snapdiff_fh,
                                          &registry->remotediff_fh);
  if (r < 0) {
    dout(5) << ": cannot proceed with sync: " << cpp_strerror(r) << dendl;
    return r;
  }
  registry->dir_root = dir_root, registry->failed = registry->canceled = false;
  registry->failed_reason = 0;
  FHandles *fh;
  if (registry->snapdiff_fh.p_fd >= 0) {
    fh = &registry->snapdiff_fh;
  } else {
    fh = &registry->remotediff_fh;
  }

  auto close_p_fd = [&registry]() {
    if (registry->snapdiff_fh.p_fd >= 0) {
      ceph_close(registry->snapdiff_fh.p_mnt, registry->snapdiff_fh.p_fd);
    }
    if (registry->remotediff_fh.p_fd >= 0) {
      ceph_close(registry->remotediff_fh.p_mnt, registry->remotediff_fh.p_fd);
    }
  };

  if (registry->snapdiff_fh.p_fd < 0) {
    r = set_snap_id_attr(dir_root, "ceph.mirror.diff_base", 0);
    if (r < 0) {
      ceph_close(m_local_mount, fh->c_fd);
      close_p_fd();
      return r;
    }
  }

  struct ceph_statx stx;
  r = ceph_fstatx(m_local_mount, fh->c_fd, &stx,
                  CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                      CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                  AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    ceph_close(m_local_mount, fh->c_fd);
    close_p_fd();
    return r;
  }

  std::string cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  void *buf_file_count = malloc(20);
  std::string rfiles = "";
  int x1 = ceph_getxattr(m_local_mount, cur_snap_path.c_str(),
                         "ceph.dir.rfiles", buf_file_count, 20);
  if (x1 < 0) {
    derr << ": failed to read ceph.dir.rfiles xattr for directory=" << dir_root
         << dendl;
  } else {
    rfiles = std::move(std::string((char *)buf_file_count, x1));
    dout(0) << ": xattr ceph.dir.rfiles=" << rfiles
            << " for directory=" << dir_root << dendl;
  }

  void *buf_file_bytes = malloc(20);
  std::string rbytes = "";
  int x2 = ceph_getxattr(m_local_mount, cur_snap_path.c_str(),
                         "ceph.dir.rbytes", buf_file_bytes, 20);
  if (x2 < 0) {
    derr << ": failed to read ceph.dir.rfiles xattr for directory=" << dir_root
         << dendl;
  } else {
    rbytes = std::move(std::string((char *)buf_file_bytes, x2));
    dout(0) << ": xattr ceph.dir.rbytes=" << rbytes
            << " for directory=" << dir_root << dendl;
  }

  SyncMechanism *syncm;
  std::shared_ptr <SyncEntry> root_entry;

  lock.lock();
  dout(0) << ": Number of dir scanning threads=" << dir_scanning_thread
          << dendl;
  registry->sync_start(file_mirror_pool.sync_start(dir_root));
  sync_stat->current_stat.rfiles = std::stoull(rfiles);
  sync_stat->current_stat.rbytes = std::stoull(rbytes);
  sync_stat->current_stat.start_timer();
  registry->sync_pool =
      std::make_unique<DirSyncPool>(dir_scanning_thread, dir_root, m_peer);
  registry->sync_pool->activate();
  lock.unlock();

  if (registry->snapdiff_fh.p_fd >= 0) {
    // dout(0) << ": snapdiff-->" << dendl;
    sync_stat->current_stat.set_diff_base("local_" + (*prev).first);
    std::shared_ptr<SyncEntry> cur_entry;
    if (use_snapdiff_api) {
      cur_entry =
          std::make_shared<SyncEntry>(".", ceph_snapdiff_info(), stx, 0);
    } else {
      cur_entry = std::make_shared<SyncEntry>(".", nullptr, stx, 0, true);
    }
    cur_entry->set_stat_known();
    root_entry = cur_entry;

    if (use_snapdiff_api) {
      dout(0) << ": started syncing of dir_root=" << dir_root << ", using snapdiff api" << dendl;
      syncm = new DirSnapDiffSync(m_local_mount, std::move(cur_entry), registry,
                                  sync_stat, registry->snapdiff_fh, this);
    } else {
      dout(0) << ": started syncing of dir_root=" << dir_root
              << ", using snapdiff legacy calculation" << dendl;
      syncm =
          new DirBruteDiffSync(m_local_mount, std::move(cur_entry), registry,
                               sync_stat, registry->snapdiff_fh, this);
    }
  } else {
    dout(0) << ": started syncing of dir_root=" << dir_root
            << ", using remote as diff base" << dendl;
    sync_stat->current_stat.set_diff_base("remote");
    std::shared_ptr<SyncEntry> cur_entry =
        std::make_shared<SyncEntry>(".", nullptr, stx, 0, false);
    cur_entry->set_stat_known();
    root_entry = cur_entry;
    syncm = new DirBruteDiffSync(m_local_mount, std::move(cur_entry), registry,
                                 sync_stat, registry->remotediff_fh, this);
  }
  registry->sync_pool->sync_anyway(syncm);
  registry->wait_for_sync_finish();
  if (!prev || registry->snapdiff_fh.p_fd < 0 || !use_snapdiff_api) {
    if (root_entry->dirp &&
        ceph_closedir(m_local_mount, root_entry->dirp) < 0) {
      derr << ": failed to close local directory=." << dendl;
    }
  } else if (ceph_close_snapdiff(&root_entry->info) < 0) {
    derr << ": failed to close local directory=." << dendl;
  }

  should_backoff(registry, &r);
  if (registry->failed) {
    r = registry->failed_reason;
  }

  lock.lock();
  file_mirror_pool.sync_finish(registry->get_file_sync_queue_idx(), dir_root);
  registry->file_sync_queue_idx = -2;
  registry->sync_pool->deactivate();
  registry->sync_pool = nullptr;
  sync_stat->reset_stats(r);
  lock.unlock();

  dout(20) << " cur:" << fh->c_fd
           << " prev:" << fh->p_fd
           << " ret = " << r
           << dendl;

  // @FHandles.r_fd_dir_root is closed in @unregister_directory since
  // its used to acquire an exclusive lock on remote dir_root.

  // c_fd has been used in ceph_fdopendir call so
  // there is no need to close this fd manually.
  ceph_close(m_local_mount, fh->c_fd);
  close_p_fd();
  dout(0) << ": sync done-->" << dir_root << ", " << current.first << dendl;

  return r;
}

int PeerReplayer::synchronize(const std::string &dir_root, const Snapshot &current,
                              boost::optional<Snapshot> prev) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  int r = 0;

  if (r < 0) {
    r = do_synchronize(dir_root, current, boost::none);
  } else {
    r = do_synchronize(dir_root, current, prev);
  }

  // snap sync failed -- bail out!
  if (r < 0) {
    return r;
  }

  auto cur_snap_id_str{stringify(current.second)};
  snap_metadata snap_meta[] = {{PRIMARY_SNAP_ID_KEY.c_str(), cur_snap_id_str.c_str()}};
  r = ceph_mksnap(m_remote_mount, dir_root.c_str(), current.first.c_str(), 0755,
                  snap_meta, sizeof(snap_meta)/sizeof(snap_metadata));
  if (r < 0) {
    derr << ": failed to snap remote directory dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
  }
  r = set_snap_id_attr(dir_root, "ceph.mirror.diff_base", current.second);
  if (r < 0) {
    return r;
  }

  r = set_snap_id_attr(dir_root, "ceph.mirror.dirty_snap_id", 0);
  if (r < 0) {
    return r;
  }

  return r;
}

int64_t PeerReplayer::get_snap_id_attr(const std::string &dir_root,
                                       const std::string &attr) {
  int r =
      ceph_getxattr(m_remote_mount, dir_root.c_str(), attr.c_str(), nullptr, 0);
  if (r < 0 && r != -ENODATA) {
    derr << ": failed to fetch last_snap_id length from dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
  }
  if (r < 0) {
    return 0;
  }
  size_t xlen = r;
  char *val = (char *)alloca(xlen + 1);
  r = ceph_getxattr(m_remote_mount, dir_root.c_str(), attr.c_str(), (void *)val,
                    xlen);
  if (r < 0) {
    derr << ": failed to fetch '" << attr << "' for dir_root: " << dir_root
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  val[xlen] = '\0';
  return atoll(val);
}

int PeerReplayer::set_snap_id_attr(const std::string &dir_root,
                                   const std::string &attr, uint64_t snap_id) {
  auto snap_id_str{stringify(snap_id)};
  int r = ceph_setxattr(m_remote_mount, dir_root.c_str(), attr.c_str(),
                        snap_id_str.c_str(), snap_id_str.size(), 0);
  if (r < 0) {
    derr << ": error setting '" << attr << "' on dir_root=" << dir_root << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void PeerReplayer::sync_directory(
    std::string dir_root, std::map<uint64_t, std::string> snaps_deleted,
    std::map<uint64_t, std::pair<std::string, std::string>> snaps_renamed,
    Snapshot last_snap, Snapshot cur_snap) {
  // start mirroring snapshots from the last snap-id synchronized

  int r = propagate_snap_deletes(dir_root, snaps_deleted);
  if (r < 0) {
    derr << ": failed to propgate deleted snapshots" << dendl;
    goto sanity_check;
  }

  r = propagate_snap_renames(dir_root, snaps_renamed);
  if (r < 0) {
    derr << ": failed to propgate renamed snapshots" << dendl;
    goto sanity_check;
  }

  if (last_snap.second != cur_snap.second) {
    dout(10) << ": synchronizing snap-id=" << cur_snap.second
             << "of directory=" << dir_root << dendl;
    dout(0) << ": cur_snap.first=" << cur_snap.first << ", "
            << "cur_snap.second=" << cur_snap.second
            << ", last_snap.first=" << last_snap.first
            << ", last_snap.second=" << last_snap.second << dendl;
    r = _do_sync_snaps(dir_root, cur_snap, last_snap);    
  }


sanity_check:
  std::unique_lock locker(m_lock);
  if (r < 0) {
    _inc_failed_count(dir_root);
  } else {
    _reset_failed_count(dir_root);
  }
  unregister_directory(dir_root);
  ++nr_replayers;
  m_cond.notify_one();
}

int PeerReplayer::get_candidate_snap(
    const std::string &dir_root,
    std::map<uint64_t, std::string> &local_snap_map,
    std::map<uint64_t, std::string> &remote_snap_map, SnapshotPair &snap_pair) {

  bool remote_diff_base = false;

  std::unique_lock locker(m_lock);
  auto& dir_sync_stat = m_snap_sync_stats.at(dir_root);
  if ((remote_diff_base_upon_start && dir_sync_stat->synced_snap_count == 0) ||
      dir_sync_stat->nr_failures >=
          max_consecutive_failures_before_remote_sync) {
    remote_diff_base = true;
  }
  locker.unlock();

  int r = 0;
  Snapshot& last_snap = snap_pair.first;
  Snapshot& cur_snap = snap_pair.second;

  last_snap = {"", 0};
  cur_snap = {"", 0};
  uint64_t diff_base = 0;
  uint64_t dirty_snap_id = 0;
  std::map<uint64_t, std::string>::iterator it = local_snap_map.end();

  if (!remote_diff_base) {
    r = get_snap_id_attr(dir_root, "ceph.mirror.diff_base");
    if (r < 0) {
      return r;
    }
    diff_base = r;
    if (diff_base > 0 && !remote_snap_map.empty() &&
        remote_snap_map.rbegin()->first > diff_base) {
      diff_base = remote_snap_map.rbegin()->first;
      r = set_snap_id_attr(dir_root, "ceph.mirror.diff_base", diff_base);
      if (r < 0) {
        return r;
      }
    } else if (diff_base > 0) {
      dout(4) << ": diff_base_snap_id=" << diff_base << " got deleted for "
              << dir_root << dendl;
    }
    auto last = remote_snap_map.find(diff_base);
    if (diff_base > 0 && last != remote_snap_map.end()) {
      r = get_snap_id_attr(dir_root, "ceph.mirror.dirty_snap_id");
      if (r < 0) {
        return r;
      }
      dirty_snap_id = r;
      if (dirty_snap_id == diff_base) {
        dout(4) << ": ceph.mirror.dirty_snap_id is equal to "
                   "ceph.mirror.diff_base for directory="
                << dir_root << dendl;
        r = set_snap_id_attr(dir_root, "ceph.mirror.dirty_snap_id", 0);
        if (r < 0) {
          return r;
        }
        dirty_snap_id = 0;
      }
      if (dirty_snap_id > 0) {
        it = local_snap_map.find(dirty_snap_id);
        if (it != local_snap_map.end()) {
          last_snap = {last->second, last->first};
        }
      } else {
        last_snap = {last->second, last->first};
      }
    }
  }

  if (sync_latest_snapshot) {
    if (local_snap_map.empty()) {
      dout(20) << ": nothing to synchronize" << dendl;
      last_snap = cur_snap = {"", 0};
      return 0;
    }
    if (it == local_snap_map.end()) {
      it = prev(local_snap_map.end());
    }
    cur_snap = {it->second, it->first};
  } else {
    if (it == local_snap_map.end()) {
      it = remote_snap_map.empty()
               ? local_snap_map.begin()
               : local_snap_map.upper_bound(remote_snap_map.rbegin()->first);
    }

    if (it != local_snap_map.end()) {
      cur_snap = {it->second, it->first};
    } else {
      last_snap = cur_snap = {"", 0};
    }
  }

  if (!remote_snap_map.empty() &&
      cur_snap.second == remote_snap_map.rbegin()->first) {
    last_snap = cur_snap = {"", 0};
  }

  if (last_snap.second != cur_snap.second) {
    dout(0) << ": diff_base=" << diff_base
            << ", last_snap_id=" << last_snap.second
            << ", dirty_snap_id=" << dirty_snap_id
            << ", cur_snap_id=" << cur_snap.second << dendl;
  }
  return 0;
}

int PeerReplayer::do_sync_snaps(const std::string &dir_root,
                                bool *nothing_to_sync) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  std::map<uint64_t, std::string> local_snap_map;
  std::map<uint64_t, std::string> remote_snap_map;

  int r = build_snap_map(dir_root, &local_snap_map);
  if (r < 0) {
    derr << ": failed to build local snap map" << dendl;
    return r;
  }

  r = build_snap_map(dir_root, &remote_snap_map, true);
  if (r < 0) {
    derr << ": failed to build remote snap map" << dendl;
    return r;
  }

  // infer deleted and renamed snapshots from local and remote
  // snap maps
  std::map<uint64_t, std::string> snaps_deleted;
  std::map<uint64_t, std::pair<std::string, std::string>> snaps_renamed;
  for (auto &[primary_snap_id, snap_name] : remote_snap_map) {
    auto it = local_snap_map.find(primary_snap_id);
    if (it == local_snap_map.end()) {
      snaps_deleted.emplace(primary_snap_id, snap_name);
    } else if (it->second != snap_name) {
      snaps_renamed.emplace(primary_snap_id,
                            std::make_pair(snap_name, it->second));
    }
  }

  for (auto& [snap_id, _]: snaps_deleted) {
    auto it = remote_snap_map.find(snap_id);
    if (it != remote_snap_map.end()) {
      remote_snap_map.erase(it);
    }
  }

  for (auto &[snap_id, data] : snaps_renamed) {
    auto it = remote_snap_map.find(snap_id);
    if (it != remote_snap_map.end()) {
      it->second = data.second;
    }
  }

  bool need_thread = false;

  if (!snaps_deleted.empty() || !snaps_renamed.empty()) {
    need_thread = true;
  }

  if (!remote_snap_map.empty()) {
    auto last_synced_snap = remote_snap_map.rbegin();
    set_last_synced_snap(dir_root, last_synced_snap->first,
                         last_synced_snap->second);
  }

  SnapshotPair snap_pair;
  r = get_candidate_snap(dir_root, local_snap_map, remote_snap_map, snap_pair);
  if (r < 0) {
    return r;
  }

  auto [last_snap, cur_snap] = snap_pair;
  if (last_snap.second != cur_snap.second) {
    need_thread = true;
  }

  if (need_thread) {
    {
      std::scoped_lock locker(m_lock);
      --nr_replayers;
    }
    {
      std::scoped_lock thread_map_locker(thread_map_lock);
      thread_map[dir_root] = std::make_unique<std::thread>(
          &PeerReplayer::sync_directory, this, dir_root, snaps_deleted,
          snaps_renamed, last_snap, cur_snap);
      dout(5) << ": spawning thread for synchronization of directory="
              << dir_root << dendl;
    }
    *nothing_to_sync = false;
  } else {
    dout(20) << ": nothing to synchronize" << dendl;
  }

  return 0;
}

int PeerReplayer::_do_sync_snaps(const std::string &dir_root, Snapshot cur_snap,
                                 Snapshot last_snap) {
  int r = 0;
  double start = 0, end = 0, duration = 0;
  if (m_perf_counters) {
    start = std::chrono::duration_cast<std::chrono::seconds>(
                clock::now().time_since_epoch())
                .count();
    utime_t t;
    t.set_from_double(start);
    m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_start, t);
  }
  set_current_syncing_snap(dir_root, cur_snap.second, cur_snap.first);
  boost::optional<Snapshot> prev = boost::none;
  if (last_snap.second != 0) {
    prev = last_snap;
  }

  r = set_snap_id_attr(dir_root, "ceph.mirror.diff_base", last_snap.second);
  if (r < 0) {
    return r;
  }

  r = set_snap_id_attr(dir_root, "ceph.mirror.dirty_snap_id", cur_snap.second);
  if (r < 0) {
    return r;
  }

  r = synchronize(dir_root, cur_snap, prev);
  if (r < 0) {
    derr << ": failed to synchronize dir_root=" << dir_root
         << ", snapshot=" << cur_snap.first << dendl;
    clear_current_syncing_snap(dir_root);
    return r;
  }
  if (m_perf_counters) {
    m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_synced);
    end = std::chrono::duration_cast<std::chrono::seconds>(
              clock::now().time_since_epoch())
              .count();
    utime_t t;
    t.set_from_double(end);
    m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_end, t);
    duration = end - start;
    t.set_from_double(duration);
    m_perf_counters->tinc(l_cephfs_mirror_peer_replayer_avg_sync_time, t);
    m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_duration,
                          t);
    m_perf_counters->set(l_cephfs_mirror_peer_replayer_last_synced_bytes,
                         m_snap_sync_stats.at(dir_root)->sync_bytes);
  }

  set_last_synced_stat(dir_root, cur_snap.second, cur_snap.first, duration);
  return r;
}

int PeerReplayer::sync_snaps(const std::string &dir_root,
                             std::unique_lock<ceph::mutex> &locker) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  locker.unlock();
  bool nothing_to_sync = true;
  int r = do_sync_snaps(dir_root, &nothing_to_sync);
  locker.lock();
  if (r < 0) {
    derr << ": failed to sync snapshots for dir_root=" << dir_root << dendl;
    _inc_failed_count(dir_root);
    unregister_directory(dir_root);
  } else if (nothing_to_sync) {
    unregister_directory(dir_root);
  }
  return r;
}

void PeerReplayer::cleanup_idle_threads(std::unique_lock<ceph::mutex> &locker) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_deleted_directories.empty()) {
    return;
  }
  std::vector<std::string> deleted;
  swap(deleted, m_deleted_directories);


  locker.unlock();

  {
    std::scoped_lock thread_map_locker(thread_map_lock);
    for (auto &dir : deleted) {
      auto it = thread_map.find(dir);
      if (it == thread_map.end()) {
        continue;
      }
      it->second->join();
      thread_map.erase(it);
    }
    dout(0) << ": Idle threads joined" << dendl;
  }
  locker.lock();
}

void PeerReplayer::run_scan() {
  dout(10) << ": scanning thread started" << dendl;

  std::unique_lock locker(m_lock);
  monotime last_scan = clock::zero();
  std::string next_dir = "";
  int idx = 0;
  if (idx < m_directories.size()) {
    next_dir = m_directories[idx];
  }

  while (true) {
    m_cond.wait(locker, [this] {
      return is_stopping() || (start_syncing && nr_replayers > 0 &&
                               m_directories.size() > m_registered.size() &&
                               !m_directories.empty());
    });

    cleanup_idle_threads(locker);

    auto scan_interval = g_ceph_context->_conf.get_val<uint64_t>(
      "cephfs_mirror_directory_scan_interval");
    auto retry_interval = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_retry_failed_directories_interval");

    std::chrono::duration<double> interval = clock::now() - last_scan;
    if (interval.count() < scan_interval - 1.0) {

      auto timeout = std::chrono::seconds(static_cast<int64_t>(
          std::max(scan_interval - interval.count(), 1.0)));
      m_cond.wait_for(locker, timeout,
                      [this] { return is_stopping(); });
    }

    if (is_stopping()) {
      dout(5) << ": exiting" << dendl;
      break;
    }
    if (!(start_syncing && nr_replayers > 0 &&
          m_directories.size() > m_registered.size() &&
          !m_directories.empty())) {
      last_scan = clock::now();
      continue;
    }

    locker.unlock();

    if (m_fs_mirror->is_blocklisted()) {
      dout(5) << ": exiting as client is blocklisted" << dendl;
      break;
    }

    locker.lock();

    if (idx >= m_directories.size() || m_directories[idx] != next_dir) {
      idx = std::find(m_directories.begin(), m_directories.end(), next_dir) -
            m_directories.begin();
      if (idx >= m_directories.size()) {
        idx = (int)m_directories.size() - 1;
      }
    }

    for (int i = 0; i < m_directories.size() && nr_replayers > 0 &&
                    m_directories.size() > m_registered.size();
         ++i, idx = (idx + 1) % m_directories.size()) {
      next_dir = m_directories[idx];
      auto &sync_stat = m_snap_sync_stats.at(next_dir);
      if (sync_stat->failed) {
        std::chrono::duration<double> d =
            clock::now() - *sync_stat->last_failed;
        if (d.count() < retry_interval) {
          continue;
        }
      }

      if (m_registered.count(next_dir)) {
        continue;
      }


      locker.unlock();
      {
        std::scoped_lock thread_map_locker(thread_map_lock);
        auto thread_it = thread_map.find(next_dir);
        if (thread_it != thread_map.end()) {
          thread_it->second->join();
          thread_map.erase(thread_it);
        }
      }
      locker.lock();

      int r = register_directory(next_dir);
      if (r == 0) {
        r = sync_perms(next_dir);
        if (r == 0) {
          r = sync_snaps(next_dir, locker);
          if (r < 0 && m_perf_counters) {
            m_perf_counters->inc(
                l_cephfs_mirror_peer_replayer_snap_sync_failures);
          }
        } else {
          _inc_failed_count(next_dir);
          if (m_perf_counters) {
            m_perf_counters->inc(
                l_cephfs_mirror_peer_replayer_snap_sync_failures);
          }
          unregister_directory(next_dir);
        }
      }
    }
    next_dir = m_directories[idx];
    last_scan = clock::now();
  }
}

void PeerReplayer::peer_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("stats");
  for (auto &[dir_root, sync_stat] : m_snap_sync_stats) {
    f->open_object_section(dir_root);
    sync_stat->dump_stats(f);
    if (!sync_stat->failed && sync_stat->current_syncing_snap) {
      auto &dir_sync_pool = m_registered.at(dir_root)->sync_pool;
      if (dir_sync_pool) {
        dir_sync_pool->dump_stats(f);
      }
    }
    f->close_section(); // dir_root
  }
  file_mirror_pool.dump_stats(f);
  f->close_section(); // stats
}

void PeerReplayer::reopen_logs() {
  std::scoped_lock locker(m_lock);

  if (m_remote_cluster) {
    reinterpret_cast<CephContext *>(m_remote_cluster->cct())->reopen_logs();
  }
}

} // namespace mirror
} // namespace cephfs