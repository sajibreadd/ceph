// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stack>
#include <fcntl.h>
#include <algorithm>
#include <sys/time.h>
#include <sys/file.h>
#include <boost/scope_exit.hpp>

#include "common/admin_socket.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/Cond.h"
#include "FSMirror.h"
#include "PeerReplayer.h"
#include "Utils.h"

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
    ServiceDaemon *service_daemon)
    : m_cct(cct), m_fs_mirror(fs_mirror), m_local_cluster(local_cluster),
      m_filesystem(filesystem), m_peer(peer),
      m_directories(directories.begin(), directories.end()),
      m_local_mount(mount), m_service_daemon(service_daemon),
      m_asok_hook(new PeerReplayerAdminSocketHook(cct, filesystem, peer, this)),
      m_lock(ceph::make_mutex("cephfs::mirror::PeerReplayer::" +
                              stringify(peer.uuid))),
      task_sink_context(g_ceph_context->_conf.get_val<uint64_t>(
                             "cephfs_mirror_file_sync_thread")) {
  // reset sync stats sent via service daemon
  m_service_daemon->add_or_update_peer_attribute(
      m_filesystem.fscid, m_peer, SERVICE_DAEMON_FAILED_DIR_COUNT_KEY,
      (uint64_t)0);
  m_service_daemon->add_or_update_peer_attribute(
      m_filesystem.fscid, m_peer, SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY,
      (uint64_t)0);

  std::string labels = ceph::perf_counters::key_create(
      "cephfs_mirror_peers",
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
  m_perf_counters = plb.create_perf_counters();
  m_cct->get_perfcounters_collection()->add(m_perf_counters);
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

int PeerReplayer::init() {
  dout(20) << ": initial dir list=[" << m_directories << "]" << dendl;
  for (auto &dir_root : m_directories) {
    m_snap_sync_stats.emplace(dir_root, std::make_shared<SnapSyncStat>());
  }

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
  dout(0) << ": Activating file transfer thread pool having, "
             "cephfs_mirror_file_sync_thread="
          << g_ceph_context->_conf.get_val<uint64_t>(
                 "cephfs_mirror_file_sync_thread")
          << "cephfs_mirror_dir_scanning_thread="
          << g_ceph_context->_conf.get_val<uint64_t>(
                 "cephfs_mirror_dir_scanning_thread")
          << ", cephfs_mirror_max_concurrent_directory_syncs="
          << g_ceph_context->_conf.get_val<uint64_t>(
                 "cephfs_mirror_max_concurrent_directory_syncs")
          << dendl;
  task_sink_context.activate();
  auto nr_replayers = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_concurrent_directory_syncs");
  dout(20) << ": spawning " << nr_replayers << " snapshot replayer(s)" << dendl;

  while (nr_replayers-- > 0) {
    std::unique_ptr<SnapshotReplayerThread> replayer(
      new SnapshotReplayerThread(this));
    std::string name("replayer-" + stringify(nr_replayers));
    replayer->create(name.c_str());
    m_replayers.push_back(std::move(replayer));
  }
  // dout(0) << ": initiation done-->" << dendl;
  return 0;
}

void PeerReplayer::shutdown() {
  dout(20) << dendl;
  dout(0) << ": starting shutdown" << dendl;
  {
    std::scoped_lock locker(m_lock);
    ceph_assert(!m_stopping);
    m_stopping = true;
    m_cond.notify_all();
  }

  dir_op_handler_context.deactivate();
  task_sink_context.deactivate();
  dout(0) << ": Operation handler thread pool deactivated" << dendl;
  for (auto &replayer : m_replayers) {
    replayer->join();
  }
  m_replayers.clear();
  ceph_unmount(m_remote_mount);
  ceph_release(m_remote_mount);
  m_remote_mount = nullptr;
  m_remote_cluster.reset();
}

void PeerReplayer::add_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  std::scoped_lock locker(m_lock);
  m_directories.emplace_back(dir_root);
  m_snap_sync_stats.emplace(dir_root, std::make_shared<SnapSyncStat>());
  m_cond.notify_all();
}

void PeerReplayer::remove_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  auto _dir_root = std::string(dir_root);

  std::scoped_lock locker(m_lock);
  auto it = std::find(m_directories.begin(), m_directories.end(), _dir_root);
  if (it != m_directories.end()) {
    m_directories.erase(it);
  }

  auto it1 = m_registered.find(_dir_root);
  if (it1 == m_registered.end()) {
    m_snap_sync_stats.erase(_dir_root);
  } else {
    it1->second->canceled = true;
  }
  m_cond.notify_all();
}

boost::optional<std::string> PeerReplayer::pick_directory() {
  dout(20) << dendl;

  auto now = clock::now();
  auto retry_timo = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_retry_failed_directories_interval");

  boost::optional<std::string> candidate;
  for (auto &dir_root : m_directories) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    if (sync_stat->failed) {
      std::chrono::duration<double> d = now - *sync_stat->last_failed;
      if (d.count() < retry_timo) {
        continue;
      }
    }
    if (!m_registered.count(dir_root)) {
      candidate = dir_root;
      break;
    }
  }

  std::rotate(m_directories.begin(), m_directories.begin() + 1, m_directories.end());
  return candidate;
}

int PeerReplayer::register_directory(const std::string &dir_root,
                                     SnapshotReplayerThread *replayer) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  ceph_assert(m_registered.find(dir_root) == m_registered.end());

  std::shared_ptr <DirRegistry> registry = std::make_shared <DirRegistry>();
  int r = try_lock_directory(dir_root, replayer, registry);
  if (r < 0) {
    return r;
  }

  dout(5) << ": dir_root=" << dir_root << " registered with replayer="
          << replayer << dendl;
  m_registered.emplace(dir_root, registry);
  return 0;
}

void PeerReplayer::unregister_directory(const std::string &dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  auto it = m_registered.find(dir_root);
  ceph_assert(it != m_registered.end());

  unlock_directory(it->first, *it->second);
  m_registered.erase(it);
  if (std::find(m_directories.begin(), m_directories.end(), dir_root) == m_directories.end()) {
    m_snap_sync_stats.erase(dir_root);
  }
}

int PeerReplayer::try_lock_directory(const std::string &dir_root,
                                     SnapshotReplayerThread *replayer,
                                     std::shared_ptr<DirRegistry> &registry) {
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
  r = ceph_flock(m_remote_mount, fd, LOCK_EX | LOCK_NB, (uint64_t)replayer->get_thread_id());
  if (r != 0) {
    if (r == -EWOULDBLOCK) {
      dout(5) << ": dir_root=" << dir_root << " is locked by cephfs-mirror, "
              << "will retry again" << dendl;
    } else {
      derr << ": failed to lock dir_root=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
    }

    if (ceph_close(m_remote_mount, fd) < 0) {
      derr << ": failed to close (cleanup) remote dir_root=" << dir_root << ": "
           << cpp_strerror(r) << dendl;
    }
    return r;
  }

  dout(10) << ": dir_root=" << dir_root << " locked" << dendl;

  registry->fd = fd;
  registry->replayer = replayer;
  return 0;
}

void PeerReplayer::unlock_directory(const std::string &dir_root, const DirRegistry &registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  int r = ceph_flock(m_remote_mount, registry.fd, LOCK_UN,
                     (uint64_t)registry.replayer->get_thread_id());
  if (r < 0) {
    derr << ": failed to unlock remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
    return;
  }

  r = ceph_close(m_remote_mount, registry.fd);
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
        derr << ": snap_path=" << snap_path << " has invalid metadata in remote snapshot"
             << dendl;
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
  if (dirp) {
    r = ceph_closedir(mnt, dirp);
  }
  if (r < 0) {
    derr << ": failed to close " << lr_str << " snap directory=" << snap_dir
         << ": " << cpp_strerror(r) << dendl;
  }

  dout(10) << ": " << lr_str << " snap_map=" << *snap_map << dendl;
  return rv;
}

int PeerReplayer::propagate_snap_deletes(const std::string &dir_root,
                                         const std::set<std::string> &snaps) {
  dout(5) << ": dir_root=" << dir_root << ", deleted snapshots=" << snaps << dendl;

  for (auto &snap : snaps) {
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
    const std::set<std::pair<std::string,std::string>> &snaps) {
  dout(10) << ": dir_root=" << dir_root << ", renamed snapshots=" << snaps << dendl;

  for (auto &snapp : snaps) {
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

int PeerReplayer::sync_attributes(const std::string &epath,
                                  const struct ceph_statx &stx,
                                  unsigned int change_mask, bool is_dir,
                                  const FHandles &fh) {
  int r = 0;
  if ((change_mask & CEPH_STATX_UID) || (change_mask & CEPH_STATX_GID)) {
    r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                     stx.stx_uid, stx.stx_gid, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chown remote directory=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if ((change_mask & CEPH_STATX_MODE)) {
    r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                     stx.stx_mode & ~S_IFMT, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chmod remote directory=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (!is_dir && (change_mask & CEPH_STATX_MTIME)) {
    struct timespec times[] = {{stx.stx_atime.tv_sec, stx.stx_atime.tv_nsec},
                               {stx.stx_mtime.tv_sec, stx.stx_mtime.tv_nsec}};
    r = ceph_utimensat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), times,
                       AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to change [am]time on remote directory=" << epath
           << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return 0;
}

int PeerReplayer::_remote_mkdir(const std::string &epath,
                                const struct ceph_statx &cstx,
                                const FHandles &fh) {
  int r = ceph_mkdirat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                   cstx.stx_mode & ~S_IFDIR);
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int PeerReplayer::remote_mkdir(const std::string &epath,
                               const struct ceph_statx &cstx, bool create_fresh,
                               unsigned int change_mask, const FHandles &fh,
                               std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  dout(10) << ": remote epath=" << epath << dendl;
  int r = 0;

  if (create_fresh) { // must need to create
    r = _remote_mkdir(epath, cstx, fh);
    if (r < 0) {
      return r;
    }
    dir_sync_stat->current_stat.inc_dir_created_count();
  }

  r = sync_attributes(epath, cstx, change_mask, true, fh);
  return r;
}

void PeerReplayer::C_DoDirSync::finish(int r) {
  if (r < 0) {
    return;
  }
  if (r == 0) {
    common_entry_info_count = 0;
  }
  replayer->do_dir_sync(dir_root, cur_path, cstx, dirp, create_fresh,
                        entry_info_known, entry_info, common_entry_info_count,
                        thread_pool, fh, dir_registry, op_counter, fin,
                        dir_sync_stat);
}

void PeerReplayer::C_TransferAndSyncFile::add_into_stat() {
  replayer->task_sink_context.thread_pool_stats.add_file(stx.stx_size);
}

void PeerReplayer::C_TransferAndSyncFile::remove_from_stat() {
  replayer->task_sink_context.thread_pool_stats.remove_file(stx.stx_size);
}

void PeerReplayer::C_TransferAndSyncFile::finish(int r) {
  if (r < 0) {
    return;
  }
  replayer->transfer_and_sync_file(dir_root, epath, stx, change_mask, fh,
                                   dir_registry, dir_sync_stat);
}

void PeerReplayer::C_CleanUpRemoteDir::finish(int r) {
  if (r < 0) {
    return;
  }
  r = replayer->cleanup_remote_dir(dir_root, epath, fh, dir_registry,
                                   dir_sync_stat);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to cleanup remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    if (!dir_registry->failed) {
      replayer->mark_failed(dir_root, r);
    }
  }
}

void PeerReplayer::C_DeleteFile::finish(int r) {
  if (r < 0) {
    return;
  }
  r = replayer->delete_file(dir_root, epath, fh, dir_registry, dir_sync_stat);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to cleanup remote file=" << epath << ": "
         << cpp_strerror(r) << dendl;
    if (!dir_registry->failed) {
      replayer->mark_failed(dir_root, r);
    }
  }
}

void PeerReplayer::DirOpHandlerContext::ThreadPool::activate() {
  stop_flag = false;
  task_queue_limit = std::max(500, 10 * num_threads);
  for (int i = 0; i < num_threads; ++i) {
    workers.push_back(std::thread(&ThreadPool::run_task, this));
  }
}

std::shared_ptr<PeerReplayer::DirOpHandlerContext::ThreadPool>
PeerReplayer::DirOpHandlerContext::sync_start(int _num_threads) {
  std::unique_lock<std::mutex> lock(context_mutex);
  if (!active) {
    return nullptr;
  }
  int idx = 0;
  if (!unassigned_sync_ids.empty()) {
    idx = unassigned_sync_ids.back();
    thread_pools[idx] = std::make_shared<ThreadPool>(_num_threads, idx);
    unassigned_sync_ids.pop_back();
  } else {
    idx = sync_count++;
    thread_pools.emplace_back(std::make_shared<ThreadPool>(_num_threads, idx));
  }
  thread_pools[idx]->activate();
  return thread_pools[idx];
}

void PeerReplayer::DirOpHandlerContext::dump_stats(Formatter *f) {
  std::unique_lock<std::mutex> lock(context_mutex);
  int task_count = 0;
  for (int i = 0; i < thread_pools.size(); ++i) {
    task_count += thread_pools[i]->queued_task;
  }
  f->dump_int("queued_dir_ops_count", task_count);
}

void PeerReplayer::DirOpHandlerContext::deactivate() {
  std::unique_lock<std::mutex> lock(context_mutex);
  active = false;
  for (int i = 0; i < thread_pools.size(); ++i) {
    thread_pools[i]->deactivate();
  }
}

void PeerReplayer::DirOpHandlerContext::ThreadPool::deactivate() {
  stop_flag = true;
  pick_task.notify_all();
  for (auto &worker : workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  drain_queue();
  workers.clear();
}

void PeerReplayer::DirOpHandlerContext::sync_finish(int idx) {
  std::unique_lock<std::mutex> lock(context_mutex);
  ceph_assert(idx < thread_pools.size());
  thread_pools[idx]->deactivate();
  unassigned_sync_ids.push_back(idx);
}


void PeerReplayer::DirOpHandlerContext::ThreadPool::drain_queue() {
  std::scoped_lock lock(mtx);
  while (!task_queue.empty()) {
    auto &task = task_queue.front();
    task->complete(-1);
    task_queue.pop();
  }
}

void PeerReplayer::DirOpHandlerContext::ThreadPool::run_task() {
  while (true) {
    C_MirrorContext *task;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_task.wait(lock,
                     [this] { return (stop_flag || !task_queue.empty()); });
      if (stop_flag) {
        return;
      }
      task = task_queue.front();
      task_queue.pop();
      queued_task--;
    }
    task->complete(0);
  }
}

bool PeerReplayer::DirOpHandlerContext::ThreadPool::do_task_async(
    C_MirrorContext *task) {
  {
    std::unique_lock<std::mutex> lock(mtx);
    if (task_queue.size() >=
        g_ceph_context->_conf->cephfs_mirror_thread_pool_queue_size) {
      return false;
    }
    task->inc_counter();
    task_queue.emplace(task);
    queued_task++;
  }
  pick_task.notify_one();
  return true;
}

bool PeerReplayer::DirOpHandlerContext::ThreadPool::handle_task_async(
    C_MirrorContext *task) {
  bool success = false;
  if (task_queue.size() <
      g_ceph_context->_conf->cephfs_mirror_thread_pool_queue_size) {
    success = do_task_async(task);
  }
  return success;
}

void PeerReplayer::DirOpHandlerContext::ThreadPool::handle_task_force(
    C_MirrorContext *task) {
  if (!handle_task_async(task)) {
    handle_task_sync(task);
  }
}

void PeerReplayer::DirOpHandlerContext::ThreadPool::handle_task_sync(
    C_MirrorContext *task) {
  task->inc_counter();
  task->complete(1);
}

void PeerReplayer::TaskSinkContext::activate() {
  ceph_assert(stop_flag);
  stop_flag = false;
  for (int i = 0; i < workers.size(); ++i) {
    workers[i] = std::thread(&TaskSinkContext::run_task, this);
  }
}

void PeerReplayer::TaskSinkContext::drain_queue() {
  std::scoped_lock lock(mtx);
  for (int i = 0; i < task_queue.size(); ++i) {
    while (!task_queue[i].empty()) {
      auto &task = task_queue[i].front();
      task->complete(-1);
      task_queue[i].pop();
    }
  }
}

PeerReplayer::TaskSinkContext::TaskSinkContext(int num_threads)
    : workers(num_threads), stop_flag(true),
      thread_pool_stats(num_threads, this), sync_count(0) {
  task_limit = max(10 * num_threads, 5000);
  // file_task_queue_limit = 100000;
  // other_task_queue_limit = 100000;
}

void PeerReplayer::TaskSinkContext::deactivate() {
  ceph_assert(!stop_flag);
  stop_flag = true;

  pick_task.notify_all();
  give_task.notify_all();
  for (auto &worker : workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  drain_queue();
}

int PeerReplayer::TaskSinkContext::sync_start() {
  std::unique_lock<std::mutex> lock(mtx);
  int idx = 0;
  if (!unassigned_sync_ids.empty()) {
    idx = unassigned_sync_ids.back();
    task_queue[idx] = std::queue<C_MirrorContext *>();
    unassigned_sync_ids.pop_back();
  } else {
    idx = sync_count++;
    task_queue.emplace_back(std::queue<C_MirrorContext *>());
  }
  return idx;
}

void PeerReplayer::TaskSinkContext::sync_finish(int idx) {
  std::unique_lock<std::mutex>(mtx);
  ceph_assert(idx < task_queue.size());
  while (!task_queue[idx].empty()) {
    auto &task = task_queue[idx].front();
    task->complete(-1);
    task_queue[idx].pop();
  }
  unassigned_sync_ids.push_back(idx);
}

void PeerReplayer::TaskSinkContext::run_task() {
  while (true) {
    C_MirrorContext* task;
    int idx = 0;
    uint64_t file_size;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_task.wait(lock,
                     [this] { return (stop_flag || !task_ring.empty()); });
      if (stop_flag) {
        return;
      }
      task = task_ring.front();
      idx = task->dir_sync_stat->sync_idx;
      ceph_assert(idx < task_queue.size() && !task_queue[idx].empty());
      task_queue[idx].pop();
      task_ring.pop();
      if (!task_queue[idx].empty()) {
        task_ring.emplace(task_queue[idx].front());
      }
      task->remove_from_stat();
      // thread_pool_stats.remove_file(task->stx.stx_size);
    }
    give_task.notify_one();
    task->complete(0);
  }
}

void PeerReplayer::TaskSinkContext::do_task_async(C_MirrorContext *task) {
  int idx = task->dir_sync_stat->sync_idx;
  {
    std::unique_lock<std::mutex> lock(mtx);
    give_task.wait(lock, [this, idx] {
      return (stop_flag || task_queue[idx].size() < task_limit);
    });
    if (stop_flag) {
      return;
    }
    if (task_queue[idx].empty()) {
      task_ring.emplace(task);
    }
    task_queue[idx].emplace(task);
    task->inc_counter();
    task->add_into_stat();
    // thread_pool_stats.add_file(task->stx.stx_size);
  }
  pick_task.notify_one();
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::copy_to_remote(const std::string &dir_root,
                                 const std::string &epath,
                                 const struct ceph_statx &stx,
                                 const FHandles &fh,
                                 std::shared_ptr<DirRegistry> &dir_registry) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;
  uint64_t total_read = 0, total_wrote = 0;
  std::string full_path = entry_path(dir_root, epath);
  int l_fd;
  int r_fd;
  void *ptr;
  struct iovec iov[NR_IOVECS];

  int r = ceph_openat(m_local_mount, fh.c_fd, epath.c_str(), O_RDONLY | O_NOFOLLOW, 0);
  if (r < 0) {
    derr << ": failed to open local file path=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  l_fd = r;
  r = ceph_openat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                  O_CREAT | O_TRUNC | O_WRONLY | O_NOFOLLOW, stx.stx_mode);
  if (r < 0) {
    derr << ": failed to create remote file path=" << epath << ": "
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
    if (should_backoff(dir_registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    for (int i = 0; i < NR_IOVECS; ++i) {
      iov[i].iov_base = (char*)ptr + IOVEC_SIZE*i;
      iov[i].iov_len = IOVEC_SIZE;
    }

    r = ceph_preadv(m_local_mount, l_fd, iov, NR_IOVECS, -1);
    if (r < 0) {
      derr << ": failed to read local file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
    if (r == 0) {
      break;
    }
    total_read += r;
    dout(20) << ": successfully read " << total_read << " bytes from local"
             << full_path << dendl;

    int iovs = (int)(r / IOVEC_SIZE);
    int t = r % IOVEC_SIZE;
    if (t) {
      iov[iovs].iov_len = t;
      ++iovs;
    }

    r = ceph_pwritev(m_remote_mount, r_fd, iov, iovs, -1);
    if (r < 0) {
      derr << ": failed to write remote file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
    total_wrote += r;
    dout(20) << ": successfully wrote " << total_wrote << " bytes to remote "
             << full_path << dendl;
  }

  if (r == 0) {
    r = ceph_fsync(m_remote_mount, r_fd, 0);
    if (r < 0) {
      derr << ": failed to sync data for file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
    }
  }

  free(ptr);

close_remote_fd:
  if (ceph_close(m_remote_mount, r_fd) < 0) {
    derr << ": failed to close remote fd path=" << epath << ": " << cpp_strerror(r)
         << dendl;
    r = -EINVAL;
  }

close_local_fd:
  if (ceph_close(m_local_mount, l_fd) < 0) {
    derr << ": failed to close local fd path=" << epath << ": " << cpp_strerror(r)
         << dendl;
    r = -EINVAL;
  }
  // dout(0) << ": file transfer finished-->" << epath << dendl;
  return r == 0 ? 0 : r;
}

void PeerReplayer::transfer_and_sync_file(
    const std::string &dir_root, const std::string &epath,
    const struct ceph_statx &stx, unsigned int change_mask, const FHandles &fh,
    std::shared_ptr<DirRegistry> &dir_registry,
    std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  int r = copy_to_remote(dir_root, epath, stx, fh, dir_registry);
  if (r < 0) {
    if (!dir_registry->failed) {
      mark_failed(dir_root, r);
    }
    derr << ": failed to copy path=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return;
  }
  if (m_perf_counters) {
    m_perf_counters->inc(l_cephfs_mirror_peer_replayer_sync_bytes,
                         stx.stx_size);
  }
  if (change_mask > 0) {
    r = sync_attributes(epath, stx, change_mask, false, fh);
  }
  if (r < 0 && !dir_registry->failed) {
    mark_failed(dir_root, r);
    return;
  }
  dir_sync_stat->current_stat.inc_file_op_count(true, (change_mask > 0),
                                                stx.stx_size);
  dir_sync_stat->current_stat.dec_file_in_flight_count(stx.stx_size);
}

int PeerReplayer::remote_file_op(
    const std::string &dir_root, const std::string &epath,
    const struct ceph_statx &stx, bool need_data_sync, unsigned int change_mask,
    const FHandles &fh,
    std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
    std::shared_ptr<DirRegistry> &dir_registry,
    std::atomic<int64_t> &op_counter, Context *fin,
    std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath
           << ", need_data_sync=" << need_data_sync
           << ", stat_change_mask=" << change_mask << dendl;

  int r;
  if (need_data_sync) {
    if (S_ISREG(stx.stx_mode)) {
      C_TransferAndSyncFile *task = new C_TransferAndSyncFile(
          dir_root, epath, stx, change_mask, fh, dir_registry, op_counter, fin,
          this, dir_sync_stat);
      task_sink_context.do_task_async(task);
      dir_sync_stat->current_stat.inc_file_in_flight_count(stx.stx_size);
      
      // task();
      return 0;
    } else if (S_ISLNK(stx.stx_mode)) {
      // free the remote link before relinking
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
      if (r < 0 && r != -ENOENT) {
        derr << ": failed to remove remote symlink=" << epath << dendl;
        return r;
      }
      char *target = (char *)alloca(stx.stx_size+1);
      r = ceph_readlinkat(m_local_mount, fh.c_fd, epath.c_str(), target, stx.stx_size);
      if (r < 0) {
        derr << ": failed to readlink local path=" << epath << ": " << cpp_strerror(r)
             << dendl;
        return r;
      }

      target[stx.stx_size] = '\0';
      r = ceph_symlinkat(m_remote_mount, target, fh.r_fd_dir_root, epath.c_str());
      if (r < 0 && r != EEXIST) {
        derr << ": failed to symlink remote path=" << epath << " to target=" << target
             << ": " << cpp_strerror(r) << dendl;
        return r;
      }
      change_mask = (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID |
                     CEPH_STATX_GID | CEPH_STATX_MTIME);
    } else {
      dout(5) << ": skipping entry=" << epath << ": unsupported mode=" << stx.stx_mode
              << dendl;
      dir_sync_stat->current_stat.inc_file_op_count(false, false, stx.stx_size);
      return 0;
    }
  }

  if (change_mask) {
    r = sync_attributes(epath, stx, change_mask, false, fh);
    if (r < 0) {
      return r;
    }
  }
  dir_sync_stat->current_stat.inc_file_op_count(
      need_data_sync, (change_mask > 0), stx.stx_size);

  return 0;
}

int PeerReplayer::cleanup_remote_dir(
    const std::string &dir_root, const std::string &epath, const FHandles &fh,
    std::shared_ptr<DirRegistry> &dir_registry,
    std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  dout(20) << ": dir_root=" << dir_root << ", epath=" << epath
           << dendl;

  struct ceph_statx tstx;
  int r = ceph_statxat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), &tstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                           CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                       AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r == -ENOENT) {
    dout(10) << ": directory already removed=" << epath << dendl;
    return r;
  }
  if (!S_ISDIR(tstx.stx_mode)) {
    dout(10) << ": although it is a directory in previous snapshot but in "
                "remote it is not a directory because of intermediate "
                "shutdown, removing "
                "it as a file="
             << epath << dendl;
    r = delete_file(dir_root, epath, fh, dir_registry, dir_sync_stat);
  }
  if (r < 0) {
    derr << ": failed to remove remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
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
    if (should_backoff(dir_registry, &r)) {
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
        r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(), AT_REMOVEDIR);
        if (r < 0) {
          derr << ": failed to remove remote directory=" << entry.epath << ": "
               << cpp_strerror(r) << dendl;
          break;
        }
        dir_sync_stat->current_stat.inc_dir_deleted_count();

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
        r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath, AT_SYMLINK_NOFOLLOW,
                      &dirp);
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
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(), 0);
      if (r < 0) {
        derr << ": failed to remove remote directory=" << entry.epath << ": "
             << cpp_strerror(r) << dendl;
        break;
      }
      dout(10) << ": done for remote file=" << entry.epath << dendl;
      dir_sync_stat->current_stat.inc_file_del_count();
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

int PeerReplayer::delete_file(const std::string &dir_root,
                              const std::string &epath, const FHandles &fh,
                              std::shared_ptr<DirRegistry> &dir_registry,
                              std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  dout(20) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;
  int r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
  if (r == 0) {
    dir_sync_stat->current_stat.inc_file_del_count();
  }
  return r;
}

int PeerReplayer::propagate_deleted_entries(
    const std::string &dir_root, const std::string &epath,
    std::unordered_map<std::string, CommonEntryInfo> &common_entry_info,
    uint64_t &common_entry_info_count, const FHandles &fh,
    std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
    std::shared_ptr<DirRegistry> &dir_registry,
    std::atomic<int64_t> &op_counter, Context *fin,
    std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;

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
      dout(5) << ": epath=" << epath << " missing in previous-snap/remote dir-root"
              << dendl;
    }
    return r;
  }
  struct ceph_statx pstx;
  struct dirent de;
  while (true) {
    if (should_backoff(dir_registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }
    unsigned int extra_flags =
        (common_entry_info_count <
         g_ceph_context->_conf->cephfs_mirror_max_element_in_cache_per_thread)
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
      if (S_ISDIR(pstx.stx_mode)) {
        C_CleanUpRemoteDir *task =
            new C_CleanUpRemoteDir(dir_root, dpath, fh, dir_registry,
                                   op_counter, fin, this, dir_sync_stat);
        // task_sink_context.do_task_async(task);
        thread_pool->handle_task_force(task);
      } else {
        C_DeleteFile *task =
            new C_DeleteFile(dir_root, dpath, fh, dir_registry, op_counter, fin,
                             this, dir_sync_stat);
        // task_sink_context.do_task_async(task);
        thread_pool->handle_task_force(task);
      }
    } else if (extra_flags) {
      unsigned int change_mask = 0;
      build_change_mask(pstx, cstx, purge_remote, change_mask);
      common_entry_info[d_name] =
          CommonEntryInfo(S_ISDIR(pstx.stx_mode), purge_remote, change_mask);
      ++common_entry_info_count;
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
    FHandles *fh) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  auto cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  auto fd = open_dir(m_local_mount, cur_snap_path, current.second);
  if (fd < 0) {
    return fd;
  }

  // current snapshot file descriptor
  fh->c_fd = fd;

  MountRef mnt;
  if (prev) {
    dout(0) << "mirroring snapshot '" << current.first
            << "' using a local copy of snapshot '" << (*prev).first
            << "' as a diff base, dir_root=" << dir_root.c_str()
            << dendl;
    mnt = m_local_mount;
    auto prev_snap_path = snapshot_path(m_cct, dir_root, (*prev).first);
    fd = open_dir(mnt, prev_snap_path, (*prev).second);
  } else {
    dout(0) << "mirroring snapshot '" << current.first
            << "' using a remote state as a diff base, "
            "dir_root = " << dir_root.c_str()
            << dendl;
    mnt = m_remote_mount;
    fd = open_dir(mnt, dir_root, boost::none);
  }
  if (fd < 0) {
    if (!prev || fd != -ENOENT) {
      ceph_close(m_local_mount, fh->c_fd);
      return fd;
    }

    // ENOENT of previous snap
    dout(5) << ": previous snapshot=" << *prev << " missing" << dendl;
    mnt = m_remote_mount;
    fd = open_dir(mnt, dir_root, boost::none);
    if (fd < 0) {
      ceph_close(m_local_mount, fh->c_fd);
      return fd;
    }
  }

  // "previous" snapshot or dir_root file descriptor
  fh->p_fd = fd;
  fh->p_mnt = mnt;

  {
    std::scoped_lock locker(m_lock);
    auto it = m_registered.find(dir_root);
    ceph_assert(it != m_registered.end());
    fh->r_fd_dir_root = it->second->fd;
  }

  dout(5) << ": using " << ((fh->p_mnt == m_local_mount) ? "local (previous) snapshot" : "remote dir_root")
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

void PeerReplayer::build_change_mask(const struct ceph_statx &pstx,
                                     const struct ceph_statx &cstx,
                                     bool create_fresh,
                                     unsigned int &change_mask) {
  if (create_fresh) {
    change_mask = (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID |
                   CEPH_STATX_GID | CEPH_STATX_MTIME);
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

void PeerReplayer::do_dir_sync(
    const std::string &dir_root, const std::string &cur_path,
    const struct ceph_statx &cstx, ceph_dir_result *dirp, bool create_fresh,
    bool entry_info_known, CommonEntryInfo &entry_info,
    uint64_t common_entry_info_count,
    std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
    const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
    std::atomic<int64_t> &op_counter, Context *fin,
    std::shared_ptr<SnapSyncStat> &dir_sync_stat) {
  // dout(0) << ": do_dir_sync1-->" << cur_path << dendl;
  int r = 0;
  bool is_root = (cur_path == ".");
  struct ceph_statx child_stx, pstx;
  struct dirent child_de;
  std::string child_dname, child_ename, child_path;
  C_DoDirSync *task;
  std::unordered_map<std::string, CommonEntryInfo> common_entry_info;
  int pstat_r = 0, rem_r = 0;

  if (should_backoff(dir_registry, &r)) {
    dout(0) << ": backing off r=" << r << dendl;
    goto safe_exit;
  }

  if (entry_info_known) {
    dir_sync_stat->current_stat.inc_cache_hit();
  }

  if (!is_root) {
    if (!entry_info_known) {
      if (!create_fresh) {
        pstat_r = ceph_statxat(fh.p_mnt, fh.p_fd, cur_path.c_str(), &pstx,
                               CEPH_STATX_MODE | CEPH_STATX_UID |
                                   CEPH_STATX_GID | CEPH_STATX_SIZE |
                                   CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                               AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
        if (pstat_r < 0 && pstat_r != -ENOENT && pstat_r != -ENOTDIR) {
          r = pstat_r;
          derr << ": failed to stat prev entry= " << cur_path << ": "
               << cpp_strerror(r) << dendl;
          goto sanity_check;
        }
        entry_info.is_dir = S_ISDIR(pstx.stx_mode);
        entry_info.purge_remote =
            (pstat_r == 0 &&
             (cstx.stx_mode & S_IFMT) != (pstx.stx_mode & S_IFMT));
      }
    }
    if (entry_info.purge_remote || pstat_r < 0) {
      create_fresh = true;
    }
    if (create_fresh && entry_info.purge_remote) {
      if (entry_info.is_dir) {
        rem_r = cleanup_remote_dir(dir_root, cur_path, fh, dir_registry,
                                   dir_sync_stat);
      } else {
        rem_r =
            delete_file(dir_root, cur_path, fh, dir_registry, dir_sync_stat);
        if (rem_r == -EISDIR && S_ISDIR(cstx.stx_mode)) {
          dout(10)
              << ": although it is not a directory in previous snapshot but in "
                 "remote it is a directory because of intermediate shutdown="
              << cur_path << dendl;
          rem_r = 0;
        }
        
      }
      if (rem_r < 0 && rem_r != -ENOENT) {
        derr << ": failed to cleanup remote entry=" << cur_path << ": "
             << cpp_strerror(rem_r) << dendl;
        r = rem_r;
        goto sanity_check;
      }
    }
    if (!entry_info_known) {
      build_change_mask(pstx, cstx, create_fresh, entry_info.change_mask);
    }
    if (S_ISDIR(cstx.stx_mode)) {
      r = remote_mkdir(cur_path, cstx, create_fresh, entry_info.change_mask, fh,
                       dir_sync_stat);
      if (r < 0) {
        goto sanity_check;
      }
      r = opendirat(m_local_mount, fh.c_fd, cur_path, AT_SYMLINK_NOFOLLOW,
                    &dirp);
      if (r < 0) {
        derr << ": failed to open local directory=" << cur_path << ": "
             << cpp_strerror(r) << dendl;
        goto safe_exit;
      }
      dir_sync_stat->current_stat.inc_dir_scanned_count();
    } else {
      bool need_data_sync = create_fresh ||
                            ((entry_info.change_mask & CEPH_STATX_SIZE) > 0) ||
                            (((entry_info.change_mask & CEPH_STATX_MTIME) > 0));
      dout(5) << ": entry=" << cur_path << ", data_sync=" << need_data_sync
              << ", attr_change_mask=" << entry_info.change_mask << dendl;
      if (need_data_sync || entry_info.change_mask) {
        r = remote_file_op(dir_root, cur_path, cstx, need_data_sync,
                           entry_info.change_mask, fh, thread_pool,
                           dir_registry, op_counter, fin, dir_sync_stat);
        if (r < 0) {
          goto sanity_check;
        }
      } else {
        dir_sync_stat->current_stat.inc_file_op_count(0, 0, cstx.stx_size);
      }
      goto sanity_check;
    }
  }
  if (!create_fresh) {
    r = propagate_deleted_entries(dir_root, cur_path, common_entry_info,
                                  common_entry_info_count, fh, thread_pool,
                                  dir_registry, op_counter, fin, dir_sync_stat);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to propagate missing dirs: " << cpp_strerror(r)
           << dendl;
      goto safe_exit;
    }
  }

  while (true) {
    if (should_backoff(dir_registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      goto safe_exit;
    }
    r = ceph_readdirplus_r(m_local_mount, dirp, &child_de, &child_stx,
                           CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                               CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                           AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
    if (r < 0) {
      derr << ": failed to local read directory=" << cur_path << ":"
           << cpp_strerror(r) << dendl;
      break;
    }
    if (r == 0) {
      dout(10) << ": done for directory=" << cur_path << dendl;
      break;
    }
    auto child_dname = std::string(child_de.d_name);
    if (child_dname == "." || child_dname == "..") {
      continue;
    }
    child_path = entry_path(cur_path, child_dname);
    CommonEntryInfo child_info;
    bool child_info_known = false;
    if (!create_fresh) {
      auto it = common_entry_info.find(child_dname);
      if (it != common_entry_info.end()) {
        child_info_known = true;
        child_info = it->second;
        common_entry_info.erase(it);
        --common_entry_info_count;
        if (!S_ISDIR(child_stx.stx_mode) && !child_info.purge_remote &&
            child_info.change_mask == 0) {
          dir_sync_stat->current_stat.inc_cache_hit();
          dir_sync_stat->current_stat.inc_file_op_count(0, 0,
                                                        child_stx.stx_size);
          continue;
        }
      }
    }
    task = new C_DoDirSync(dir_root, child_path, child_stx, nullptr,
                           child_info.purge_remote | create_fresh,
                           child_info_known, child_info,
                           common_entry_info_count, thread_pool, fh,
                           dir_registry, op_counter, fin, this, dir_sync_stat);
    thread_pool->handle_task_force(task);
  }

safe_exit:
  if (!is_root && dirp && ceph_closedir(m_local_mount, dirp) < 0) {
    derr << ": failed to close local directory=" << cur_path << dendl;
  }

sanity_check:
  if (r < 0 && !dir_registry->failed) {
    mark_failed(dir_root, r);
  }
}

int PeerReplayer::do_synchronize(const std::string &dir_root, const Snapshot &current,
                                 boost::optional<Snapshot> prev) {
  dout(0)
      << ": cephfs_mirror_file_sync_thread="
      << g_ceph_context->_conf.get_val<uint64_t>(
             "cephfs_mirror_file_sync_thread")
      << ", cephfs_mirror_dir_scanning_thread="
      << g_ceph_context->_conf.get_val<uint64_t>(
             "cephfs_mirror_dir_scanning_thread")
      << ", cephfs_mirror_remote_diff_base_upon_start="
      << g_ceph_context->_conf.get_val<bool>(
             "cephfs_mirror_remote_diff_base_upon_start")
      << ", cephfs_mirror_sync_latest_snapshot="
      << g_ceph_context->_conf->cephfs_mirror_sync_latest_snapshot
      << ", cephfs_mirror_thread_pool_queue_size="
      << g_ceph_context->_conf->cephfs_mirror_thread_pool_queue_size
      << ", cephfs_mirror_max_element_in_cache_per_thread="
      << g_ceph_context->_conf->cephfs_mirror_max_element_in_cache_per_thread
      << dendl;
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": incremental sync check from prev=" << prev << dendl;
  }

  FHandles fh;
  int r = pre_sync_check_and_open_handles(dir_root, current, prev, &fh);
  if (r < 0) {
    dout(5) << ": cannot proceeed with sync: " << cpp_strerror(r) << dendl;
    return r;
  }

  // record that we are going to "dirty" the data under this
  // directory root
  auto snap_id_str{stringify(current.second)};
  r = ceph_fsetxattr(m_remote_mount, fh.r_fd_dir_root, "ceph.mirror.dirty_snap_id",
                     snap_id_str.c_str(), snap_id_str.size(), 0);
  if (r < 0) {
    derr << ": error setting \"ceph.mirror.dirty_snap_id\" on dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
    ceph_close(m_local_mount, fh.c_fd);
    ceph_close(fh.p_mnt, fh.p_fd);
    return r;
  }

  struct ceph_statx tstx;
  r = ceph_fstatx(m_local_mount, fh.c_fd, &tstx,
                  CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                      CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                  AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    ceph_close(m_local_mount, fh.c_fd);
    ceph_close(fh.p_mnt, fh.p_fd);
    return r;
  }

  ceph_dir_result *tdirp;
  r = ceph_fdopendir(m_local_mount, fh.c_fd, &tdirp);
  if (r < 0) {
    derr << ": failed to open local snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    ceph_close(m_local_mount, fh.c_fd);
    ceph_close(fh.p_mnt, fh.p_fd);
    return r;
  }

  // starting from this point we shouldn't care about manual closing of fh.c_fd,
  // it will be closed automatically when bound tdirp is closed.

  std::string cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  void* buf_file_count = malloc(20);
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

  void* buf_file_bytes = malloc(20);
  std::string rbytes = "";
  int x2 = ceph_getxattr(m_local_mount, cur_snap_path.c_str(),
                         "ceph.dir.rbytes", buf_file_bytes, 20);
  if (x2 < 0) {
    derr << ": failed to read ceph.dir.rfiles xattr for directory=" << dir_root
         << dendl;
  } else {
    rbytes = std::move(std::string((char*)buf_file_bytes, x2));
    dout(0) << ": xattr ceph.dir.rbytes=" << rbytes
            << " for directory=" << dir_root << dendl;
  }

  std::atomic<int64_t> op_counter(0);
  C_SaferCond fin;
  std::unique_lock<ceph::mutex> lock(m_lock);
  auto dir_registry = m_registered.at(dir_root);
  dir_registry->failed = false, dir_registry->failed_reason = 0;
  auto dir_sync_stat = m_snap_sync_stats.at(dir_root);
  dir_sync_stat->current_stat.rfiles = std::stoull(rfiles);
  dir_sync_stat->current_stat.rbytes = std::stoull(rbytes);
  dir_sync_stat->current_stat.start_timer();
  dir_sync_stat->sync_idx = task_sink_context.sync_start();
  lock.unlock();
  int _num_threads = g_ceph_context->_conf.get_val<uint64_t>(
      "cephfs_mirror_dir_scanning_thread");
  dout(0) << ": number of threads for this sync=" << _num_threads << dendl;
  std::shared_ptr <DirOpHandlerContext::ThreadPool> thread_pool =
      dir_op_handler_context.sync_start(_num_threads);

  dout(0) << ": dir_sync_stat.sync_idx=" << dir_sync_stat->sync_idx
          << ", thread_pool=" << thread_pool->thread_idx << dendl;

  if (thread_pool) {
    C_DoDirSync *task = new C_DoDirSync(
        dir_root, ".", tstx, tdirp, false, false, CommonEntryInfo(), 0,
        thread_pool, fh, dir_registry, op_counter, &fin, this, dir_sync_stat);
    thread_pool->handle_task_force(task);
    fin.wait();
    dir_op_handler_context.sync_finish(thread_pool->thread_idx);
    task_sink_context.sync_finish(dir_sync_stat->sync_idx);
  }

  if (tdirp && ceph_closedir(m_local_mount, tdirp) < 0) {
    derr << ": failed to close local directory=." << dendl;
  }
  if (m_stopping) {
    r = -EINPROGRESS;
  }
  if (r >= 0 && dir_registry->failed) {
    r = get_failed_reason(dir_root);
  }
  lock.lock();
  dir_sync_stat->reset_stats();
  lock.unlock();

  dout(0) << ": done sync-->" << dir_root << ", " << current.first << dendl;
  dout(20) << " cur:" << fh.c_fd
           << " prev:" << fh.p_fd
           << " ret = " << r
           << dendl;

  // @FHandles.r_fd_dir_root is closed in @unregister_directory since
  // its used to acquire an exclusive lock on remote dir_root.

  // c_fd has been used in ceph_fdopendir call so
  // there is no need to close this fd manually.
  ceph_close(fh.p_mnt, fh.p_fd);

  return r;
}

int PeerReplayer::synchronize(const std::string &dir_root, const Snapshot &current,
                              boost::optional<Snapshot> prev) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  int r = ceph_getxattr(m_remote_mount, dir_root.c_str(), "ceph.mirror.dirty_snap_id", nullptr, 0);
  if (r < 0 && r != -ENODATA) {
    derr << ": failed to fetch primary_snap_id length from dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  {
    std::scoped_lock lock(m_lock);
    auto &dir_sync_stat = m_snap_sync_stats.at(dir_root);
    bool remote_diff_base = g_ceph_context->_conf.get_val<bool>(
        "cephfs_mirror_remote_diff_base_upon_start");
    if (dir_sync_stat->synced_snap_count == 0 && remote_diff_base) {
      r = -1;
    }
  }
  // r = -1;
  // no xattr, can't determine which snap the data belongs to!
  if (r < 0) {
    dout(5) << ": missing \"ceph.mirror.dirty_snap_id\" xattr on remote -- using"
            << " incremental sync with remote scan" << dendl;
    r = do_synchronize(dir_root, current, boost::none);
  } else {
    size_t xlen = r;
    char *val = (char *)alloca(xlen+1);
    r = ceph_getxattr(m_remote_mount, dir_root.c_str(), "ceph.mirror.dirty_snap_id", (void*)val, xlen);
    if (r < 0) {
      derr << ": failed to fetch \"dirty_snap_id\" for dir_root: " << dir_root
           << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    val[xlen] = '\0';
    uint64_t dirty_snap_id = atoll(val);

    dout(20) << ": dirty_snap_id: " << dirty_snap_id << " vs (" << current.second
             << "," << (prev ? stringify((*prev).second) : "~") << ")" << dendl;
    if (prev && (dirty_snap_id == (*prev).second || dirty_snap_id == current.second)) {
      dout(5) << ": match -- using incremental sync with local scan" << dendl;
      r = do_synchronize(dir_root, current, prev);
    } else {
      dout(5) << ": mismatch -- using incremental sync with remote scan" << dendl;
      r = do_synchronize(dir_root, current, boost::none);
    }
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

  return r;
}

int PeerReplayer::_do_sync_snaps(const std::string &dir_root,
                                 uint64_t cur_snap_id,
                                 std::string cur_snap_name,
                                 uint64_t last_snap_id,
                                 std::string last_snap_name) {
  int r = 0;
  set_current_syncing_snap(dir_root, cur_snap_id, cur_snap_name);
  auto start = clock::now();
  boost::optional<Snapshot> prev = boost::none;
  if (last_snap_id != 0) {
    prev = std::make_pair(last_snap_name, last_snap_id);
  }
  r = synchronize(dir_root, std::make_pair(cur_snap_name, cur_snap_id), prev);
  if (r < 0) {
    derr << ": failed to synchronize dir_root=" << dir_root
         << ", snapshot=" << cur_snap_name << dendl;
    clear_current_syncing_snap(dir_root);
    return r;
  }
  if (m_perf_counters) {
    m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_synced);
  }
  std::chrono::duration<double> duration = clock::now() - start;

  utime_t d;
  d.set_from_double(duration.count());
  if (m_perf_counters) {
    m_perf_counters->tinc(l_cephfs_mirror_peer_replayer_avg_sync_time, d);
  }

  set_last_synced_stat(dir_root, cur_snap_id, cur_snap_name, duration.count());
  return r;
}

int PeerReplayer::do_sync_snaps(const std::string &dir_root) {
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
  std::set<std::string> snaps_deleted;
  std::set<std::pair<std::string,std::string>> snaps_renamed;
  for (auto &[primary_snap_id, snap_name] : remote_snap_map) {
    auto it = local_snap_map.find(primary_snap_id);
    if (it == local_snap_map.end()) {
      snaps_deleted.emplace(snap_name);
    } else if (it->second != snap_name) {
      snaps_renamed.emplace(std::make_pair(snap_name, it->second));
    }
  }

  r = propagate_snap_deletes(dir_root, snaps_deleted);
  if (r < 0) {
    derr << ": failed to propgate deleted snapshots" << dendl;
    return r;
  }

  r = propagate_snap_renames(dir_root, snaps_renamed);
  if (r < 0) {
    derr << ": failed to propgate renamed snapshots" << dendl;
    return r;
  }

  // start mirroring snapshots from the last snap-id synchronized
  uint64_t last_snap_id = 0;
  std::string last_snap_name;
  if (!remote_snap_map.empty()) {
    auto last = remote_snap_map.rbegin();
    last_snap_id = last->first;
    last_snap_name = last->second;
    set_last_synced_snap(dir_root, last_snap_id, last_snap_name);
  }

  if (g_ceph_context->_conf->cephfs_mirror_sync_latest_snapshot) {
    auto it = local_snap_map.rbegin();
    if (it->first != last_snap_id) {
      dout(5) << ": latest_local_snap_id=" << it->first
              << ", latest_local_snap_name=" << it->second << dendl;
      r = _do_sync_snaps(dir_root, it->first, it->second, last_snap_id,
                        last_snap_name);
    }

  } else {
    dout(5) << ": last snap-id transferred=" << last_snap_id << dendl;
    auto it = local_snap_map.upper_bound(last_snap_id);
    if (it == local_snap_map.end()) {
      dout(20) << ": nothing to synchronize" << dendl;
      return 0;
    }

    auto snaps_per_cycle = g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_max_snapshot_sync_per_cycle");

    dout(10) << ": synchronizing from snap-id=" << it->first << dendl;
    for (; it != local_snap_map.end(); ++it) {
      r = _do_sync_snaps(dir_root, it->first, it->second, last_snap_id,
                         last_snap_name);
      if (r < 0) {
        return r;
      }
      if (--snaps_per_cycle == 0) {
        break;
      }

      last_snap_name = it->second;
      last_snap_id = it->first;
    }
  }

  return 0;
}

void PeerReplayer::sync_snaps(const std::string &dir_root,
                              std::unique_lock<ceph::mutex> &locker) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  locker.unlock();
  int r = do_sync_snaps(dir_root);
  if (r < 0) {
    derr << ": failed to sync snapshots for dir_root=" << dir_root << dendl;
  }
  locker.lock();
  if (r < 0) {
    _inc_failed_count(dir_root);
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snap_sync_failures);
    }
  } else {
    _reset_failed_count(dir_root);
  }
}

void PeerReplayer::run(SnapshotReplayerThread *replayer) {
  dout(10) << ": snapshot replayer=" << replayer << dendl;
  // dout(0) << ": I am here1-->" << dendl;

  time last_directory_scan = clock::zero();
  auto scan_interval = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_directory_scan_interval");

  std::unique_lock locker(m_lock);
  while (true) {
    // do not check if client is blocklisted under lock
    m_cond.wait_for(locker, 1s, [this]{return is_stopping();});
    if (is_stopping()) {
      dout(5) << ": exiting" << dendl;
      break;
    }

    locker.unlock();

    if (m_fs_mirror->is_blocklisted()) {
      dout(5) << ": exiting as client is blocklisted" << dendl;
      break;
    }

    locker.lock();

    auto now = clock::now();
    std::chrono::duration<double> timo = now - last_directory_scan;
    if (timo.count() >= scan_interval && m_directories.size()) {
      dout(20) << ": trying to pick from " << m_directories.size() << " directories" << dendl;
      auto dir_root = pick_directory();
      if (dir_root) {
        dout(5) << ": picked dir_root=" << *dir_root << dendl;
        int r = register_directory(*dir_root, replayer);
        if (r == 0) {
	  r = sync_perms(*dir_root);
	  if (r < 0) {
	    _inc_failed_count(*dir_root);
	  } else {
	    sync_snaps(*dir_root, locker);
	  }
	  unregister_directory(*dir_root);
        }
      }

      last_directory_scan = now;
    }
  }
}

void PeerReplayer::peer_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("stats");
  for (auto &[dir_root, sync_stat] : m_snap_sync_stats) {
    f->open_object_section(dir_root);
    if (sync_stat->failed) {
      f->dump_string("state", "failed");
    } else if (!sync_stat->current_syncing_snap) {
      f->dump_string("state", "idle");
    } else {
      f->dump_string("state", "syncing");
      f->open_object_section("current_sycning_snap");
      f->dump_unsigned("id", (*sync_stat->current_syncing_snap).first);
      f->dump_string("name", (*sync_stat->current_syncing_snap).second);
      sync_stat->current_stat.dump(f);
      f->close_section();
    }
    if (sync_stat->last_synced_snap) {
      f->open_object_section("last_synced_snap");
      f->dump_unsigned("id", (*sync_stat->last_synced_snap).first);
      f->dump_string("name", (*sync_stat->last_synced_snap).second);
      if (sync_stat->last_sync_duration) {
        f->dump_float("sync_duration", *sync_stat->last_sync_duration);
        f->dump_stream("sync_time_stamp") << sync_stat->last_synced;
      }
      sync_stat->last_stat.dump(f);
      f->close_section();
    }
    f->dump_unsigned("snaps_synced", sync_stat->synced_snap_count);
    f->dump_unsigned("snaps_deleted", sync_stat->deleted_snap_count);
    f->dump_unsigned("snaps_renamed", sync_stat->renamed_snap_count);
    f->close_section(); // dir_root
  }
  f->open_object_section("thread_pool_stats");
  task_sink_context.dump_stats(f);
  dir_op_handler_context.dump_stats(f);
  f->close_section();
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
