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
  int r = ceph_openat(mnt, dirfd, relpath.c_str(), flags, 0);
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
      file_transfer_context(g_ceph_context->_conf.get_val<uint64_t>(
          "cephfs_mirror_max_concurrent_file_transfer")) {
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
    m_snap_sync_stats.emplace(dir_root, SnapSyncStat());
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
             "cephfs_mirror_max_concurrent_file_transfer="
          << g_ceph_context->_conf.get_val<uint64_t>(
                 "cephfs_mirror_max_concurrent_file_transfer")
          << ", cephfs_mirror_max_concurrent_directory_syncs="
          << g_ceph_context->_conf.get_val<uint64_t>(
                 "cephfs_mirror_max_concurrent_directory_syncs")
          << dendl;
  file_transfer_context.activate();
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

  return 0;
}

void PeerReplayer::shutdown() {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    ceph_assert(!m_stopping);
    m_stopping = true;
    m_cond.notify_all();
  }

  file_transfer_context.deactivate();
  dout(0) << ": File transfer thread pool deactivated" << dendl;
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
  m_snap_sync_stats.emplace(dir_root, SnapSyncStat());
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
    it1->second.canceled = true;
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
    if (sync_stat.failed) {
      std::chrono::duration<double> d = now - *sync_stat.last_failed;
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

  DirRegistry registry;
  int r = try_lock_directory(dir_root, replayer, &registry);
  if (r < 0) {
    return r;
  }

  dout(4) << ": dir_root=" << dir_root << " registered with replayer="
          << replayer
          << " fd = " << registry.fd
          << dendl;
  m_registered.emplace(dir_root, std::move(registry));
  return 0;
}

void PeerReplayer::unregister_directory(const std::string &dir_root) {
  dout(4) << ": dir_root=" << dir_root << dendl;

  auto it = m_registered.find(dir_root);
  ceph_assert(it != m_registered.end());

  dout(4) << ": dir_root=" << dir_root
          << " fd = " << it->second.fd
          << dendl;

  unlock_directory(it->first, it->second);
  m_registered.erase(it);
  if (std::find(m_directories.begin(), m_directories.end(), dir_root) == m_directories.end()) {
    m_snap_sync_stats.erase(dir_root);
  }
}

int PeerReplayer::try_lock_directory(const std::string &dir_root,
                                     SnapshotReplayerThread *replayer, DirRegistry *registry) {
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

  r = ceph_closedir(mnt, dirp);
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
                                  uint64_t change_mask, bool is_dir,
                                  const FHandles &fh) {
  int r = 0;
  if ((change_mask & CEPH_STATX_UID) || (change_mask & CEPH_STATX_GID)) {
    // dout(0) << ": epath-->" << epath << ", ceph_chownat" << dendl;
    r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                     stx.stx_uid, stx.stx_gid, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chown remote directory=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if ((change_mask & CEPH_STATX_MODE)) {
    // dout(0) << ": epath-->" << epath << ", ceph_chmodat" << dendl;
    r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                     stx.stx_mode & ~S_IFMT, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chmod remote directory=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (!is_dir && (change_mask & CEPH_STATX_MTIME)) {
    // dout(0) << ": epath-->" << epath << ", ceph_utimensat" << dendl;
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
  // dout(0) << ": dir_created-->" << epath << ": " << cpp_strerror(r) << dendl;
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int PeerReplayer::remote_mkdir(const std::string &epath,
                               const struct ceph_statx &cstx,
                               bool *newly_created, int create,
                               uint64_t change_mask, const FHandles &fh,
                               SnapSyncStat &dir_sync_stat) {
  dout(10) << ": remote epath=" << epath << dendl;
  int r = 0;

  if (create == 1) { // must need to create
    r = _remote_mkdir(epath, cstx, fh);
    if (r < 0) {
      return r;
    }
    *newly_created = true;
    dir_sync_stat.current_stat.inc_dir_created_count();
    change_mask =
        (CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID | CEPH_STATX_MTIME);
  } else if (create == 2) { // I am confused, I need to check the stat
    struct ceph_statx pstx;
    r = ceph_statxat(fh.p_mnt, fh.p_fd, epath.c_str(), &pstx,
                     CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                         CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    // dout(0) << ": dir_stated-->" << epath << ": " << cpp_strerror(r) << ", "
    //         << (pstx.stx_mode & S_IFMT) << ", " << (cstx.stx_mode & S_IFMT)
    //         << dendl;
    if (r < 0 && r != -ENOENT && r != -ENOTDIR) {
      derr << ": failed to stat prev entry= " << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    if (r < 0 || (pstx.stx_mode & S_IFMT) != (cstx.stx_mode & S_IFMT)) {
      r = _remote_mkdir(epath, cstx, fh);
      if (r < 0) {
        return r;
      }
      *newly_created = true;
      dir_sync_stat.current_stat.inc_dir_created_count();
      change_mask = (CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                     CEPH_STATX_MTIME);
    } else {
      update_change_mask(cstx, pstx, change_mask);
    }
  }

  // dout(0) << ": epath-->" << epath << ", create=" << create
  //         << ", change_mask=" << change_mask << dendl;
  r = sync_attributes(epath, cstx, change_mask, true, fh);
  return r;
}

PeerReplayer::FileTransferHandlerThreadPool::FileTransferHandlerThreadPool(
    int _num_threads)
    : workers(_num_threads), stop_flag(true),
      thread_pool_stats(_num_threads, this) {}

void PeerReplayer::FileTransferHandlerThreadPool::activate() {
  ceph_assert(stop_flag);
  stop_flag = false;
  for (int i = 0; i < workers.size(); ++i){
    workers[i] = std::thread(&FileTransferHandlerThreadPool::run, this, i);
  }
}

void PeerReplayer::FileTransferHandlerThreadPool::deactivate() {
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

void PeerReplayer::FileTransferHandlerThreadPool::drain_queue() {
  std::scoped_lock lock(pmtx);
  while (!task_queue.empty()) {
    auto& [task, file_size] = task_queue.front();
    task(true, -1);
    task_queue.pop();
  }
}


void PeerReplayer::FileTransferHandlerThreadPool::run(int idx) {

  while (true) {
    std::function<void(bool, int)> task;
    uint64_t file_size;
    {
      std::unique_lock<std::mutex> lock(pmtx);
      pick_task.wait(
          lock, [this] { return (stop_flag || !task_queue.empty()); });
      if (stop_flag) {
        return;
      }
      std::tie(task, file_size) = std::move(task_queue.front());
      task_queue.pop();
      thread_pool_stats.remove_file(file_size);
      give_task.notify_one();
    }
    task(false, idx);
  }
}

template <class F>
void PeerReplayer::FileTransferHandlerThreadPool::enqueue(
    std::atomic<int64_t> &counter, std::condition_variable &dir_cv, F &&f,
    uint64_t file_size) {
  std::unique_lock<std::mutex> lock(pmtx);
  give_task.wait(lock, [this] {
    return (stop_flag || task_queue.size() < task_queue_limit);
  });
  if (stop_flag) {
    return;
  }
  counter++;
  task_queue.emplace(
      [f = std::forward<F>(f), &counter, &dir_cv](bool dont_execute,
                                                  int thread_idx) mutable {
        if (!dont_execute) {
          f(thread_idx);
        }
        --counter;
        if (counter == 0) {
          dir_cv.notify_one();
        }
      },
      file_size);
  thread_pool_stats.add_file(file_size);
  pick_task.notify_one();
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::copy_to_remote(const std::string &dir_root,
                                 const std::string &epath,
                                 const struct ceph_statx &stx,
                                 const FHandles &fh,
                                 std::atomic<bool> &canceled,
                                 std::atomic<bool> &failed,
                                 int thread_idx) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;

  auto &stats =
      file_transfer_context.thread_pool_stats.thread_stats[thread_idx];
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

  stats.new_transfer(full_path, stx.stx_size);
  while (true) {
    if (should_backoff(canceled, failed, &r)) {
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
    stats.inc_sync_bytes(r);
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
  // dout(0) << ": file transfer finished-->" << dir_root << "/" << epath << dendl;
  stats.roll_over(r);
  return r == 0 ? 0 : r;
}

int PeerReplayer::remote_file_op(
    const std::string &dir_root, const std::string &epath,
    const struct ceph_statx &stx, bool need_data_sync, uint64_t change_mask,
    std::atomic<int64_t> &rem_file_transfer, std::condition_variable &cv,
    const FHandles &fh, std::atomic<bool> &canceled, std::atomic<bool> &failed,
    SnapSyncStat &dir_sync_stat) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << ", need_data_sync=" << need_data_sync
           << ", attr_change_mask=" << change_mask << dendl;

  int r;
  if (need_data_sync) {
    if (S_ISREG(stx.stx_mode)) {
      auto task = [&dir_root, epath, stx, &fh, change_mask, &canceled,
                   &failed, &dir_sync_stat, this](int thread_idx) {
        int r = copy_to_remote(dir_root, epath, stx, fh, canceled, failed,
                               thread_idx);
        if (r < 0) {
          if (!failed) {
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
        if (change_mask) {
          r = sync_attributes(epath, stx, change_mask, false, fh);
        }
        if (r < 0 && !failed) {
          mark_failed(dir_root, r);
        }
        dir_sync_stat.current_stat.inc_file_op_count(true, (change_mask > 0),
                                                     stx.stx_size);
        dir_sync_stat.current_stat.dec_file_in_flight_count(stx.stx_size);
      };
      file_transfer_context.enqueue(rem_file_transfer, cv, task, stx.stx_size);
      dir_sync_stat.current_stat.inc_file_in_flight_count(stx.stx_size);
      // task();
      return 0;
    } else if (S_ISLNK(stx.stx_mode)) {
      // dout(0) << ": link-->" << epath << dendl;
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
    } else {
      dout(5) << ": skipping entry=" << epath << ": unsupported mode=" << stx.stx_mode
              << dendl;
      dir_sync_stat.current_stat.inc_file_op_count(false, false, stx.stx_size);
      return 0;
    }
  }

  if (change_mask) {
    r = sync_attributes(epath, stx, change_mask, false, fh);
    if (r < 0) {
      return r;
    }
  }
  dir_sync_stat.current_stat.inc_file_op_count(need_data_sync,
                                               (change_mask > 0), stx.stx_size);

  return 0;
}

int PeerReplayer::cleanup_remote_dir(const std::string &dir_root,
                                     const std::string &epath,
                                     const FHandles &fh,
                                     std::atomic<bool> &canceled,
                                     std::atomic<bool> &failed,
                                     SnapSyncStat &dir_sync_stat) {
  dout(20) << ": dir_root=" << dir_root << ", epath=" << epath
           << dendl;

  struct ceph_statx tstx;
  int r = ceph_statxat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), &tstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                           CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                       AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat remote directory=" << epath << ": "
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
    if (should_backoff(canceled, failed, &r)) {
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
                               CEPH_STATX_MODE, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
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
        dir_sync_stat.current_stat.inc_dir_deleted_count();

        dout(10) << ": done for remote directory=" << entry.epath << dendl;
        if (ceph_closedir(m_remote_mount, entry.dirp) < 0) {
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
      dir_sync_stat.current_stat.inc_file_del_count(entry.stx.stx_size);
      rm_stack.pop();
    }
  }

  while (!rm_stack.empty()) {
    auto &entry = rm_stack.top();
    if (entry.is_directory()) {
      dout(20) << ": closing remote directory=" << entry.epath << dendl;
      if (ceph_closedir(m_remote_mount, entry.dirp) < 0) {
        derr << ": failed to close remote directory=" << entry.epath << dendl;
      }
    }

    rm_stack.pop();
  }

  return r;
}

int PeerReplayer::should_sync_entry(const std::string &epath,
                                    const struct ceph_statx &cstx,
                                    const FHandles &fh, bool *need_data_sync,
                                    uint64_t &change_mask) {
  dout(10) << ": epath=" << epath << dendl;

  *need_data_sync = false;
  change_mask = 0;
  struct ceph_statx pstx;
  int r = ceph_statxat(fh.p_mnt, fh.p_fd, epath.c_str(), &pstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                           CEPH_STATX_SIZE | CEPH_STATX_MTIME,
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
    change_mask =
        (CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID | CEPH_STATX_MTIME);
    return 0;
  }

  dout(10) << ": local cur statx: mode=" << cstx.stx_mode
           << ", uid=" << cstx.stx_uid << ", gid=" << cstx.stx_gid
           << ", size=" << cstx.stx_size << ", ctime=" << cstx.stx_ctime
           << ", mtime=" << cstx.stx_mtime << dendl;
  dout(10) << ": local prev statx: mode=" << pstx.stx_mode
           << ", uid=" << pstx.stx_uid << ", gid=" << pstx.stx_gid
           << ", size=" << pstx.stx_size << ", ctime=" << pstx.stx_ctime
           << ", mtime=" << pstx.stx_mtime << dendl;
  if ((cstx.stx_mode & S_IFMT) != (pstx.stx_mode & S_IFMT)) {
    dout(5) << ": entry=" << epath << " has mode mismatch" << dendl;
    *need_data_sync = true;
    change_mask =
        (CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID | CEPH_STATX_MTIME);
  } else {
    *need_data_sync =
        (cstx.stx_size != pstx.stx_size) || (cstx.stx_mtime != pstx.stx_mtime);
    update_change_mask(cstx, pstx, change_mask);
    if (*need_data_sync) {
      change_mask = change_mask | CEPH_STATX_MTIME;
    }
  }
  return 0;
}

void PeerReplayer::update_change_mask(const struct ceph_statx &cstx,
                                      const struct ceph_statx &pstx, uint64_t& change_mask) {
  if ((cstx.stx_mode & ~S_IFMT) != (pstx.stx_mode & ~S_IFMT)) {
    change_mask = change_mask | CEPH_STATX_MODE;
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

int PeerReplayer::propagate_deleted_entries(
    const std::string &dir_root, const std::string &epath,
    std::unordered_map<std::string, std::pair<bool, uint64_t>>
        &common_subdir_info,
    uint64_t &common_subdir_info_count, const FHandles &fh,
    std::atomic<bool> &canceled, std::atomic<bool> &failed,
    SnapSyncStat &dir_sync_stat) {
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
    if (should_backoff(canceled, failed, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }
    unsigned int extra_flags =
        (common_subdir_info_count < PER_THREAD_SUBDIR_THRESH)
            ? (CEPH_STATX_UID | CEPH_STATX_GID | CEPH_STATX_SIZE |
               CEPH_STATX_MTIME)
            : 0;

    r = ceph_readdirplus_r(fh.p_mnt, dirp, &de, &pstx,
                           CEPH_STATX_MODE | extra_flags,
                           AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
    if (r < 0) {
      derr << ": failed to read directory entries: " << cpp_strerror(r)
           << dendl;
      if (r == -ENOENT) {
        r = -EINVAL;
      }
      break;
    }
    if (r == 0) {
      dout(10) << ": reached EOD" << dendl;
      break;
    }
    auto d_name = std::string(de.d_name);
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
    if (r == 0) {
      // directory entry present in both snapshots -- check inode
      // type
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
    // dout(0) << ": propagate checking-->" << dpath
    //         << ", purge_remote=" << purge_remote << ", "
    //         << (pstx.stx_mode & S_IFMT) << ", " << (cstx.stx_mode & S_IFMT)
    //         << dendl;

    if (purge_remote) {
      dout(5) << ": purging remote entry=" << dpath << dendl;
      if (S_ISDIR(pstx.stx_mode)) {
        r = cleanup_remote_dir(dir_root, dpath, fh, canceled, failed,
                               dir_sync_stat);
      } else {
        r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, dpath.c_str(), 0);
        dir_sync_stat.current_stat.inc_file_del_count(cstx.stx_size);
      }

      if (r < 0 && r != -ENOENT) {
        derr << ": failed to cleanup remote entry=" << d_name << ": "
             << cpp_strerror(r) << dendl;
        break;
      }
    } else if (extra_flags) {
      uint64_t change_mask = 0;
      update_change_mask(cstx, pstx, change_mask);
      if (S_ISDIR(cstx.stx_mode)) {
        common_subdir_info[d_name] = {false, change_mask};
      } else {
        bool need_data_sync = (cstx.stx_size != pstx.stx_size) ||
                              (cstx.stx_mtime != pstx.stx_mtime);
        if (need_data_sync) {
          change_mask = change_mask | CEPH_STATX_MTIME;
        }
        common_subdir_info[d_name] = {need_data_sync, change_mask};
      }
      ++common_subdir_info_count;
    }
  }

  ceph_closedir(fh.p_mnt, dirp);
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

  bool first_sync = false;
  {
    std::scoped_lock locker(m_lock);
    auto& stat = m_snap_sync_stats.at(dir_root);
    first_sync = (stat.synced_snap_count == 0);
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
    fh->r_fd_dir_root = it->second.fd;
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

void PeerReplayer::do_dir_sync(
    const std::string &dir_root, const std::string &cur_path,
    const struct ceph_statx &cstx, ceph_dir_result *dirp, bool entry_known,
    bool need_data_sync, uint64_t change_mask,
    uint64_t common_subdir_info_count, bool any_ancestor_newly_created,
    std::atomic<int64_t> &rem_file_transfer, std::condition_variable &cv,
    const FHandles &fh, std::atomic<bool> &canceled, std::atomic<bool> &failed,
    SnapSyncStat &dir_sync_stat) {
  int r = 0;
  bool dont_close = (cur_path == ".");
  struct ceph_statx child_stx;
  struct dirent child_de;
  std::string child_dname, child_ename, child_path;
  std::unordered_map<std::string, std::pair<bool, uint64_t>> common_subdir_info;
  bool newly_created = false;

  if (should_backoff(canceled, failed, &r)) {
    dout(0) << ": backing off r=" << r << dendl;
    goto safe_exit;
  }
  if (!dont_close) {
    if (S_ISDIR(cstx.stx_mode)) {
      // dout(0) << "checkdir1-->" << cur_path << ", "
      //         << "entry_known=" << entry_known << ", "
      //         << "need_data_sync=" << need_data_sync
      //         << ", change_mask=" << change_mask << dendl;
      r = remote_mkdir(cur_path, cstx, &newly_created,
                       any_ancestor_newly_created ? 1 : (entry_known ? 0 : 2),
                       change_mask, fh, dir_sync_stat);
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
      dir_sync_stat.current_stat.inc_dir_scanned_count();
    } else {
      // dout(0) << ": check1-->" << cur_path << ", entry_known=" << entry_known
      //         << ", need_data_sync=" << need_data_sync
      //         << ", change_mask=" << change_mask << dendl;
      if (!entry_known) {
        
        r = should_sync_entry(cur_path, cstx, fh, &need_data_sync, change_mask);
        if (r < 0) {
          goto sanity_check;
        }
      }

      // dout(0) << ": check2-->" << cur_path << ", entry_known=" << entry_known
      //         << ", need_data_sync=" << need_data_sync
      //         << ", change_mask=" << change_mask << dendl;
      dout(5) << ": entry=" << cur_path << ", data_sync=" << need_data_sync
              << ", attr_change_mask=" << change_mask << dendl;
      if (need_data_sync || change_mask) {
        r = remote_file_op(dir_root, cur_path, cstx, need_data_sync,
                           change_mask, rem_file_transfer, cv, fh, canceled,
                           failed, dir_sync_stat);
        if (r < 0) {
          goto sanity_check;
        }
      } else {
        dir_sync_stat.current_stat.inc_file_op_count(0, 0, cstx.stx_size);
      }
      goto sanity_check;
    }
  }

  if (!newly_created && !any_ancestor_newly_created) {
    r = propagate_deleted_entries(dir_root, cur_path, common_subdir_info,
                                  common_subdir_info_count, fh, canceled,
                                  failed, dir_sync_stat);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to propagate missing dirs: " << cpp_strerror(r)
           << dendl;
      goto safe_exit;
    }
  }

  while (true) {
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
    // dout(0) << ": child_path-->" << child_path << dendl;
    bool child_need_sync_data = false;
    uint64_t child_change_mask = 0;
    bool child_known = false;
    auto it = common_subdir_info.find(child_dname);
    if (it != common_subdir_info.end()) {
      child_known = true;
      child_need_sync_data = it->second.first;
      child_change_mask = it->second.second;
      // dout(0) << ": cache hit-->" << child_path << ", "
      //         << common_subdir_info_count << ", " << child_need_sync_data
      //         << ", " << child_change_mask << dendl;
      common_subdir_info.erase(it);
      --common_subdir_info_count;
    }

    do_dir_sync(dir_root, child_path, child_stx, nullptr, child_known,
                child_need_sync_data, child_change_mask,
                common_subdir_info_count,
                any_ancestor_newly_created | newly_created, rem_file_transfer,
                cv, fh, canceled, failed, dir_sync_stat);
  }

safe_exit:
  if (!dont_close && dirp && ceph_closedir(m_local_mount, dirp) < 0) {
    derr << ": failed to close local directory=" << cur_path << dendl;
  }

sanity_check:
  if (r < 0 && !failed) {
    mark_failed(dir_root, r);
  }
}

int PeerReplayer::do_synchronize(const std::string &dir_root, const Snapshot &current,
                                 boost::optional<Snapshot> prev) {
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
  void* buf_file_count = malloc(20);
  std::string rfiles = "";
  int x1 = ceph_getxattr(m_local_mount, dir_root.c_str(), "ceph.dir.rfiles",
                        buf_file_count, 20);
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
  int x2 = ceph_getxattr(m_local_mount, dir_root.c_str(), "ceph.dir.rbytes",
                        buf_file_bytes, 20);
  if (x2 < 0) {
    derr << ": failed to read ceph.dir.rfiles xattr for directory=" << dir_root
         << dendl;
  } else {
    rbytes = std::move(std::string((char*)buf_file_bytes, x2));
    dout(0) << ": xattr ceph.dir.rbytes=" << rbytes
            << " for directory=" << dir_root << dendl;
  }

  std::atomic<int64_t> rem_file_transfer(0);
  std::condition_variable all_file_transfer_done;
  std::unique_lock lock(m_lock);
  auto &dr = m_registered.at(dir_root);
  dr.failed = false, dr.failed_reason = 0;
  std::atomic<bool> &canceled = dr.canceled;
  std::atomic<bool> &failed = dr.failed;
  auto &dir_sync_stat = m_snap_sync_stats.at(dir_root);
  dir_sync_stat.current_stat.rfiles = std::stoull(rfiles);
  dir_sync_stat.current_stat.rbytes = std::stoull(rbytes);
  lock.unlock();

  // C_DoDirSync *task =
  //     new C_DoDirSync(dir_root, ".", tstx, tdirp, false, false, 0, 0, fh,
  //                     canceled, failed, op_counter, &fin, this,
  //                     dir_sync_stat);

  do_dir_sync(dir_root, ".", tstx, tdirp, false, false, 0, 0, false,
              rem_file_transfer, all_file_transfer_done, fh, canceled, failed,
              dir_sync_stat);

  if (r < 0 && !failed) {
    mark_failed(dir_root, r);
  }

  std::mutex dummy_mtx;
  std::unique_lock<std::mutex> dummy_lock(dummy_mtx);
  all_file_transfer_done.wait(dummy_lock, [&rem_file_transfer] {
    return (rem_file_transfer <= 0);
  });

  if (ceph_closedir(m_local_mount, tdirp) < 0) {
    derr << ": failed to close local directory=." << dendl;
  }

  if (r >= 0 && failed) {
    r = get_failed_reason(dir_root);
  }
  lock.lock();
  dir_sync_stat.reset_stats();
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
    set_current_syncing_snap(dir_root, it->first, it->second);
    auto start = clock::now();
    boost::optional<Snapshot> prev = boost::none;
    if (last_snap_id != 0) {
      prev = std::make_pair(last_snap_name, last_snap_id);
    }
    r = synchronize(dir_root, std::make_pair(it->second, it->first), prev);
    if (r < 0) {
      derr << ": failed to synchronize dir_root=" << dir_root
           << ", snapshot=" << it->second << dendl;
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

    set_last_synced_stat(dir_root, it->first, it->second, duration.count());
    if (--snaps_per_cycle == 0) {
      break;
    }

    last_snap_name = it->second;
    last_snap_id = it->first;
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
    if (sync_stat.failed) {
      f->dump_string("state", "failed");
    } else if (!sync_stat.current_syncing_snap) {
      f->dump_string("state", "idle");
    } else {
      f->dump_string("state", "syncing");
      f->open_object_section("current_sycning_snap");
      f->dump_unsigned("id", (*sync_stat.current_syncing_snap).first);
      f->dump_string("name", (*sync_stat.current_syncing_snap).second);
      sync_stat.current_stat.dump(f);
      f->close_section();
    }
    if (sync_stat.last_synced_snap) {
      f->open_object_section("last_synced_snap");
      f->dump_unsigned("id", (*sync_stat.last_synced_snap).first);
      f->dump_string("name", (*sync_stat.last_synced_snap).second);
      if (sync_stat.last_sync_duration) {
        f->dump_float("sync_duration", *sync_stat.last_sync_duration);
        f->dump_stream("sync_time_stamp") << sync_stat.last_synced;
      }
      sync_stat.last_stat.dump(f);
      f->close_section();
    }
    f->dump_unsigned("snaps_synced", sync_stat.synced_snap_count);
    f->dump_unsigned("snaps_deleted", sync_stat.deleted_snap_count);
    f->dump_unsigned("snaps_renamed", sync_stat.renamed_snap_count);
    f->close_section(); // 
  }
  file_transfer_context.dump_stats(f);
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
