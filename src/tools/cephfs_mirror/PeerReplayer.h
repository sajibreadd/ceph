// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_PEER_REPLAYER_H
#define CEPHFS_MIRROR_PEER_REPLAYER_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "Types.h"
#include "common/Cond.h"
#include "FileMirrorPool.h"

#include <stack>
#include <boost/optional.hpp>

namespace cephfs {
namespace mirror {

class FSMirror;
class PeerReplayerAdminSocketHook;
class ServiceDaemon;

struct SnapSyncStat {
  std::atomic <uint64_t> nr_failures = 0;              // number of consecutive failures
  boost::optional<monotime> last_failed; // lat failed timestamp
  boost::optional<std::string> last_failed_reason;
  bool failed = false; // hit upper cap for consecutive failures
  boost::optional<std::pair<uint64_t, std::string>> last_synced_snap;
  boost::optional<std::pair<uint64_t, std::string>> current_syncing_snap;
  uint64_t synced_snap_count = 0;
  uint64_t deleted_snap_count = 0;
  uint64_t renamed_snap_count = 0;
  monotime last_synced = clock::zero();
  boost::optional<double> last_sync_duration;
  boost::optional<uint64_t>
      last_sync_bytes; // last sync bytes for display in status
  uint64_t sync_bytes =
      0; // sync bytes counter, independently for each directory sync.
  uint64_t dir_lock_key = 0;
  uint64_t generate64BitID() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    return dis(gen);
  }
  SnapSyncStat() : dir_lock_key(generate64BitID()) {}
  struct SyncStat {
    uint64_t rfiles;
    uint64_t rbytes;
    std::string diff_base = "";
    std::atomic<uint64_t> files_in_flight{0};
    std::atomic<uint64_t> files_synced{0};
    std::atomic<uint64_t> files_deleted{0};
    std::atomic<uint64_t> files_skipped{0};
    std::atomic<uint64_t> file_bytes_synced{0};
    std::atomic<uint64_t> dir_created{0};
    std::atomic<uint64_t> dir_deleted{0};
    std::atomic<uint64_t> dir_scanned{0};
    std::atomic<uint64_t> cache_hit{0};
    boost::optional<monotime> start_time;
    SyncStat() : start_time(boost::none) {}
    SyncStat(const SyncStat &other)
        : rfiles(other.rfiles), rbytes(other.rbytes),
          diff_base(other.diff_base),
          files_in_flight(other.files_in_flight.load()),
          files_synced(other.files_synced.load()),
          files_deleted(other.files_deleted.load()),
          files_skipped(other.files_skipped.load()),
          file_bytes_synced(other.file_bytes_synced.load()),
          dir_created(other.dir_created.load()),
          dir_deleted(other.dir_deleted.load()),
          dir_scanned(other.dir_scanned.load()),
          cache_hit(other.cache_hit.load()) {}
    SyncStat &operator=(const SyncStat &other) {
      if (this != &other) { // Self-assignment check
        rfiles = other.rfiles;
        rbytes = other.rbytes;
        diff_base = other.diff_base;
        files_in_flight.store(other.files_in_flight.load());
        files_synced.store(other.files_synced.load());
        files_deleted.store(other.files_deleted.load());
        files_skipped.store(other.files_skipped.load());
        file_bytes_synced.store(other.file_bytes_synced.load());
        dir_created.store(other.dir_created.load());
        dir_deleted.store(other.dir_deleted.load());
        dir_scanned.store(other.dir_scanned.load());
        cache_hit.store(other.cache_hit.load());
      }
      return *this;
    }
    inline void inc_cache_hit() { cache_hit++; }
    inline void inc_file_del_count() { files_deleted++; }
    inline void inc_file_skipped_count() { files_skipped++; }
    inline void inc_files_synced(bool data_synced, uint64_t file_size) {
      files_synced++;
      if (data_synced) {
        file_bytes_synced += file_size;
      }
    }
    inline void set_diff_base(const std::string &_diff_base) {
      diff_base = _diff_base;
    }
    inline void inc_file_in_flight_count() { files_in_flight++; }
    inline void dec_file_in_flight_count() { files_in_flight--; }
    inline void inc_dir_created_count() { dir_created++; }
    inline void inc_dir_deleted_count() { dir_deleted++; }
    inline void inc_dir_scanned_count() { dir_scanned++; }
    inline void start_timer() { start_time = clock::now(); }
    void dump(Formatter *f) {
      f->dump_unsigned("rfiles", rfiles);
      f->dump_unsigned("rbytes", rbytes);
      f->dump_string("diff_base", diff_base);
      f->dump_unsigned("files_in_flight", files_in_flight.load());
      f->dump_unsigned("files_synced", files_synced.load());
      f->dump_unsigned("files_deleted", files_deleted.load());
      f->dump_unsigned("files_skipped", files_skipped.load());
      f->dump_unsigned("files_bytes_synced", file_bytes_synced.load());
      f->dump_unsigned("dir_created", dir_created);
      f->dump_unsigned("dir_scanned", dir_scanned);
      f->dump_unsigned("dir_deleted", dir_deleted);
      f->dump_unsigned("cache_hit", cache_hit);
      if (start_time) {
        std::chrono::duration<double> duration = clock::now() - *start_time;
        double time_elapsed = duration.count();
        f->dump_float("time_elapsed", time_elapsed);
        double speed = 0;
        std::string syncing_speed = "";
        if (time_elapsed > 0) {
          speed = (file_bytes_synced * 8.0) / time_elapsed;
          if (speed >= (double)1e9) {
            syncing_speed = std::to_string(speed / (double)1e9) + "Gbps";
          } else if (speed >= (double)1e6) {
            syncing_speed = std::to_string(speed / (double)1e6) + "Mbps";
          } else if (speed >= (double)1e3) {
            syncing_speed = std::to_string(speed / (double)1e3) + "Kbps";
          } else {
            syncing_speed = std::to_string(speed) + "bps";
          }
          f->dump_string("transfer-rate", syncing_speed);
        }
      }
    }
  };
  SyncStat last_stat, current_stat;
  void reset_stats(int r) {
    if (r == 0) {
      last_stat = current_stat;
      last_stat.start_time = boost::none;
    }
    current_stat = SyncStat();
  }
  void dump_stats(Formatter *f) {
    if (failed) {
      f->dump_string("state", "failed");
      if (last_failed_reason) {
        f->dump_string("failure_reason", *last_failed_reason);
      }
    } else if (!current_syncing_snap) {
      f->dump_string("state", "idle");
    } else {
      f->dump_string("state", "syncing");
      f->open_object_section("current_syncing_snap");
      f->dump_unsigned("id", (*current_syncing_snap).first);
      f->dump_string("name", (*current_syncing_snap).second);
      current_stat.dump(f);
      f->close_section();
    }
    if (last_synced_snap) {
      f->open_object_section("last_synced_snap");
      f->dump_unsigned("id", (*last_synced_snap).first);
      f->dump_string("name", (*last_synced_snap).second);
      if (last_sync_duration) {
        f->dump_float("sync_duration", *last_sync_duration);
        f->dump_stream("sync_time_stamp") << last_synced;
      }
      if (last_sync_bytes) {
        f->dump_unsigned("sync_bytes", *last_sync_bytes);
      }
      last_stat.dump(f);
      f->close_section();
    }
    f->dump_unsigned("failure_count", nr_failures);
    f->dump_unsigned("snaps_synced", synced_snap_count);
    f->dump_unsigned("snaps_deleted", deleted_snap_count);
    f->dump_unsigned("snaps_renamed", renamed_snap_count);
  }
};

class SyncMechanism;
class DeleteMechanism;
class FileSyncMechanism;
class DirSyncMechanism;
class DirBruteDiffSync;
class DirSnapDiffSync;

class PeerReplayer : public md_config_obs_t {
public:
  PeerReplayer(CephContext *cct, FSMirror *fs_mirror, RadosRef local_cluster,
               const Filesystem &filesystem, const Peer &peer,
               const std::set<std::string, std::less<>> &directories,
               MountRef mount, ServiceDaemon *service_daemon,
               FileMirrorPool &file_mirror_pool);
  ~PeerReplayer();
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy &conf,
                          const std::set<std::string> &changed) override;

  // initialize replayer for a peer
  int init();

  // shutdown replayer for a peer
  void shutdown();

  // add a directory to mirror queue
  void add_directory(std::string_view dir_root);

  // remove a directory from queue
  void remove_directory(std::string_view dir_root);

  // admin socket helpers
  void peer_status(Formatter *f);

  // reopen logs
  void reopen_logs();
  friend class SyncMechanism;
  friend class DeleteMechanism;
  friend class FileSyncMechanism;
  friend class DirSyncMechanism;
  friend class DirBruteDiffSync;
  friend class DirSnapDiffSync;

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  inline static const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
  inline static const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";

  bool start_syncing = false;
  bool use_snapdiff_api = false;
  bool lock_directory_before_sync = false;
  uint64_t remote_timeout = 0;
  uint64_t local_timeout = 0;
  uint64_t dir_scanning_thread = 3;
  bool remote_diff_base_upon_start = false;
  bool sync_latest_snapshot = false;
  uint64_t thread_pool_queue_size = 5000;
  uint64_t max_element_in_cache_per_thread = 1000000;
  uint64_t max_consecutive_failures_before_remote_sync = 2;

      // file descriptor "triplet" for synchronizing a snapshot
  // w/ an added MountRef for accessing "previous" snapshot.
  using Snapshot = std::pair<std::string, uint64_t>;
  using SnapshotPair = std::pair<Snapshot, Snapshot>;
  class DirSyncPool;
  struct FHandles {
    // open file descriptor on the snap directory for snapshot
    // currently being synchronized. Always use this fd with
    // @m_local_mount.
    int c_fd = -1;

    // open file descriptor on the "previous" snapshot or on
    // dir_root on remote filesystem (based on if the snapshot
    // can be used for incremental transfer). Always use this
    // fd with p_mnt which either points to @m_local_mount (
    // for local incremental comparison) or @m_remote_mount (
    // for remote incremental comparison).
    int p_fd = -1;
    MountRef p_mnt;

    // open file descriptor on dir_root on remote filesystem.
    // Always use this fd with @m_remote_mount.
    int r_fd_dir_root = -1;
    Snapshot m_current;
    boost::optional<Snapshot> m_prev;
  };

  struct DirRegistry {
    int fd;
    std::atomic<bool> canceled = false;
    std::atomic<bool> failed = false;
    std::string dir_root;
    FHandles snapdiff_fh, remotediff_fh;
    std::atomic<int> failed_reason = 0;
    int file_sync_queue_idx = -2;
    std::unique_ptr<C_SaferCond> sync_finish_cond;
    std::atomic<int> sync_indicator = 0;
    std::unique_ptr<DirSyncPool> sync_pool = nullptr;
    void set_failed(int r) {
      ceph_assert(r < 0);
      if (failed) {
        return;
      }
      failed.store(true);
      failed_reason.store(r);
    }
    void sync_start(int _file_sync_queue_idx) {
      sync_indicator = 0;
      file_sync_queue_idx = _file_sync_queue_idx;
      sync_finish_cond = std::make_unique<C_SaferCond>();
    }
    void wait_for_sync_finish() { sync_finish_cond->wait(); }
    int get_file_sync_queue_idx() { return file_sync_queue_idx; }
    void inc_sync_indicator() { ++sync_indicator; }
    void dec_sync_indicator() {
      --sync_indicator;
      if (sync_indicator <= 0) {
        sync_finish_cond->complete(0);
      }
    }
  };

  struct SyncEntry {
    static const unsigned int PURGE_REMOTE = (1 << 20);
    static const unsigned int CREATE_FRESH = (1 << 21);
    static const unsigned int WASNT_DIR_IN_PREV_SNAPSHOT = (1 << 22);
    static const unsigned int CHANGE_MASK_POPULATED = (1 << 23);
    std::string epath;
    ceph_dir_result *dirp = nullptr; // valid for directories
    ceph_snapdiff_info info;
    struct ceph_statx stx;
    // set by incremental sync _after_ ensuring missing entries
    // in the currently synced snapshot have been propagated to
    // the remote filesystem.
    bool remote_synced = false;

    unsigned int change_mask = 0;
    bool is_snapdiff = false;
    ceph_snapdiff_entry_t deleted_sibling;
    bool deleted_sibling_exist = false;
    bool stat_known = false;

    SyncEntry() {}

    SyncEntry(std::string_view path) : epath(path) {}

    SyncEntry(std::string_view path, const struct ceph_statx &stx)
        : epath(path), stx(stx) {}
    SyncEntry(std::string_view path, ceph_dir_result *dirp,
              const struct ceph_statx &stx)
        : epath(path), dirp(dirp), stx(stx) {}
    SyncEntry(std::string_view path, ceph_dir_result *dirp,
              const struct ceph_statx &stx, unsigned int change_mask,
              bool is_snapdiff)
        : epath(path), dirp(dirp), stx(stx), change_mask(change_mask),
          is_snapdiff(is_snapdiff) {}
    SyncEntry(std::string_view path, const ceph_snapdiff_info &info,
              const struct ceph_statx &stx, unsigned int change_mask)
        : epath(path), info(info), stx(stx), change_mask(change_mask) {
      is_snapdiff = true;
    }

    bool is_directory() const { return S_ISDIR(stx.stx_mode); }

    bool needs_remote_sync() const { return !remote_synced; }
    void set_remote_synced() { remote_synced = true; }

    void set_is_snapdiff(bool f) { is_snapdiff = f; }

    bool sync_is_snapdiff() const { return is_snapdiff; }

    void set_stat_known() { stat_known = true; }

    void reset_change_mask() { change_mask = 0; }

    bool change_mask_populated() {
      return (change_mask & CHANGE_MASK_POPULATED);
    }

    bool purge_remote() { return (change_mask & PURGE_REMOTE); }
    bool create_fresh() { return (change_mask & CREATE_FRESH); }
    bool wasnt_dir_in_prev_snapshot() {
      return (change_mask & WASNT_DIR_IN_PREV_SNAPSHOT);
    }
  };

  class DirSyncPool {
  public:
    DirSyncPool(int num_threads, const std::string epath, const Peer &m_peer)
        : num_threads(num_threads), active(false), queued(0), qlimit(0),
          epath(epath), m_peer(m_peer) {}
    void activate();
    void deactivate();
    bool try_sync(SyncMechanism *task);
    void sync_anyway(SyncMechanism *task);
    void sync_direct(SyncMechanism *task);
    void update_state(int thread_count);
    void dump_stats(Formatter *f) {
      std::scoped_lock lock(mtx);
      f->dump_unsigned("dir_sync_queue_size", sync_queue.size());
    }
    void update_qlimit(int _qlimit);
    friend class PeerReplayer;

  private:
    struct DirScanner {
      bool stop_called = true;
      bool active = false;
      std::thread worker;
      void join() {
        if (worker.joinable()) {
          worker.join();
        }
      }
    };
    void run(DirScanner *dir_scanner);
    void drain_queue();
    bool _try_sync(SyncMechanism *task);
    int num_threads;
    std::queue<SyncMechanism *> sync_queue;
    std::vector <DirScanner*> dir_scanners;
    bool active;
    std::condition_variable pick_cv;
    std::mutex mtx;
    int queued = 0;
    int qlimit;
    std::string epath;
    const Peer &m_peer; // just for using dout
  };

  bool is_stopping() {
    return m_stopping;
  }

  struct Replayer;

  std::map<std::string, std::unique_ptr<std::thread>> thread_map;

  void sync_directory(
      std::string dir_root, std::map<uint64_t, std::string> snaps_deleted,
      std::map<uint64_t, std::pair<std::string, std::string>> snaps_renamed,
      Snapshot last_snap, Snapshot cur_snap);

  int max_concurrent_directory_syncs;
  int nr_replayers;
  std::unique_ptr <std::thread> scanner_thread;
  void run_scan();
  
  FileMirrorPool& file_mirror_pool;

  // stats sent to service daemon
  struct ServiceDaemonStats {
    uint64_t failed_dir_count = 0;
    uint64_t recovered_dir_count = 0;
  };

  void _set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->last_synced_snap = std::make_pair(snap_id, snap_name);
    sync_stat->current_syncing_snap = boost::none;
  }
  void set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->sync_bytes = 0;
  }
  void set_current_syncing_snap(const std::string &dir_root, uint64_t snap_id,
                                const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->current_syncing_snap = std::make_pair(snap_id, snap_name);
  }
  void clear_current_syncing_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->current_syncing_snap = boost::none;
  }
  void inc_deleted_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat->deleted_snap_count;
  }
  void inc_renamed_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat->renamed_snap_count;
  }
  void set_last_synced_stat(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name, double duration) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->last_synced = clock::now();
    sync_stat->last_sync_duration = duration;
    sync_stat->last_sync_bytes = sync_stat->sync_bytes;
    ++sync_stat->synced_snap_count;
  }
  void inc_sync_bytes(const std::string &dir_root, const uint64_t& b) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->sync_bytes += b;
  }


  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  RadosRef m_local_cluster;
  Filesystem m_filesystem;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry*> m_registered;
  std::vector<std::string> m_directories, m_deleted_directories;
  std::map<std::string, std::shared_ptr<SnapSyncStat>> m_snap_sync_stats;
  MountRef m_local_mount;
  ServiceDaemon *m_service_daemon;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
  std::mutex thread_map_lock;
  ceph::condition_variable m_cond;
  RadosRef m_remote_cluster;
  MountRef m_remote_mount;
  bool m_stopping = false;

  ServiceDaemonStats m_service_daemon_stats;

  PerfCounters *m_perf_counters;


  int register_directory(const std::string &dir_root);
  void unregister_directory(const std::string &dir_root);
  int try_lock_directory(const std::string &dir_root, DirRegistry *registry);
  void unlock_directory(const std::string &dir_root, DirRegistry* registry);
  int sync_snaps(const std::string &dir_root,
                 std::unique_lock<ceph::mutex> &locker);

  int build_snap_map(const std::string &dir_root, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);

  int propagate_snap_deletes(const std::string &dir_root,
                             const std::map<uint64_t, std::string> &snaps);
  int propagate_snap_renames(
      const std::string &dir_root,
      const std::map<uint64_t, std::pair<std::string, std::string>> &snaps);
  int propagate_deleted_entries(
      const std::string &epath, DirRegistry *registry,
      std::shared_ptr<SnapSyncStat> &sync_stat, const FHandles &fh,
      std::unordered_map<std::string, unsigned int> &change_mask_map,
      int &change_mask_map_size);
  int cleanup_remote_entry(const std::string &epath, DirRegistry *registry,
                           const FHandles &fh,
                           std::shared_ptr<SnapSyncStat> &sync_stat,
                           int not_dir = -1);

  int should_sync_entry(const std::string &epath, const struct ceph_statx &cstx,
                        const FHandles &fh, bool *need_data_sync, bool *need_attr_sync);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root,
                                      const Snapshot &current,
                                      boost::optional<Snapshot> prev,
                                      FHandles *snapdiff_fh,
                                      FHandles *remotediff_fh);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);
  int do_synchronize(const std::string &dir_root, const Snapshot &current) {
    return do_synchronize(dir_root, current, boost::none);
  }

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);
  int _do_sync_snaps(const std::string &dir_root, Snapshot cur_snap,
                     Snapshot last_snap);
  int do_sync_snaps(const std::string &dir_root, bool *nothing_to_sync);

  int sync_attributes(std::shared_ptr<SyncEntry> &cur_entry,
                      const FHandles &fh);

  int remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry, const FHandles &fh,
                   std::shared_ptr<SnapSyncStat> &sync_stat);
  int _remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry, const FHandles &fh,
                    std::shared_ptr<SnapSyncStat> &sync_stat);
  int remote_file_op(std::shared_ptr<SyncEntry> &cur_entry,
                     DirRegistry *registry,
                     std::shared_ptr<SnapSyncStat> &sync_stat,
                     const FHandles &fh);
  int copy_to_remote(std::shared_ptr<SyncEntry> &cur_entry,
                     DirRegistry *registry, const FHandles &fh,
                     FileMirrorPool::FileWorker *_file_worker);
  int sync_perms(const std::string& path);
  void build_change_mask(const struct ceph_statx &pstx,
                         const struct ceph_statx &cstx, bool create_fresh,
                         bool purge_remote, unsigned int &change_mask);
  void _inc_failed_count(const std::string &dir_root);
  void _reset_failed_count(const std::string &dir_root);
  bool should_backoff(DirRegistry* registry, int *retval);
  void enqueue_file_transfer(FileSyncMechanism *syncm, DirRegistry *registry,
                             std::shared_ptr<SnapSyncStat> &sync_stat);
  int64_t get_snap_id_attr(const std::string &dir_root,
                           const std::string &attr);
  int set_snap_id_attr(const std::string &dir_root, const std::string &attr,
                       uint64_t snap_id);
  void cleanup_idle_threads(std::unique_lock<ceph::mutex> &locker);
  int get_candidate_snap(const std::string &dir_root,
                         std::map<uint64_t, std::string> &local_snap_map,
                         std::map<uint64_t, std::string> &remote_snap_map,
                         SnapshotPair &snap_pair);
};

class SyncMechanism : public Context {
public:
  SyncMechanism(MountRef m_local,
                std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                PeerReplayer::DirRegistry *registry,
                std::shared_ptr<SnapSyncStat> &sync_stat,
                const PeerReplayer::FHandles &fh, PeerReplayer *replayer)
      : m_local(m_local), m_peer(replayer->m_peer),
        cur_entry(std::move(cur_entry)), registry(registry),
        sync_stat(sync_stat), fh(fh), replayer(replayer) {}

  void sync_in_flight() { registry->inc_sync_indicator(); }
  bool sync_failed_or_canceled() {
    return (registry->failed || registry->canceled);
  }
  std::shared_ptr<PeerReplayer::SyncEntry> &&move_cur_entry() {
    return std::move(cur_entry);
  }

protected:
  MountRef m_local;
  Peer &m_peer;
  std::shared_ptr<PeerReplayer::SyncEntry> cur_entry;
  PeerReplayer::DirRegistry *registry;
  std::shared_ptr<SnapSyncStat> &sync_stat;
  const PeerReplayer::FHandles &fh;
  PeerReplayer *replayer;
  int populate_change_mask(const PeerReplayer::FHandles &fh);
  int populate_current_stat(const PeerReplayer::FHandles &fh);
};

class DeleteMechanism : public SyncMechanism {
public:
  DeleteMechanism(MountRef m_local,
                  std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                  PeerReplayer::DirRegistry *registry,
                  std::shared_ptr<SnapSyncStat> &sync_stat,
                  const PeerReplayer::FHandles &fh, PeerReplayer *replayer,
                  int not_dir = -1)
      : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                      replayer),
        not_dir(not_dir) {}

private:
  void finish(int r) override;
  int not_dir = -1;
};

class FileSyncMechanism : public SyncMechanism {
public:
  FileSyncMechanism(MountRef m_local,
                    std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                    PeerReplayer::DirRegistry *registry,
                    std::shared_ptr<SnapSyncStat> &sync_stat,
                    const PeerReplayer::FHandles &fh, PeerReplayer *replayer)
      : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                      replayer) {}

  uint64_t get_file_size() { return cur_entry->stx.stx_size; }
  std::string get_file_name() {
    return registry->dir_root + "/" + cur_entry->epath;
  }
  std::string get_peer_uid() { return m_peer.uuid; }
  int get_file_sync_queue_idx() { return registry->get_file_sync_queue_idx(); }
  inline void set_worker_ref(FileMirrorPool::FileWorker* _file_worker) {
    file_worker = _file_worker;
  }

private:
  void finish(int r) override;
  int sync_file();
  FileMirrorPool::FileWorker* file_worker;
};

class DirSyncMechanism : public SyncMechanism {
public:
  DirSyncMechanism(MountRef m_local,
                   std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                   PeerReplayer::DirRegistry *registry,
                   std::shared_ptr<SnapSyncStat> &sync_stat,
                   const PeerReplayer::FHandles &fh, PeerReplayer *replayer)
      : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                      replayer) {}

protected:
  void finish(int r) override;
  int sync_tree();
  bool try_spawning(SyncMechanism *syncm);
  std::stack<std::shared_ptr<PeerReplayer::SyncEntry>> m_sync_stack;

private:
  virtual void finish_sync() = 0;
  virtual int go_next() = 0;
  virtual int sync_current_entry() = 0;
};

class DirBruteDiffSync : public DirSyncMechanism {
public:
  DirBruteDiffSync(MountRef m_local,
                   std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                   PeerReplayer::DirRegistry *registry,
                   std::shared_ptr<SnapSyncStat> &sync_stat,
                   const PeerReplayer::FHandles &fh, PeerReplayer *replayer)
      : DirSyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                         replayer) {}

private:
  std::stack<std::unordered_map<std::string, unsigned int>>
      m_change_mask_map_stack;
  int change_mask_map_size = 0;
  void finish_sync() override;
  int go_next() override;
  int sync_current_entry() override;
};

class DirSnapDiffSync : public DirSyncMechanism {
public:
  DirSnapDiffSync(MountRef m_local,
                  std::shared_ptr<PeerReplayer::SyncEntry> &&cur_entry,
                  PeerReplayer::DirRegistry *registry,
                  std::shared_ptr<SnapSyncStat> &sync_stat,
                  const PeerReplayer::FHandles &fh, PeerReplayer *replayer)
      : DirSyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                         replayer) {}

private:
  void finish_sync() override;
  int go_next() override;
  int sync_current_entry() override;
  bool should_delete_current_entry(int not_dir = -1);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H