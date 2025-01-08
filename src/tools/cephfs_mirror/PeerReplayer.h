// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_PEER_REPLAYER_H
#define CEPHFS_MIRROR_PEER_REPLAYER_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "ServiceDaemon.h"
#include "Types.h"

namespace cephfs {
namespace mirror {

class FSMirror;
class PeerReplayerAdminSocketHook;

class PeerReplayer {
public:
  PeerReplayer(CephContext *cct, FSMirror *fs_mirror,
               RadosRef local_cluster, const Filesystem &filesystem,
               const Peer &peer, const std::set<std::string, std::less<>> &directories,
               MountRef mount, ServiceDaemon *service_daemon);
  ~PeerReplayer();

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

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  inline static const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
  inline static const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";

  using Snapshot = std::pair<std::string, uint64_t>;

  // file descriptor "triplet" for synchronizing a snapshot
  // w/ an added MountRef for accessing "previous" snapshot.
  struct FHandles {
    // open file descriptor on the snap directory for snapshot
    // currently being synchronized. Always use this fd with
    // @m_local_mount.
    int c_fd;

    // open file descriptor on the "previous" snapshot or on
    // dir_root on remote filesystem (based on if the snapshot
    // can be used for incremental transfer). Always use this
    // fd with p_mnt which either points to @m_local_mount (
    // for local incremental comparison) or @m_remote_mount (
    // for remote incremental comparison).
    int p_fd;
    MountRef p_mnt;

    // open file descriptor on dir_root on remote filesystem.
    // Always use this fd with @m_remote_mount.
    int r_fd_dir_root;
  };

  bool is_stopping() {
    return m_stopping.load();
  }

  class FileTransferHandlerThreadPool {
  public:
    struct ThreadStats {
      int id;
      std::string current_syncing_file = "";
      uint64_t current_file_size = 0;
      uint64_t current_file_synced_bytes = 0;
      double current_file_transfer_rate = 0.0;
      std::string last_synced_file = "";
      uint64_t last_synced_file_size = 0;
      ceph::coarse_mono_clock::time_point start;
      double last_sync_duration = 0.0;
      std::unique_ptr <std::mutex> mtx;
      ThreadStats() {}
      ThreadStats(int id) : id(id), mtx(std::make_unique<std::mutex>()) {}
      ThreadStats(const ThreadStats&) = delete;
      ThreadStats& operator=(const ThreadStats&) = delete;
      ThreadStats& operator=(ThreadStats&&) = delete;
      ThreadStats(ThreadStats &&thread_stats)
          : id(thread_stats.id),
            current_syncing_file(std::move(thread_stats.current_syncing_file)),
            current_file_size(thread_stats.current_file_size),
            current_file_synced_bytes(thread_stats.current_file_synced_bytes),
            current_file_transfer_rate(thread_stats.current_file_transfer_rate),
            last_synced_file(std::move(thread_stats.last_synced_file)),
            last_synced_file_size(thread_stats.last_synced_file_size),
            start(thread_stats.start),
            last_sync_duration(thread_stats.last_sync_duration),
            mtx(std::move(thread_stats.mtx)) {}
      void new_transfer(const std::string &file_path,
                              uint64_t file_size) {
        std::lock_guard <std::mutex> lock(*mtx);
        start = ceph::coarse_mono_clock::now();
        current_syncing_file = file_path;
        current_file_size = file_size;
        current_file_synced_bytes = 0;
        current_file_transfer_rate = 0.0;
      }
      void roll_over(int r) {
        std::lock_guard <std::mutex> lock(*mtx);
        if (r >= 0) {
          last_synced_file = current_syncing_file;
          last_synced_file_size = current_file_size;
          auto end = ceph::coarse_mono_clock::now();
          last_sync_duration =
              std::chrono::duration<double, std::milli>(end - start).count();
        }
        current_syncing_file = "";
        current_file_size = 0;
        current_file_synced_bytes = 0;
        current_file_transfer_rate = 0.0;
      }
      void inc_sync_bytes(uint64_t r) {
        current_file_synced_bytes += r;
        auto end = ceph::coarse_mono_clock::now();
        double elapsed =
            std::chrono::duration<double, std::micro>(end - start).count();
        if (elapsed > 0) {
          current_file_transfer_rate =
              (current_file_synced_bytes / elapsed) * (double)1e6;
        }
      }
      void dump(Formatter *f) {
        std::lock_guard <std::mutex> lock(*mtx);
        f->open_object_section(std::to_string(id));
        f->dump_int("thread_no", id);
        f->open_object_section("current_syncing_file");
        f->dump_string("file_path", current_syncing_file);
        f->dump_unsigned("file_size", current_file_size);
        f->dump_unsigned("synced_bytes", current_file_synced_bytes);
        f->dump_float("transfer_rate(bytes/s)", current_file_transfer_rate);
        f->close_section();
        f->open_object_section("last_synced_file");
        f->dump_string("file_path", last_synced_file);
        f->dump_unsigned("file_size", last_synced_file_size);
        f->dump_float("sync_duration(ms)", last_sync_duration);
        f->close_section();
        f->close_section();
      }
    };
    struct ThreadPoolStats {
      uint64_t total_bytes_queued = 0;
      int large_file_queued = 0;
      std::vector <ThreadStats> thread_stats;
      FileTransferHandlerThreadPool *file_transfer_context;
      static const uint64_t large_file_threshold = 4194304;
      void add_file(uint64_t file_size) {
        large_file_queued += (file_size >= large_file_threshold);
        total_bytes_queued += file_size;
      }
      void remove_file(uint64_t file_size) {
        large_file_queued -= (file_size >= large_file_threshold);
        total_bytes_queued -= file_size;
      }
      ThreadPoolStats(){}
      ThreadPoolStats(int num_threads,
                      FileTransferHandlerThreadPool *file_transfer_context)
          : file_transfer_context(file_transfer_context) {
        for (int i = 0; i < num_threads; ++i) {
          thread_stats.emplace_back(std::move(ThreadStats(i)));
        }
      }
      void dump(Formatter* f) {
        f->open_object_section("thread_pool_stats");
        f->dump_int("queued_file_count",
                    file_transfer_context->task_queue.size());
        f->dump_int("queued_large_file_count", large_file_queued);
        f->dump_unsigned("bytes_queued_for_transfer", total_bytes_queued);
        f->open_array_section("thread_stats");
        for (auto& it : thread_stats) {
          it.dump(f);
        }
        f->close_section();
        f->close_section();
      }
    };
    FileTransferHandlerThreadPool() {}
    FileTransferHandlerThreadPool(int _num_threads);
    void activate();
    void deactivate();
    template <class F>
    void enqueue(std::atomic<int64_t> &counter, std::condition_variable &cv,
                 F &&f, uint64_t file_size);
    void dump_stats(Formatter* f) {
      std::scoped_lock lock (pmtx);
      thread_pool_stats.dump(f);
    }
    friend class PeerReplayer;
    friend class ThreadPoolStats;

  private:
    void run(int idx);
    void drain_queue();
    std::queue<std::pair<std::function<void(bool, int)>, uint64_t>> task_queue;
    std::condition_variable pick_task;
    std::condition_variable give_task;
    std::mutex pmtx;
    static const int task_queue_limit = 100000;
    std::vector<std::thread> workers;
    std::atomic<bool> stop_flag;
    ThreadPoolStats thread_pool_stats;
  };
  FileTransferHandlerThreadPool file_transfer_context;

  struct Replayer;
  class SnapshotReplayerThread : public Thread {
  public:
    SnapshotReplayerThread(PeerReplayer *peer_replayer)
      : m_peer_replayer(peer_replayer) {
    }

    void *entry() override {
      m_peer_replayer->run(this);
      return 0;
    }

  private:
    PeerReplayer *m_peer_replayer;
  };

  struct DirRegistry {
    int fd;
    std::atomic<bool> canceled = false;
    SnapshotReplayerThread *replayer;
    std::atomic<bool> failed = false;
    int failed_reason;
    DirRegistry() {}
    DirRegistry(const DirRegistry &other)
        : fd(other.fd), canceled(other.canceled.load()),
          replayer(other.replayer), failed(other.failed.load()),
          failed_reason(other.failed_reason) {}

    DirRegistry &operator=(const DirRegistry &other) {
      if (this != &other) { // Self-assignment check
        fd = other.fd;
        canceled.store(other.canceled.load());
        replayer = other.replayer;
        failed.store(other.failed.load());
        failed_reason = other.failed_reason;
      }
      return *this;
    }
  };

  struct SyncEntry {
    std::string epath;
    ceph_dir_result *dirp; // valid for directories
    struct ceph_statx stx;
    // set by incremental sync _after_ ensuring missing entries
    // in the currently synced snapshot have been propagated to
    // the remote filesystem.
    bool remote_synced = false;

    SyncEntry(std::string_view path,
              const struct ceph_statx &stx)
      : epath(path),
        stx(stx) {
    }
    SyncEntry(std::string_view path,
              ceph_dir_result *dirp,
              const struct ceph_statx &stx)
      : epath(path),
        dirp(dirp),
        stx(stx) {
    }

    bool is_directory() const {
      return S_ISDIR(stx.stx_mode);
    }

    bool needs_remote_sync() const {
      return remote_synced;
    }
    void set_remote_synced() {
      remote_synced = true;
    }
  };

  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  // stats sent to service daemon
  struct ServiceDaemonStats {
    uint64_t failed_dir_count = 0;
    uint64_t recovered_dir_count = 0;
  };

  struct SnapSyncStat {
    uint64_t nr_failures = 0; // number of consecutive failures
    boost::optional<time> last_failed; // lat failed timestamp
    bool failed = false; // hit upper cap for consecutive failures
    boost::optional<std::pair<uint64_t, std::string>> last_synced_snap;
    boost::optional<std::pair<uint64_t, std::string>> current_syncing_snap;
    uint64_t synced_snap_count = 0;
    uint64_t deleted_snap_count = 0;
    uint64_t renamed_snap_count = 0;
    time last_synced = clock::zero();
    boost::optional<double> last_sync_duration;
    std::string rfiles;
    std::string rbytes;
    std::atomic<uint64_t> files_in_flight{0};
    std::atomic<uint64_t> files_op[2][2] = {0};
    std::atomic<uint64_t> files_deleted{0};
    std::atomic<uint64_t> large_files_in_flight{0};
    std::atomic<uint64_t> large_files_op[2][2] = {0};
    std::atomic<uint64_t> large_files_deleted{0};
    std::atomic<uint64_t> file_bytes_synced{0};
    static const uint64_t large_file_threshold = 4194304;
    SnapSyncStat() {}
    SnapSyncStat(const SnapSyncStat& other)
        : nr_failures(other.nr_failures),
          last_failed(other.last_failed),
          failed(other.failed),
          last_synced_snap(other.last_synced_snap),
          current_syncing_snap(other.current_syncing_snap),
          synced_snap_count(other.synced_snap_count),
          deleted_snap_count(other.deleted_snap_count),
          renamed_snap_count(other.renamed_snap_count),
          last_synced(other.last_synced),
          last_sync_duration(other.last_sync_duration),
          rfiles(other.rfiles),
          rbytes(other.rbytes),
          files_in_flight(other.files_in_flight.load()),
          files_deleted(other.files_deleted.load()),
          large_files_in_flight(other.large_files_in_flight.load()),
          large_files_deleted(other.large_files_deleted.load()),
          file_bytes_synced(other.file_bytes_synced.load())
    {
      for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < 2; ++j) {
          files_op[i][j].store(other.files_op[i][j].load());
          large_files_op[i][j].store(other.large_files_op[i][j].load());
        }
      }
    }

    SnapSyncStat& operator=(const SnapSyncStat& other) {
        if (this != &other) {  // Self-assignment check
            nr_failures = other.nr_failures;
            last_failed = other.last_failed;
            failed = other.failed;
            last_synced_snap = other.last_synced_snap;
            current_syncing_snap = other.current_syncing_snap;
            synced_snap_count = other.synced_snap_count;
            deleted_snap_count = other.deleted_snap_count;
            renamed_snap_count = other.renamed_snap_count;
            last_synced = other.last_synced;
            last_sync_duration = other.last_sync_duration;
            rfiles = other.rfiles;
            rbytes = other.rbytes;
            files_in_flight.store(other.files_in_flight.load());
            files_deleted.store(other.files_deleted.load());
            large_files_in_flight.store(other.large_files_in_flight.load());
            large_files_deleted.store(other.large_files_deleted.load());
            file_bytes_synced.store(other.file_bytes_synced.load());
            for (size_t i = 0; i < 2; ++i) {
              for (size_t j = 0; j < 2; ++j) {
                files_op[i][j].store(other.files_op[i][j].load());
                large_files_op[i][j].store(other.large_files_op[i][j].load());
              }
            }
        }
        return *this;
    }
    void reset_file_stats() {
      files_in_flight.store(0);
      files_deleted.store(0);
      large_files_in_flight.store(0);
      large_files_deleted.store(0);
      file_bytes_synced.store(0);
      for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < 2; ++j) {
          files_op[i][j].store(0);
          large_files_op[i][j].store(0);
        }
      }
    }
    void inc_file_del_count(uint64_t file_size) {
      files_deleted++;
      if (file_size >= large_file_threshold) {
        large_files_deleted++;
      }
    }
    void inc_file_op_count(bool data_synced, bool attr_synced,
                           uint64_t file_size) {
      files_op[data_synced][attr_synced]++;
      if (file_size >= large_file_threshold) {
        large_files_op[data_synced][attr_synced]++;
      }
      if (data_synced) {
        file_bytes_synced.fetch_add(file_size, std::memory_order_relaxed);
      }
    }
    void inc_file_in_flight_count(uint64_t file_size) {
      files_in_flight++;
      if (file_size >= large_file_threshold) {
        large_files_in_flight++;
      }
    }
    void dec_file_in_flight_count(uint64_t file_size) {
      files_in_flight--;
      if (file_size >= large_file_threshold) {
        large_files_in_flight--;
      }
    }
  };

  void _inc_failed_count(const std::string &dir_root) {
    auto max_failures = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_consecutive_failures_per_directory");
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_failed = clock::now();
    if (++sync_stat.nr_failures >= max_failures && !sync_stat.failed) {
      sync_stat.failed = true;
      ++m_service_daemon_stats.failed_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_FAILED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.failed_dir_count);
    }
  }
  void _reset_failed_count(const std::string &dir_root) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    if (sync_stat.failed) {
      ++m_service_daemon_stats.recovered_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.recovered_dir_count);
    }
    sync_stat.nr_failures = 0;
    sync_stat.failed = false;
    sync_stat.last_failed = boost::none;
  }

  void _set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_synced_snap = std::make_pair(snap_id, snap_name);
    sync_stat.current_syncing_snap = boost::none;
  }
  void set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
  }
  void set_current_syncing_snap(const std::string &dir_root, uint64_t snap_id,
                                const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.current_syncing_snap = std::make_pair(snap_id, snap_name);
  }
  void clear_current_syncing_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.current_syncing_snap = boost::none;
  }
  void inc_deleted_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat.deleted_snap_count;
  }
  void inc_renamed_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat.renamed_snap_count;
  }
  void set_last_synced_stat(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name, double duration) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_synced = clock::now();
    sync_stat.last_sync_duration = duration;
    ++sync_stat.synced_snap_count;
  }
  
  bool should_backoff(std::atomic<bool> &canceled,
                      std::atomic<bool> &failed, int *retval) {
    if (m_fs_mirror->is_blocklisted()) {
      *retval = -EBLOCKLISTED;
      return true;
    }

    if (m_stopping.load()) {
      // ceph defines EBLOCKLISTED to ESHUTDOWN (108). so use
      // EINPROGRESS to identify shutdown.
      *retval = -EINPROGRESS;
      return true;
    }
    if (canceled || failed) {
      *retval = -ECANCELED;
      return true;
    }

    *retval = 0;
    return false;
  }

  int get_failed_reason(const std::string& dir_root) {
    std::scoped_lock lock(m_lock);
    auto& dr = m_registered.at(dir_root);
    return dr.failed_reason;
  }

  void mark_failed(const std::string& dir_root, int reason) {
    std::scoped_lock lock(m_lock);
    auto it = m_registered.find(dir_root);
    if (it == m_registered.end()) {
      return;
    }
    if (it->second.failed) {
      return;
    }
    it->second.failed = true;
    it->second.failed_reason = reason;
  }

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  RadosRef m_local_cluster;
  Filesystem m_filesystem;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry> m_registered;
  std::vector<std::string> m_directories;
  std::map<std::string, SnapSyncStat> m_snap_sync_stats;
  MountRef m_local_mount;
  ServiceDaemon *m_service_daemon;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
  ceph::condition_variable m_cond;
  RadosRef m_remote_cluster;
  MountRef m_remote_mount;
  std::atomic<bool> m_stopping = false;
  SnapshotReplayers m_replayers;

  ServiceDaemonStats m_service_daemon_stats;

  PerfCounters *m_perf_counters;

  void run(SnapshotReplayerThread *replayer);

  boost::optional<std::string> pick_directory();
  int register_directory(const std::string &dir_root, SnapshotReplayerThread *replayer);
  void unregister_directory(const std::string &dir_root);
  int try_lock_directory(const std::string &dir_root, SnapshotReplayerThread *replayer,
                         DirRegistry *registry);
  void unlock_directory(const std::string &dir_root, const DirRegistry &registry);
  void sync_snaps(const std::string &dir_root, std::unique_lock<ceph::mutex> &locker);


  int build_snap_map(const std::string &dir_root, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);

  int propagate_snap_deletes(const std::string &dir_root, const std::set<std::string> &snaps);
  int propagate_snap_renames(const std::string &dir_root,
                             const std::set<std::pair<std::string,std::string>> &snaps);
  int propagate_deleted_entries(const std::string &dir_root,
                                const std::string &epath, const FHandles &fh,
                                std::atomic<bool> &canceled,
                                std::atomic<bool> &failed,
                                SnapSyncStat &dir_sync_stat);
  int cleanup_remote_dir(const std::string &dir_root, const std::string &epath,
                         const FHandles &fh, std::atomic<bool> &canceled,
                         std::atomic<bool> &failed,
                         SnapSyncStat &dir_sync_stat);

  int should_sync_entry(const std::string &epath, const struct ceph_statx &cstx,
                        const FHandles &fh, bool *need_data_sync, bool *need_attr_sync);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root, const Snapshot &current,
                                      boost::optional<Snapshot> prev, FHandles *fh);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);
  int do_sync_snaps(const std::string &dir_root);

  int remote_mkdir(const std::string &epath, const struct ceph_statx &stx, const FHandles &fh);
  int remote_file_op(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     bool need_data_sync, bool need_attr_sync,
                     std::atomic<int64_t> &rem_file_transfer,
                     std::condition_variable &cv, std::atomic<bool> &canceled,
                     std::atomic<bool> &failed, SnapSyncStat &dir_sync_stat);
  int sync_attributes_of_file(const std::string &dir_root,
                              const std::string &epath,
                              const struct ceph_statx &stx, const FHandles &fh);
  int copy_to_remote(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     std::atomic<bool> &canceled,
                     std::atomic<bool> &failed,
                     int thread_idx);
  int sync_perms(const std::string& path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
