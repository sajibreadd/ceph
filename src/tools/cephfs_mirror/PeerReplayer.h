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

  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  struct CommonEntryInfo {
    bool is_dir = false;
    bool purge_remote = false;
    unsigned int change_mask = 0;
    CommonEntryInfo() : is_dir(false), purge_remote(false), change_mask(0) {}
    CommonEntryInfo(bool is_dir, bool purge_remote, unsigned int change_mask)
        : is_dir(is_dir), purge_remote(purge_remote), change_mask(change_mask) {
    }
    CommonEntryInfo(const CommonEntryInfo &other)
        : is_dir(other.is_dir), purge_remote(other.purge_remote),
          change_mask(other.change_mask) {}
    CommonEntryInfo &operator=(const CommonEntryInfo &other) {
      if (this != &other) {
        is_dir = other.is_dir;
        purge_remote = other.purge_remote;
        change_mask = other.change_mask;
      }
      return *this;
    }
  };

  struct SnapSyncStat {
    uint64_t nr_failures = 0;          // number of consecutive failures
    boost::optional<time> last_failed; // lat failed timestamp
    bool failed = false;               // hit upper cap for consecutive failures
    boost::optional<std::pair<uint64_t, std::string>> last_synced_snap;
    boost::optional<std::pair<uint64_t, std::string>> current_syncing_snap;
    uint64_t synced_snap_count = 0;
    uint64_t deleted_snap_count = 0;
    uint64_t renamed_snap_count = 0;
    time last_synced = clock::zero();
    boost::optional<double> last_sync_duration;
    struct SyncStat {
      uint64_t rfiles;
      uint64_t rbytes;
      std::atomic<uint64_t> files_in_flight{0};
      std::atomic<uint64_t> files_op[2][2] = {0};
      std::atomic<uint64_t> files_deleted{0};
      std::atomic<uint64_t> large_files_in_flight{0};
      std::atomic<uint64_t> large_files_op[2][2] = {0};
      std::atomic<uint64_t> file_bytes_synced{0};
      std::atomic<uint64_t> dir_created{0};
      std::atomic<uint64_t> dir_deleted{0};
      std::atomic<uint64_t> dir_scanned{0};
      SyncStat() {}
      SyncStat(const SyncStat &other)
          : rfiles(other.rfiles), rbytes(other.rbytes),
            files_in_flight(other.files_in_flight.load()),
            files_deleted(other.files_deleted.load()),
            large_files_in_flight(other.large_files_in_flight.load()),
            file_bytes_synced(other.file_bytes_synced.load()),
            dir_created(other.dir_created.load()),
            dir_deleted(other.dir_deleted.load()),
            dir_scanned(other.dir_scanned.load()) {
        for (size_t i = 0; i < 2; ++i) {
          for (size_t j = 0; j < 2; ++j) {
            files_op[i][j].store(other.files_op[i][j].load());
            large_files_op[i][j].store(other.large_files_op[i][j].load());
          }
        }
      }
      SyncStat &operator=(const SyncStat &other) {
        if (this != &other) { // Self-assignment check
          rfiles = other.rfiles;
          rbytes = other.rbytes;
          files_in_flight.store(other.files_in_flight.load());
          files_deleted.store(other.files_deleted.load());
          large_files_in_flight.store(other.large_files_in_flight.load());
          file_bytes_synced.store(other.file_bytes_synced.load());
          dir_created.store(other.dir_created.load());
          dir_deleted.store(other.dir_deleted.load());
          dir_scanned.store(other.dir_scanned.load());
          for (size_t i = 0; i < 2; ++i) {
            for (size_t j = 0; j < 2; ++j) {
              files_op[i][j].store(other.files_op[i][j].load());
              large_files_op[i][j].store(other.large_files_op[i][j].load());
            }
          }
        }
        return *this;
      }
      void inc_file_del_count() {
        files_deleted++;
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
      void inc_dir_created_count() { dir_created++; }
      void inc_dir_deleted_count() { dir_deleted++; }
      void inc_dir_scanned_count() { dir_scanned++; }
      void dump(Formatter *f) {
        f->dump_unsigned("rfiles", rfiles);
        f->dump_unsigned("rbytes", rbytes);
        f->dump_unsigned("files_bytes_synced", file_bytes_synced.load());
        f->dump_unsigned("files_in_flight", files_in_flight.load());
        f->dump_unsigned("files_deleted", files_deleted.load());
        f->dump_unsigned("files_data_attr_synced", files_op[1][1].load());
        f->dump_unsigned("files_data_synced", files_op[1][0].load());
        f->dump_unsigned("files_attr_synced", files_op[0][1].load());
        f->dump_unsigned("files_skipped", files_op[0][0].load());
        f->dump_unsigned("large_files_in_flight", large_files_in_flight.load());
        f->dump_unsigned("large_files_data_attr_synced",
                         large_files_op[1][1].load());
        f->dump_unsigned("large_files_data_synced",
                         large_files_op[1][0].load());
        f->dump_unsigned("large_files_attr_synced",
                         large_files_op[0][1].load());
        f->dump_unsigned("large_files_skipped", large_files_op[0][0].load());
        f->dump_unsigned("dir_scanned", dir_scanned);
        f->dump_unsigned("dir_created", dir_created);
        f->dump_unsigned("dir_deleted", dir_deleted);
      }
    };
    SyncStat last_stat, current_stat;

    static const uint64_t large_file_threshold = 4194304;
    SnapSyncStat() {}
    SnapSyncStat(const SnapSyncStat &other)
        : nr_failures(other.nr_failures), last_failed(other.last_failed),
          failed(other.failed), last_synced_snap(other.last_synced_snap),
          current_syncing_snap(other.current_syncing_snap),
          synced_snap_count(other.synced_snap_count),
          deleted_snap_count(other.deleted_snap_count),
          renamed_snap_count(other.renamed_snap_count),
          last_synced(other.last_synced),
          last_sync_duration(other.last_sync_duration),
          last_stat(other.last_stat),
          current_stat(other.current_stat) {}

    SnapSyncStat &operator=(const SnapSyncStat &other) {
      if (this != &other) { // Self-assignment check
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
        last_stat = other.last_stat;
        current_stat = other.current_stat;
      }
      return *this;
    }
    void reset_stats() {
      last_stat = current_stat;
      current_stat = SyncStat();
    }
  };

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

  using Snapshot = std::pair<std::string, uint64_t>;

  class C_MirrorContext : public Context {
  public:
    C_MirrorContext(std::atomic<int64_t> &op_counter, Context *fin,
                    PeerReplayer *replayer, SnapSyncStat &dir_sync_stat)
        : op_counter(op_counter), fin(fin), replayer(replayer),
          m_peer(replayer->m_peer), dir_sync_stat(dir_sync_stat) {}
    template <typename F>
    C_MirrorContext(std::atomic<int64_t> &op_counter, Context *fin,
                    PeerReplayer *replayer, SnapSyncStat &dir_sync_stat, F &&f)
        : op_counter(op_counter), fin(fin), replayer(replayer),
          m_peer(replayer->m_peer), task(std::move(f)),
          dir_sync_stat(dir_sync_stat) {}
    virtual void finish(int r) = 0;
    void complete(int r) override {
      finish(r);
      dec_counter();
      delete this;
    }
    void inc_counter() { ++op_counter; }
    virtual void add_into_stat() {}
    virtual void remove_from_stat() {}

  protected:
    std::atomic<int64_t> &op_counter;
    Context *fin;
    PeerReplayer *replayer;
    std::function<void(std::atomic<int64_t> &, Context *)> task;
    Peer &m_peer; // just for using dout
    SnapSyncStat& dir_sync_stat;

  private:
    void dec_counter() {
      --op_counter;
      if (op_counter <= 0) {
        fin->complete(0);
      }
    }
  };

  class C_DoDirSync;
  class OpHandlerThreadPool;

  class C_TransferAndSyncFile : public C_MirrorContext {
  public:
    C_TransferAndSyncFile(const std::string &dir_root, const std::string &epath,
                          const struct ceph_statx &stx,
                          unsigned int change_mask, const FHandles &fh,
                          std::atomic<bool> &canceled,
                          std::atomic<bool> &failed,
                          std::atomic<int64_t> &op_counter, Context *fin,
                          PeerReplayer *replayer, SnapSyncStat &dir_sync_stat)
        : dir_root(dir_root), epath(epath), stx(stx), change_mask(change_mask),
          fh(fh), canceled(canceled), failed(failed),
          C_MirrorContext(op_counter, fin, replayer, dir_sync_stat) {}

    void finish(int r) override;
    void add_into_stat() override;
    void remove_from_stat() override;
    friend class OpHandlerThreadPool;

  private:
    const std::string &dir_root;
    std::string epath;
    struct ceph_statx stx;
    unsigned int change_mask;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class OpHandlerThreadPool {
  public:
    struct ThreadPoolStats {
      uint64_t total_bytes_queued = 0;
      int large_file_queued = 0;
      static const uint64_t large_file_threshold = 4194304;
      OpHandlerThreadPool* op_handler_context;
      void add_file(uint64_t file_size) {
        large_file_queued += (file_size >= large_file_threshold);
        total_bytes_queued += file_size;
      }
      void remove_file(uint64_t file_size) {
        large_file_queued -= (file_size >= large_file_threshold);
        total_bytes_queued -= file_size;
      }
      ThreadPoolStats() {}
      ThreadPoolStats(int num_threads, OpHandlerThreadPool *op_handler_context)
          : op_handler_context(op_handler_context) {
      }
      void dump(Formatter *f) {
        f->open_object_section("thread_pool_stats");
        f->dump_int("queued_file_count",
                    op_handler_context->file_task_queue.size());
        f->dump_int("queued_large_file_count", large_file_queued);
        f->dump_unsigned("bytes_queued_for_transfer", total_bytes_queued);
        f->dump_int("queued_dir_ops_count",
                    op_handler_context->other_task_queue.size());
        f->close_section();
      }
    };

    OpHandlerThreadPool() {}
    OpHandlerThreadPool(int _num_file_threads, int _num_other_threads);
    void activate();
    void deactivate();
    void do_file_task_async(C_MirrorContext *task);
    bool do_other_task_async(C_MirrorContext *task);
    void handle_other_task_force(C_MirrorContext *task);
    bool handler_other_task_async(C_MirrorContext *task);
    void handle_other_task_sync(C_MirrorContext *task);
    void dump_stats(Formatter* f) {
      // std::scoped_lock lock(fmtx, omtx);
      thread_pool_stats.dump(f);
    }
    std::atomic<int> fcount, ocount;
    friend class ThreadPoolStats;
    friend class C_TransferAndSyncFile;
    friend class PeerReplayer;

  private:
    void run_file_task();
    void run_other_task();
    void drain_queue();
    std::queue<C_MirrorContext *> file_task_queue;
    std::queue<C_MirrorContext *> other_task_queue;
    std::condition_variable pick_file_task;
    std::condition_variable give_file_task;
    std::condition_variable pick_other_task;
    std::mutex fmtx, omtx;
    int file_task_queue_limit;
    int other_task_queue_limit;
    std::vector<std::thread> file_workers;
    std::vector<std::thread> other_workers;
    std::atomic<bool> stop_flag;
    ThreadPoolStats thread_pool_stats;
  };
  OpHandlerThreadPool op_handler_context;

  class C_DoDirSync : public C_MirrorContext {
  public:
    C_DoDirSync(const std::string &dir_root, const std::string &cur_path,
                const struct ceph_statx &cstx, ceph_dir_result *dirp,
                bool create_fresh, bool entry_info_known,
                const CommonEntryInfo &entry_info,
                uint64_t common_entry_info_count, const FHandles &fh,
                std::atomic<bool> &canceled, std::atomic<bool> &failed,
                std::atomic<int64_t> &op_counter, Context *fin,
                PeerReplayer *replayer, SnapSyncStat &dir_sync_stat)
        : dir_root(dir_root), cur_path(cur_path), cstx(cstx), dirp(dirp),
          create_fresh(create_fresh), entry_info_known(entry_info_known),
          entry_info(entry_info),
          common_entry_info_count(common_entry_info_count), fh(fh),
          canceled(canceled), failed(failed),
          C_MirrorContext(op_counter, fin, replayer, dir_sync_stat) {}
    void finish(int r) override;
    void set_common_entry_info_count(uint64_t _common_entry_info_count) {
      common_entry_info_count = _common_entry_info_count;
    }

  private:
    const std::string &dir_root;
    std::string cur_path;
    struct ceph_statx cstx;
    ceph_dir_result *dirp;
    bool create_fresh;
    bool entry_info_known;
    CommonEntryInfo entry_info;
    uint64_t common_entry_info_count;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class C_CleanUpRemoteDir : public C_MirrorContext {
  public:
    C_CleanUpRemoteDir(const std::string &dir_root, const std::string &epath,
                       const FHandles &fh,
                       std::atomic<bool> &canceled,
                       std::atomic<bool> &failed,
                       std::atomic<int64_t> &op_counter, Context *fin,
                       PeerReplayer *replayer, SnapSyncStat &dir_sync_stat)
        : dir_root(dir_root), epath(epath), fh(fh), canceled(canceled),
          failed(failed),
          C_MirrorContext(op_counter, fin, replayer, dir_sync_stat) {}

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class C_DeleteFile : public C_MirrorContext {
  public:
    C_DeleteFile(const std::string &dir_root, const std::string &epath,
                 const FHandles &fh, std::atomic<bool> &canceled,
                 std::atomic<bool> &failed, std::atomic<int64_t> &op_counter,
                 Context *fin, PeerReplayer *replayer,
                 SnapSyncStat &dir_sync_stat)
        : dir_root(dir_root), epath(epath), fh(fh), canceled(canceled),
          failed(failed),
          C_MirrorContext(op_counter, fin, replayer, dir_sync_stat) {}

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  friend class C_MirrorContext;
  friend class C_DoDirSync;
  friend class C_TransferFile;
  friend class C_CleanUpRemoteDir;
  friend class C_DeleteFile;

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  inline static const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
  inline static const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";
  static const uint64_t PER_THREAD_SUBDIR_THRESH = 100000;

  bool is_stopping() {
    return m_stopping;
  }

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
    ceph_snapdiff_info* sd_info;
    ceph_snapdiff_entry_t sd_entry;
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
    SyncEntry(std::string_view path, ceph_snapdiff_info *sd_info,
              const ceph_snapdiff_entry_t &sd_entry,
              const struct ceph_statx &stx)
        : epath(path), sd_info(sd_info), sd_entry(sd_entry), stx(stx) {}

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

  // stats sent to service daemon
  struct ServiceDaemonStats {
    uint64_t failed_dir_count = 0;
    uint64_t recovered_dir_count = 0;
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

    if (m_stopping) {
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

  int get_failed_reason(const std::string &dir_root) {
    std::scoped_lock lock(m_lock);
    auto &dr = m_registered.at(dir_root);
    return dr.failed_reason;
  }

  void mark_failed(const std::string &dir_root, int reason) {
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

  int delete_file(const std::string &dir_root, const std::string &epath,
                  const FHandles &fh, std::atomic<bool> &canceled,
                  std::atomic<bool> &failed, SnapSyncStat &dir_sync_stat);

  int propagate_deleted_entries(
      const std::string &dir_root, const std::string &epath,
      std::unordered_map<std::string, CommonEntryInfo> &common_entry_info,
      uint64_t &common_entry_info_count, const FHandles &fh,
      std::atomic<bool> &canceled, std::atomic<bool> &failed,
      std::atomic<int64_t> &op_counter, Context *fin,
      SnapSyncStat &dir_sync_stat);
  int cleanup_remote_dir(const std::string &dir_root, const std::string &epath,
                         const FHandles &fh,
                         std::atomic<bool> &canceled,
                         std::atomic<bool> &failed,
                         SnapSyncStat &dir_sync_stat);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root, const Snapshot &current,
                                      boost::optional<Snapshot> prev, FHandles *fh);

  int _remote_mkdir(const std::string &epath, const struct ceph_statx &cstx,
                    const FHandles &fh);

  int remote_mkdir(const std::string &epath, const struct ceph_statx &cstx,
                   bool create_fresh, unsigned int change_mask,
                   const FHandles &fh, SnapSyncStat &dir_sync_stat);

  void build_change_mask(const struct ceph_statx &pstx,
                         const struct ceph_statx &cstx, bool create_fresh,
                         unsigned int &change_mask);

  void do_dir_sync(const std::string &dir_root, const std::string &cur_path,
                   const struct ceph_statx &cstx, ceph_dir_result *dirp,
                   bool create_fresh, bool stat_known,
                   CommonEntryInfo &entry_info,
                   uint64_t common_entry_info_count, const FHandles &fh,
                   std::atomic<bool> &canceled, std::atomic<bool> &failed,
                   std::atomic<int64_t> &op_counter, Context *fin,
                   SnapSyncStat &dir_sync_stat);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);

  void do_dir_sync_using_snapdiff(
      const std::string &dir_root, const std::string &cur_path,
      const struct ceph_statx &cur_stx, ceph_snapdiff_info *snapdiff_info,
      const FHandles &fh, const Snapshot &current, const Snapshot &prev,
      std::atomic<bool> &canceled,
      std::atomic<bool> &failed,
      std::atomic<int64_t> &op_counter, Context *fin);

  int do_synchronize(const std::string &dir_root, const Snapshot &current);

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);
  int do_sync_snaps(const std::string &dir_root);

  int remote_file_op(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, bool need_data_sync,
                     unsigned int change_mask, const FHandles &fh,
                     std::atomic<bool> &canceled, std::atomic<bool> &failed,
                     std::atomic<int64_t> &op_counter, Context *fin,
                     SnapSyncStat &dir_sync_stat);

  int sync_attributes(const std::string &epath, const struct ceph_statx &stx,
                      unsigned int change_mask, bool is_dir,
                      const FHandles &fh);

  int copy_to_remote(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     std::atomic<bool> &canceled,
                     std::atomic<bool> &failed);

  void transfer_and_sync_file(const std::string &dir_root,
                              const std::string &epath,
                              const struct ceph_statx &stx,
                              unsigned int change_mask, const FHandles &fh,
                              std::atomic<bool> &canceled,
                              std::atomic<bool> &failed,
                              SnapSyncStat &dir_sync_stat);
  int sync_perms(const std::string &path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
