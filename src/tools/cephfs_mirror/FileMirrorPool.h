// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPHFS_MIRROR_FILE_MIRROR_POOL_H
#define CEPHFS_MIRROR_FILE_MIRROR_POOL_H


#include "common/Formatter.h"
#include "common/Thread.h"
#include "common/config_obs.h"


namespace cephfs {
namespace mirror {

class FileMirrorPool : public md_config_obs_t{
  public:
    FileMirrorPool(int num_threads);
    const char **get_tracked_conf_keys() const override;
    void handle_conf_change(const ConfigProxy &conf,
                            const std::set<std::string> &changed) override;
    void activate();
    void deactivate();
    void sync_file_data(Context *task, int sync_idx);
    int sync_start();
    void sync_finish(int idx);
    void update_state(int thread_count);
    void drain_queue(int idx = -1);

  private:
    void run(int idx);
    int num_threads;
    std::queue<int> sync_ring;
    std::vector<std::queue<Context *>> sync_queue;
    std::condition_variable pick_cv;
    std::condition_variable give_cv;
    std::mutex mtx;
    std::mutex config_mutex;
    int qlimit;
    std::vector<std::unique_ptr<std::thread>> workers;
    std::vector<bool> stop_thread;
    std::vector<int> unassigned_sync_ids;
    int sync_count = 0;
    bool active;
  };

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
