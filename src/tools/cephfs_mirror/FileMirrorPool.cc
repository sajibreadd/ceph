// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "Utils.h"

#include "FileMirrorPool.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::FileMirrorPool " << __func__

namespace cephfs {
namespace mirror {

FileMirrorPool::FileMirrorPool(int num_threads)
    : num_threads(num_threads), qlimit(std::max(10 * num_threads, 5000)),
      stop_thread(num_threads, true), sync_count(0), active(false) {
  g_conf().add_observer(this);
}

void FileMirrorPool::activate() {
  {
    std::scoped_lock lock(mtx);
    active = true;
    for (int i = 0; i < num_threads; ++i) {
      stop_thread[i] = false;
    }
  }
  for (int i = 0; i < num_threads; ++i) {
    workers.emplace_back(
        std::make_unique<std::thread>(&FileMirrorPool::run, this, i));
  }
  dout(0) << ": " << num_threads << " number of threads are activated" << dendl;
}

void FileMirrorPool::deactivate() {
  {
    std::scoped_lock lock(mtx);
    active = false;
    for (int i = 0; i < workers.size(); ++i) {
      stop_thread[i] = true;
    }
  }
  pick_cv.notify_all();
  give_cv.notify_all();
  for (auto &worker : workers) {
    if (worker->joinable()) {
      worker->join();
    }
  }
  drain_queue();
}

void FileMirrorPool::drain_queue(int idx) {
  auto cleanup_queue = [this](int i) {
    while (!sync_queue[i].empty()) {
      auto &task = sync_queue[i].front();
      task->complete(-1);
      sync_queue[i].pop();
    }
  };
  {
    std::scoped_lock lock(mtx);
    if (idx == -1) {
      for (int i = 0; i < sync_queue.size(); ++i) {
        cleanup_queue(i);
      }
    } else {
      cleanup_queue(idx);
    }
  }
  pick_cv.notify_all();
  give_cv.notify_all();
}

void FileMirrorPool::run(int thread_idx) {
  while (true) {
    Context *task;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_cv.wait(lock, [this, thread_idx] {
        return (stop_thread[thread_idx] || !sync_ring.empty());
      });
      if (stop_thread[thread_idx]) {
        return;
      }
      int sync_idx = sync_ring.front();
      sync_ring.pop();
      ceph_assert(sync_idx < sync_queue.size());

      if (sync_queue[sync_idx].empty()) {
        continue;
      }

      task = sync_queue[sync_idx].front();
      sync_queue[sync_idx].pop();
      if (!sync_queue[sync_idx].empty()) {
        sync_ring.emplace(sync_idx);
      }
    }
    give_cv.notify_one();
    task->complete(0);
  }
}

void FileMirrorPool::sync_file_data(Context *task, int sync_idx) {
  {
    std::unique_lock<std::mutex> lock(mtx);
    give_cv.wait(lock, [this, sync_idx] {
      return (!active || sync_queue[sync_idx].size() < qlimit);
    });
    if (!active) {
      return;
    }
    if (sync_queue[sync_idx].empty()) {
      sync_ring.emplace(sync_idx);
    }
    sync_queue[sync_idx].emplace(task);
  }
  pick_cv.notify_one();
}

int FileMirrorPool::sync_start() {
  std::scoped_lock lock(mtx);
  int sync_idx = 0;
  if (!unassigned_sync_ids.empty()) {
    sync_idx = unassigned_sync_ids.back();
    sync_queue[sync_idx] = std::queue<Context *>();
    unassigned_sync_ids.pop_back();
  } else {
    sync_idx = sync_count++;
    sync_queue.emplace_back(std::queue<Context *>());
  }
  return sync_idx;
}

void FileMirrorPool::sync_finish(int sync_idx) {
  std::scoped_lock lock(mtx);
  ceph_assert(sync_idx >= 0 && sync_idx < sync_queue.size());
  while (!sync_queue[sync_idx].empty()) {
    auto &task = sync_queue[sync_idx].front();
    task->complete(-1);
    sync_queue[sync_idx].pop();
  }
  unassigned_sync_ids.push_back(sync_idx);
}

void FileMirrorPool::update_state(int thread_count) {
  std::unique_lock<std::mutex> lock(mtx);
  if (!active) {
    return;
  }
  if (thread_count == workers.size()) {
    return;
  }

  if (thread_count < workers.size()) {
    for (int i = thread_count; i < workers.size(); ++i) {
      stop_thread[i] = true;
    }
    lock.unlock();
    pick_cv.notify_all();
    for (int i = thread_count; i < workers.size(); ++i) {
      if (workers[i]->joinable()) {
        workers[i]->join();
        dout(0) << ": shutdown of thread having thread idx=" << i
                << ", from file sync thread pool" << dendl;
      }
    }
    lock.lock();
    while (workers.size() > thread_count) {
      workers.pop_back();
      stop_thread.pop_back();
    }
  } else {
    for (int i = workers.size(); i < thread_count; ++i) {
      stop_thread.emplace_back(false);
      workers.emplace_back(
          std::make_unique<std::thread>(&FileMirrorPool::run, this, i));
      dout(0) << ": creating thread having thread idx=" << i
              << ", in file sync thread pool" << dendl;
    }
  }
  num_threads = thread_count;

  dout(0) << ": updating number of threads in file sync threadpool to "
          << thread_count << dendl;
}

void FileMirrorPool::handle_conf_change(const ConfigProxy &conf,
                                      const std::set<std::string> &changed) {
  std::scoped_lock lock(config_mutex);
  if (changed.count("cephfs_mirror_file_sync_thread")) {
    update_state(g_ceph_context->_conf.get_val<uint64_t>(
        "cephfs_mirror_file_sync_thread"));
  }
  dout(0) << ": cephfs_mirror_file_sync_thread=" << num_threads << dendl;
}

const char **FileMirrorPool::get_tracked_conf_keys() const {
  static const char *KEYS[] = {"cephfs_mirror_file_sync_thread", NULL};
  return KEYS;
}

} // namespace mirror
} // namespace cephfs