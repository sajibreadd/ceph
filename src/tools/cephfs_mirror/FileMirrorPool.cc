// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "Utils.h"

#include "FileMirrorPool.h"
#include "PeerReplayer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::FileMirrorPool " << __func__

namespace cephfs {
namespace mirror {

std::string FileMirrorPool::FileWorker::state_name[] = {
    "LOCKCONTEST",       "IDLE",
    "CONSUME",           "EXECUTE",
    "BACKOFF_CHECK1",    "BACKOFF_CHECK2",
    "FILE_OPEN_LOCAL",   "FILE_OPEN_REMOTE",
    "FILE_READ",         "FILE_WRITE",
    "FILE_FSYNC",        "FREE_BUFFER",
    "FILE_CLOSE_REMOTE", "FILE_CLOSE_LOCAL"};

FileMirrorPool::FileMirrorPool(int num_threads)
    : num_threads(num_threads), qlimit(std::max(10 * num_threads, 5000)),
      sync_count(0), active(false) {
  g_conf().add_observer(this);
}

void FileMirrorPool::activate() {
  {
    std::scoped_lock lock(mtx);
    active = true;
    file_workers = std::vector<FileWorker *>(num_threads);
    for (int i = 0; i < num_threads; ++i) {
      file_workers[i] = new FileWorker();
      file_workers[i]->activate();
    }
  }
  for (int i = 0; i < num_threads; ++i) {
    file_workers[i]->worker =
        std::thread(&FileMirrorPool::run, this, file_workers[i]);
  }
  dout(0) << ": " << num_threads << " number of threads are activated" << dendl;
}

void FileMirrorPool::deactivate() {
  {
    std::scoped_lock lock(mtx);
    active = false;
    for (auto &file_worker : file_workers) {
      file_worker->deactivate();
    }
  }
  drain_queue();

  for (auto &file_worker : file_workers) {
    file_worker->join();
  }
}

void FileMirrorPool::drain_queue(int idx) {
  {
    std::scoped_lock lock(mtx);
    if (idx == -1) {
      for (int i = 0; i < sync_queues.size(); ++i) {
        sync_queues[i]->drain_queue();
      }
    } else if (idx >= 0 && idx < sync_queues.size()){
      sync_queues[idx]->drain_queue();
    }
  }
  pick_cv.notify_all();
}

void FileMirrorPool::run(FileWorker *file_worker) {
  while (true) {
    file_worker->state = FileWorker::ThreadState::LOCKCONTEST;
    int sync_idx = -1;
    FileSyncMechanism *task;
    SyncQueue* sq = nullptr;
    {
      std::unique_lock<std::mutex> lock(mtx);
      file_worker->state = FileWorker::ThreadState::IDLE;
      pick_cv.wait(lock, [this, file_worker] {
        return (file_worker->stop_called || !sync_ring.empty());
      });
      file_worker->state = FileWorker::ThreadState::CONSUME;
      if (file_worker->stop_called) {
        file_worker->force_deactivate();
        return;
      }
      sync_idx = sync_ring.front();
      sync_ring.pop();
      ceph_assert(sync_idx < sync_queues.size());
      sq = sync_queues[sync_idx];

      if (sq->sync_queue.empty()) {
        continue;
      }

      task = sq->sync_queue.front();
      file_worker->current_syncing_file.file_name = std::move(task->get_file_name());
      file_worker->current_syncing_file.peer_uid = std::move(task->get_peer_uid());
      file_worker->current_syncing_file.file_size = std::move(task->get_file_size());
      sq->sync_queue.pop();
      if (!sq->sync_queue.empty()) {
        sync_ring.emplace(sync_idx);
      }
      file_worker->state = FileWorker::ThreadState::EXECUTE;
      file_worker->file_transferred++;
      file_worker->bytes_synced = 0;
      task->set_worker_ref(file_worker);
    }
    sq->give_cv.notify_one();
    task->complete(0);
  }
}

void FileMirrorPool::sync_file_data(FileSyncMechanism *task, int sync_idx) {
  {
    std::unique_lock<std::mutex> lock(mtx);
    auto sq = sync_queues[sync_idx];
    sq->give_cv.wait(lock, [this, &sq] {
      return (!active || sq->sync_queue.size() < qlimit);
    });
    if (!active) {
      return;
    }
    if (sq->sync_queue.empty()) {
      sync_ring.emplace(sync_idx);
    }
    sq->sync_queue.emplace(task);
  }
  pick_cv.notify_one();
}

int FileMirrorPool::sync_start(const std::string &dir_root) {
  std::scoped_lock lock(mtx);
  int sync_idx = 0;
  if (!unassigned_sync_ids.empty()) {
    sync_idx = unassigned_sync_ids.back();
    delete sync_queues[sync_idx];
    sync_queues[sync_idx] = nullptr;
    sync_queues[sync_idx] = new SyncQueue(dir_root);
    unassigned_sync_ids.pop_back();
  } else {
    sync_idx = sync_count++;
    sync_queues.emplace_back(new SyncQueue(dir_root));
  }
  return sync_idx;
}

void FileMirrorPool::sync_finish(int sync_idx, const std::string &dir_root) {
  std::scoped_lock lock(mtx);
  ceph_assert(sync_idx >= 0 && sync_idx < sync_queues.size());
  ceph_assert(sync_queues[sync_idx]->dir_root == dir_root);

  sync_queues[sync_idx]->drain_queue();
  unassigned_sync_ids.push_back(sync_idx);
}

void FileMirrorPool::update_state(int thread_count) {
  std::unique_lock<std::mutex> lock(mtx);
  if (!active) {
    return;
  }
  if (thread_count == num_threads) {
    return;
  }

  if (thread_count < num_threads) {
    for (int i = thread_count; i < num_threads; ++i) {
      file_workers[i]->deactivate();
      dout(0) << ": Lazy shutdown of thread no " << i
              << " from file mirror threadpool" << dendl;
    }
    num_threads = thread_count;
    lock.unlock();
    pick_cv.notify_all();
  } else {
    int i;
    for (i = num_threads; i < file_workers.size() && num_threads < thread_count;
         ++i) {
      if (file_workers[i]->active) {
        std::swap(file_workers[i], file_workers[num_threads]);
        file_workers[num_threads++]->activate();
        dout(0) << ": Reactivating thread no " << i
                << " of file mirror threadpool" << dendl;
      }
    }
    for (i = (int)file_workers.size() - 1; i >= num_threads; --i) {
      if (num_threads == thread_count && file_workers[i]->active) {
        break;
      }
    }
    lock.unlock();
    pick_cv.notify_all();
    if (i < num_threads) {
      for (i = (int)file_workers.size() - 1; i >= num_threads; --i) {
        file_workers[i]->join();
        dout(0) << ": Force shutdown of already lazy shut thread having "
                   "thread no "
                << i << " from file mirror threadpool" << dendl;
        delete file_workers[i];
        file_workers[i] = nullptr;
        file_workers.pop_back();
      }
    }

    for (i = num_threads; i < thread_count; ++i) {
      file_workers.emplace_back(new FileWorker());
      file_workers[i]->activate();
      file_workers[i]->worker =
          std::thread(&FileMirrorPool::run, this, file_workers[i]);
      dout(0) << ": Creating thread no " << i << " in file mirror threadpool"
              << dendl;
    }
    num_threads = thread_count;
  }
  dout(0) << ": updating number of threads in file mirror threadpool to"
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

void FileMirrorPool::SyncQueue::drain_queue() {
  while (!sync_queue.empty()) {
    auto &task = sync_queue.front();
    task->complete(-1);
    sync_queue.pop();
  }
  give_cv.notify_all();
}

void FileMirrorPool::dump_stats(Formatter *f) {
  std::scoped_lock lock(mtx);
  f->open_object_section("file_mirror_pool");
  for (int i = 0; i < num_threads; ++i) {
    f->open_object_section(std::to_string(i));
    file_workers[i]->dump(f);
    f->close_section();
  }
  f->close_section();
}

} // namespace mirror
} // namespace cephfs