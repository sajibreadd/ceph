// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/ceph_assert.h"
#include "include/object.h"
#include "include/stringify.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <dirent.h>

using namespace std;
class TestMount {
public:
  ceph_mount_info* cmount = nullptr;
  string dir_path;

public:
  TestMount(const char* root_dir_name = "") : dir_path(root_dir_name) {
    ceph_create(&cmount, NULL);
    ceph_conf_read_file(cmount, NULL);
    ceph_conf_parse_env(cmount, NULL);
    ceph_assert(0 == ceph_mount(cmount, NULL));
  }
  ~TestMount()
  {
    ceph_shutdown(cmount);
  }

  int conf_get(const char *option, char *buf, size_t len) {
    return ceph_conf_get(cmount, option, buf, len);
  }

  int conf_set(const char *option, const char *val) {
    return ceph_conf_set(cmount, option, val);
  }

  string make_file_path(const char* relpath) {
    string ret = dir_path;
    ret += '/';
    ret += relpath;
    return ret;
  }

  int write_full(const char* relpath, const string& data)
  {
    auto file_path = make_file_path(relpath);
    int fd = ceph_open(cmount, file_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd < 0) {
      return -EACCES;
    }
    int r = ceph_write(cmount, fd, data.c_str(), data.size(), 0);
    if (r >= 0) {
      ceph_truncate(cmount, file_path.c_str(), data.size());
      ceph_fsync(cmount, fd, 0);
    }
    ceph_close(cmount, fd);
    return r;
  }
  string concat_path(string_view path, string_view name) {
    string s(path);
    if (s.empty() || s.back() != '/') {
      s += '/';
    }
    s += name;
    return s;
  }
  int unlink(const char* relpath)
  {
    auto file_path = make_file_path(relpath);
    return ceph_unlink(cmount, file_path.c_str());
  }

  int get_snapid(const char* relpath, uint64_t* res)
  {
    ceph_assert(res);
    snap_info snap_info;

    auto snap_path = make_file_path(relpath);
    int r = ceph_get_snap_info(cmount, snap_path.c_str(), &snap_info);
    if (r >= 0) {
      *res = snap_info.id;
      r = 0;
    }
    return r;
  }

  int for_each_readdir(const char* relpath,
    std::function<bool(const dirent*, const struct ceph_statx*)> fn)
  {
    auto subdir_path = make_file_path(relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, subdir_path.c_str(), &ls_dir);
    if (r != 0) {
      return r;
    }

    while (1) {
      struct dirent result;
      struct ceph_statx stx;

      r = ceph_readdirplus_r(
        cmount, ls_dir, &result, &stx, CEPH_STATX_BASIC_STATS,
        0,
        NULL);
      if (!r)
        break;
      if (r < 0) {
        std::cerr << "ceph_readdirplus_r failed, error: "
                  << r << std::endl;
        return r;
      }

      if (strcmp(result.d_name, ".") == 0 ||
          strcmp(result.d_name, "..") == 0) {
        continue;
      }
      if (!fn(&result, &stx)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_closedir(cmount, ls_dir));
    return r;
  }

  int for_each_readdir_snapdiff(
      const char *relpath, const string &prv_snap, const string &cur_snap,
      std::function<bool(const ceph_snapdiff_entry_t &,
                         const ceph_snapdiff_entry_t &, bool)>
          fn) {
    cout << __func__ << ": dir_path=" << dir_path << ", subdir_path=" << relpath
         << endl;
    ceph_snapdiff_info info;
    int r = ceph_open_snapdiff(cmount, dir_path.c_str(), relpath,
                               prv_snap.c_str(), cur_snap.c_str(), &info);
    if (r < 0) {
      std::cerr << "ceph_open_snapdiff failed, error: " << cpp_strerror(r)
                << std::endl;
      return r;
    }
    ceph_snapdiff_entry_t prv_entry;
    ceph_snapdiff_entry_t cur_entry;
    bool first_entry = true;
    while (true) {
      r = ceph_readdir_snapdiff(&info, &cur_entry);
      if (r == 0) {
        break;
      }
      if (r < 0) {
        std::cerr << "ceph_readdir_snapdiff failed, error: " << r << std::endl;
        return r;
      }
      std::string d_name = string(cur_entry.dir_entry.d_name);
      if (d_name == "." || d_name == "..") {
        continue;
      }
      if (!fn(prv_entry, cur_entry, first_entry)) {
        r = -EINTR;
        break;
      }
      prv_entry = cur_entry;
      first_entry = false;
    }
    ceph_assert(0 == ceph_close_snapdiff(&info));
    return r;
  }

  int mkdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_mkdir(cmount, path.c_str(), 0777);
  }
  int rmdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_rmdir(cmount, path.c_str());
  }
  int purge_dir(const char* relpath0)
  {
    int r =
      for_each_readdir(relpath0,
        [&](const dirent* dire, const struct ceph_statx* stx) {
          string relpath = concat_path(relpath0, dire->d_name);

	  if (S_ISDIR(stx->stx_mode)) {
            purge_dir(relpath.c_str());
            rmdir(relpath.c_str());
          } else {
            unlink(relpath.c_str());
          }
          return true;
        });
    if (r != 0) {
      return r;
    }
    r = rmdir(relpath0);
    return r;
  }

  ceph_mount_info* get_cmount() {
    return cmount;
  }

  int test_open(const char* relpath)
  {
    auto subdir_path = make_file_path(relpath);
    int r = ceph_open(cmount, subdir_path.c_str(), O_DIRECTORY | O_RDONLY, 0);
    if (r < 0) {
      std::cout << "test_open error: " << subdir_path.c_str() << ", " << r << std::endl;
      return r;
    }
    return r;
  }
  int test_close(int fd)
  {
    ceph_assert(0 == ceph_close(cmount, fd));
    return 0;
  }

  int test_statxat(int fd, const char* entry)
  {
    int r;
    {
      struct ceph_statx stx;
      r = ceph_statxat(cmount, fd, entry, &stx, CEPH_STATX_MODE | CEPH_STATX_INO, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
      if (r < 0) {
        std::cout << "test_statxat " << entry << " returns " << r << std::endl;
      } else {
        // replace CEPH_NOSNAP with 0 as the former is negative
        // and hence might be confused with an error.
        r = (uint64_t)stx.stx_dev == CEPH_NOSNAP ? 0 : stx.stx_dev;
        std::cout << "stx=" << stx.stx_ino << "." << r << std::endl;
      }
    }
    return r;
  }
  int test_statx(const char* path)
  {
    int r;
    {
      struct ceph_statx stx;
      r = ceph_statx(cmount, path, &stx, CEPH_STATX_MODE | CEPH_STATX_INO, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
      if (r < 0) {
        std::cout << "test_statx " << path << " returns " << r << std::endl;
      } else {
        // replace CEPH_NOSNAP with 0 as the former is negative
        // and hence might be confused with an error.
        r = (uint64_t)stx.stx_dev == CEPH_NOSNAP ? 0 : stx.stx_dev;
        std::cout << "stx=" << stx.stx_ino << "." << r << std::endl;
      }
    }
    return r;
  }

};

void prepareTrimCacheTest(TestMount& tm, size_t max_bulk)
{
  ceph_rmsnap(tm.cmount, "/BrokenStatxAfterTrimeCacheTest", "snap1");
  ceph_rmsnap(tm.cmount, "/BrokenStatxAfterTrimeCacheTest", "snap2");
  tm.purge_dir("/BrokenStatxAfterTrimeCacheTest");

  ASSERT_EQ(0, tm.mkdir("/BrokenStatxAfterTrimeCacheTest"));
  ASSERT_EQ(0, tm.mkdir("/BrokenStatxAfterTrimeCacheTest/bulk"));
  ASSERT_EQ(0, tm.mkdir("/BrokenStatxAfterTrimeCacheTest/test"));
  char path[PATH_MAX];
  for (size_t i = 0; i < max_bulk; i++) {
    snprintf(path, PATH_MAX - 1, "/BrokenStatxAfterTrimeCacheTest/bulk/%lu", i);
    tm.write_full(path, path);
  }

  tm.write_full("/BrokenStatxAfterTrimeCacheTest/test/file1", "abcdef");
  ASSERT_EQ(0, tm.mkdir("/BrokenStatxAfterTrimeCacheTest/.snap/snap1"));
  tm.write_full("/BrokenStatxAfterTrimeCacheTest/test/file1", "snap2>>>");
  tm.write_full("/BrokenStatxAfterTrimeCacheTest/test/file2", "snap2>>>abcdef");
  ASSERT_EQ(0, tm.mkdir("/BrokenStatxAfterTrimeCacheTest/.snap/snap2"));
}

TEST(LibCephFS, BrokenStatxAfterTrimCache)
{
  size_t bulk_count = 100;
  {
    TestMount tm;
    prepareTrimCacheTest(tm, bulk_count);
  }
  TestMount test_mount;
  ASSERT_EQ(0, test_mount.conf_set("client_cache_size", stringify(bulk_count/2).c_str()));

  uint64_t snapid1;
  uint64_t snapid2;

   // learn snapshot ids and do basic verification
  ASSERT_EQ(0, test_mount.get_snapid("/BrokenStatxAfterTrimeCacheTest/.snap/snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("/BrokenStatxAfterTrimeCacheTest/.snap/snap2", &snapid2));

  int s1fd = test_mount.test_open("/BrokenStatxAfterTrimeCacheTest/.snap/snap1");
  int s2fd = test_mount.test_open("/BrokenStatxAfterTrimeCacheTest/.snap/snap2");

  // check if file1's statxat points to snap1
  ASSERT_EQ(snapid1, test_mount.test_statxat(s1fd, "test/file1"));
  // check if file1's statxat points to snap2
  ASSERT_EQ(snapid2, test_mount.test_statxat(s2fd, "test/file1"));
  // check if file2's statxat returns -2
  ASSERT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  // check if file2's statx returns -2
  ASSERT_EQ(-2, test_mount.test_statx("/BrokenStatxAfterTrimeCacheTest/.snap/snap1/test/file2"));

  int cnt = 0;
  int r = test_mount.for_each_readdir("/BrokenStatxAfterTrimeCacheTest/bulk",
    [&](const dirent*, const struct ceph_statx*) {
      ++cnt;
      return true;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(bulk_count, cnt);

  // open folder to trigger cache trimming
  int bulk_fd = test_mount.test_open("/BrokenStatxAfterTrimeCacheTest/bulk");

  // checking if statxat returns the same values as above,
  // which isn't the case if cache trimming evicted dentries behind
  // inodes bound to s1fd/s2fd.
  EXPECT_EQ(snapid1, test_mount.test_statxat(s1fd, "test/file1"));
  EXPECT_EQ(snapid2, test_mount.test_statxat(s2fd, "test/file1"));
  // check if file2's statxat returns -2
  EXPECT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  // check if file2's statx still returns -2, should be fine irrespective of cache state.
  // This will also update the cache and bring file2 inode back to good shape
  ASSERT_EQ(-2, test_mount.test_statx("/BrokenStatxAfterTrimeCacheTest/.snap/snap1/test/file2"));
  // check if file2's statxat returns -2
  ASSERT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  test_mount.test_close(bulk_fd);

  test_mount.test_close(s2fd);
  test_mount.test_close(s1fd);

  ceph_rmsnap(test_mount.cmount, "/BrokenStatxAfterTrimeCacheTest", "snap1");
  ceph_rmsnap(test_mount.cmount, "/BrokenStatxAfterTrimeCacheTest", "snap2");
  test_mount.purge_dir("/BrokenStatxAfterTrimeCacheTest");
}

void prepareBrokenSnapdiffApiTest(TestMount &tm, int max_bulk) {
  ceph_rmsnap(tm.cmount, "/BrokenSnapdiffApiTest", "snap1");
  ceph_rmsnap(tm.cmount, "/BrokenSnapdiffApiTest", "snap2");
  int r = tm.purge_dir("");

  // ASSERT_EQ(0, tm.mkdir("/BrokenSnapdiffApiTest"));
  ASSERT_EQ(0, tm.mkdir(""));
  ASSERT_EQ(0, tm.mkdir("bulk"));
  ASSERT_EQ(0, tm.mkdir("test"));

  int i, j;
  char path[PATH_MAX];
  for (i = 0; i < max_bulk; i++) {
    cout << "snap1 progress-->" << i << endl;
    snprintf(path, PATH_MAX - 1, "bulk/%d", i);
    tm.write_full(path, path);
  }
  tm.write_full("test/file1", "abcdef");
  ASSERT_EQ(0, tm.mkdir(".snap/snap1"));
  // creation of snap1 done

  for (i = 0; i < max_bulk / 2; ++i) {
    cout << "snap2 delete progress-->" << i << endl;
    snprintf(path, PATH_MAX - 1, "bulk/%d", i);
    ASSERT_EQ(0, tm.unlink(path));
    if (i >= max_bulk / 4) {
      ASSERT_EQ(0, tm.mkdir(path));
    }
  }
  for (i = max_bulk; i < 2 * max_bulk; ++i) {
    cout << "snap2 create progress-->" << i << endl;
    snprintf(path, PATH_MAX - 1, "bulk/%d", i);
    tm.write_full(path, path);
  }
  tm.write_full("test/file1", "snap2>>>");
  tm.write_full("test/file2", "snap2>>>abcdef");
  ASSERT_EQ(0, tm.mkdir(".snap/snap2"));
}

TEST(LibCephFS, BrokenSnapdiffApiTest) {
  size_t bulk_count = 1 << 12;
  {
    TestMount tm("/BrokenSnapdiffApiTest");
    prepareBrokenSnapdiffApiTest(tm, bulk_count);
    return;
  }
  TestMount test_mount("/BrokenSnapdiffApiTest");
  ASSERT_EQ(0, test_mount.conf_set("client_cache_size",
                                   stringify(1 << 6).c_str()));

  uint64_t snapid1;
  uint64_t snapid2;

  // learn snapshot ids and do basic verification
  ASSERT_EQ(
      0, test_mount.get_snapid(".snap/snap1", &snapid1));
  ASSERT_EQ(
      0, test_mount.get_snapid(".snap/snap2", &snapid2));

  int s1fd = test_mount.test_open(".snap/snap1");
  int s2fd = test_mount.test_open(".snap/snap2");

  // check if file1's statxat points to snap1
  ASSERT_EQ(snapid1, test_mount.test_statxat(s1fd, "test/file1"));
  // check if file1's statxat points to snap2
  ASSERT_EQ(snapid2, test_mount.test_statxat(s2fd, "test/file1"));
  // check if file2's statxat returns -2
  ASSERT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  // check if file2's statx returns -2
  ASSERT_EQ(-2, test_mount.test_statx(".snap/snap1/test/file2"));

  int cnt = 0;
  multiset <int> test_purged_dentries;
  multiset <int> test_recreated_dentries;
  multiset <int> test_new_created_dentries;
  multiset <int> test_deleted_dentries;
  int r = test_mount.for_each_readdir_snapdiff(
      "bulk", "snap1", "snap2",
      [&](const ceph_snapdiff_entry_t &prv_entry,
          const ceph_snapdiff_entry_t &cur_entry, bool first_entry) {
        ++cnt;
        std::string d_name = string(cur_entry.dir_entry.d_name);
        int dentry = std::stoi(d_name);

        if (dentry >= bulk_count / 4 && dentry < bulk_count / 2) {
          if (cur_entry.snapid == snapid2) {
            if (test_recreated_dentries.find(dentry) !=
                test_recreated_dentries.end()) {
              cerr << "Recreated entry=" << d_name
                   << ", having snapid=" << cur_entry.snapid
                   << " appearing multiple times" << endl;
            }
            test_recreated_dentries.insert(dentry);
          } else {
            if (test_purged_dentries.find(dentry) !=
                test_purged_dentries.end()) {
              cerr << "Recreated entry=" << d_name
                   << ", having snapid=" << cur_entry.snapid
                   << " appearing multiple times" << endl;
            }
            test_purged_dentries.insert(dentry);
          }
        } else if (dentry >= bulk_count) {
          if (cur_entry.snapid != snapid2) {
            cerr << "Recreated entry=" << d_name
                 << ", having wrong snapid=" << cur_entry.snapid << endl;
          } else if (test_new_created_dentries.find(dentry) !=
                     test_new_created_dentries.end()) {
            cerr << "Newly created entry=" << d_name
                 << ", having snapid=" << cur_entry.snapid
                 << " appearing multiple times" << endl;
          }
          test_new_created_dentries.insert(dentry);
        } else if (dentry < bulk_count < 4) {
          if (cur_entry.snapid != snapid1) {
            cerr << "Deleted entry=" << d_name
                 << ", having wrong snapid=" << cur_entry.snapid << endl;
          } else if (test_deleted_dentries.find(dentry) !=
                     test_deleted_dentries.end()) {
            cerr << "Deleted entry=" << d_name
                 << ", having snapid=" << cur_entry.snapid
                 << " appearing multiple times" << endl;
          }
          test_deleted_dentries.insert(dentry);
        } else {
          cerr << "Unknown dentry=" << d_name << endl;
        }
        return true;
      });

  ASSERT_EQ(0, r);

  for (int i = 0; i < bulk_count / 4; ++i) {
    if (test_deleted_dentries.find(i) == test_deleted_dentries.end()) {
      cerr << "deleted entry=" << i << " not found" << endl;
    }
  }

  for (int i = bulk_count; i < 2 * bulk_count; ++i) {
    if (test_new_created_dentries.find(i) == test_new_created_dentries.end()) {
      cerr << "newly created entry=" << i << " not found" << endl;
    }
  }

  for (int i = bulk_count / 4; i < bulk_count / 2; ++i) {
    if (test_recreated_dentries.find(i) == test_recreated_dentries.end()) {
      cerr << "Recreated entry=" << i << " not found" << endl;
    }
  }

  cout << "deleted entry=" << bulk_count / 4 << ", with snapid=" << snapid1
       << ", found=" << test_deleted_dentries.size() << endl;

  cout << "recreated entry=" << bulk_count / 4 << ", with snapid=" << snapid2
       << ", found=" << test_recreated_dentries.size() << endl;

  cout << "recreated entry=" << bulk_count / 4 << ", with snapid=" << snapid1
       << ", found=" << test_purged_dentries.size() << endl;

  cout << "new created entry=" << bulk_count << ", with snapid=" << snapid2
       << ", found=" << test_new_created_dentries.size() << endl;

  EXPECT_EQ(cnt, 2 * bulk_count - bulk_count / 2 + bulk_count / 4);
  EXPECT_EQ(test_deleted_dentries.size(), bulk_count / 4);
  EXPECT_EQ(test_new_created_dentries.size(), bulk_count);
  EXPECT_EQ(test_recreated_dentries.size(), bulk_count / 4);
  EXPECT_EQ(test_recreated_dentries.size(), test_purged_dentries.size());


  // open folder to trigger cache trimming
  int bulk_fd = test_mount.test_open("bulk");

  // checking if statxat returns the same values as above,
  // which isn't the case if cache trimming evicted dentries behind
  // inodes bound to s1fd/s2fd.
  EXPECT_EQ(snapid1, test_mount.test_statxat(s1fd, "test/file1"));
  EXPECT_EQ(snapid2, test_mount.test_statxat(s2fd, "test/file1"));
  // check if file2's statxat returns -2
  EXPECT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  // check if file2's statx still returns -2, should be fine irrespective of
  // cache state. This will also update the cache and bring file2 inode back to
  // good shape
  ASSERT_EQ(-2, test_mount.test_statx(".snap/snap1/test/file2"));
  // check if file2's statxat returns -2
  ASSERT_EQ(-2, test_mount.test_statxat(s1fd, "test/file2"));
  test_mount.test_close(bulk_fd);

  test_mount.test_close(s2fd);
  test_mount.test_close(s1fd);

  // ceph_rmsnap(test_mount.cmount, "/BrokenSnapdiffApiTest", "snap1");
  // ceph_rmsnap(test_mount.cmount, "/BrokenSnapdiffApiTest", "snap2");
  // test_mount.purge_dir("/BrokenSnapdiffApiTest");
}

