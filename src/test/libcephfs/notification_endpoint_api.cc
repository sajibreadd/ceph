
#include "common/ceph_json.h"
#include "include/ceph_fs.h"
#include "include/cephfs/libcephfs.h"
#include "include/compat.h"
#include "include/fs_types.h"
#include "include/uuid.h"
#include "gtest/gtest.h"
#include <bits/stdc++.h>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <errno.h>
#include <fcntl.h>

TEST(LibCephFS, AddUDPEndpoint) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_add_udp_endpoint(cmount, "udp", "127.0.0.1", 8080));
  ASSERT_EQ(0, ceph_add_kafka_topic(cmount, "my-topic", "xxx", "localhost:9092",
                                    false, nullptr, nullptr, nullptr, nullptr));
  ASSERT_EQ(0, ceph_mkdirs(cmount, "/dir1", 0777));
  ASSERT_EQ(0, ceph_mkdirs(cmount, "/dir2", 0777));
  ASSERT_EQ(0, ceph_mkdirs(cmount, "/dir3", 0777));
  ASSERT_EQ(0, ceph_mkdirs(cmount, "/dir4", 0777));
  sleep(10);
  ASSERT_EQ(0, ceph_remove_udp_endpoint(cmount, "udp"));
  ASSERT_EQ(0, ceph_remove_kafka_topic(cmount, "my-topic", "xxx"));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

