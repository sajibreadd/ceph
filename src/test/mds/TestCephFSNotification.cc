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
#include <string.h>

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif
struct CheckListContainer {
  struct CheckList {
    std::vector<std::pair<std::string, std::string>> lst;
    CheckList() {}
    CheckList(std::vector<std::pair<std::string, std::string>> &&lst)
        : lst(std::move(lst)) {}
    bool evaluate(JSONParser &parser) {
      for (auto &[x, y] : lst) {
        auto obj = parser.find_obj(x);
        if (obj == nullptr || obj->get_data() != y) {
          return false;
        }
      }
      return true;
    }
  };
  CheckListContainer() : total(0) {}
  std::vector<std::pair<CheckList, int>> container;
  int total;

  void expect_notification(
      std::vector<std::pair<std::string, std::string>> &&check_list, int cnt) {
    container.emplace_back(std::move(check_list), cnt);
    total += cnt;
  }

  void clear() {
    container.clear();
    total = 0;
  }

  bool evaluate(JSONParser &parser) {
    if (total == 0) {
      return false;
    }
    for (auto &[check_list, cnt] : container) {
      if (cnt == 0) {
        continue;
      }
      if (check_list.evaluate(parser)) {
        --cnt, --total;
        return true;
      }
    }
    return false;
  }
};

class TestInterface {
public:
  TestInterface() : result(false) {
    socket = new boost::asio::ip::udp::socket(
        io_context,
        boost::asio::ip::udp::endpoint(boost::asio::ip::udp::v4(), 8081));
    timer = new boost::asio::steady_timer(io_context);
    init();
  }
  ~TestInterface() {
    release();
    timer->cancel();
    socket->cancel();
    delete timer;
    delete socket;
  }
  virtual void do_test(bool ignore) = 0;
  std::string create_random_directory(const std::string &parent_dir,
                                      mode_t mode) {
    uuid_d uid;
    uid.generate_random();
    std::string dir = parent_dir + "/" + uid.to_string();
    EXPECT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), mode));
    return dir;
  }
  std::string create_random_file(const std::string &parent_dir, int flags,
                                 mode_t mode) {
    uuid_d uid;
    uid.generate_random();
    std::string file_path = parent_dir + "/" + uid.to_string() + ".txt";
    int fd = ceph_open(cmount, file_path.c_str(), flags | O_CREAT, mode);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(0, ceph_close(cmount, fd));
    return file_path;
  }
  bool is_data_correct(std::size_t bytes_read) {
    uint64_t &len = *(uint64_t *)data;
    std::cout << std::string(data + sizeof(len), bytes_read - sizeof(len))
              << std::endl;
    JSONParser parser;
    if (!parser.parse(data + sizeof(len), bytes_read - sizeof(len))) {
      return false;
    }
    return checker.evaluate(parser);
  }
  void init() {
    ASSERT_EQ(0, ceph_create(&cmount, NULL));
    ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
    ASSERT_EQ(0, ceph_mount(cmount, "/"));
    ASSERT_EQ(0, ceph_add_udp_endpoint(cmount, "udp", "127.0.0.1", 8081));
  }
  void release() {
    ASSERT_EQ(0, ceph_remove_udp_endpoint(cmount, "udp"));
    ASSERT_EQ(0, ceph_unmount(cmount));
    ASSERT_EQ(0, ceph_release(cmount));
  }
  void start_timer() {
    timer->expires_after(std::chrono::seconds(timeout_seconds));
    timer->async_wait(boost::bind(&TestInterface::timeout, this,
                                  boost::asio::placeholders::error));
  }
  void timeout(const boost::system::error_code &e) {
    if (e) {
      return;
    }
    if (result) {
      result = (checker.total == 0);
    }
    socket->cancel();
  }
  void read_data(bool ignore) {
    boost::asio::ip::udp::endpoint sender_endpoint;
    socket->async_receive_from(
        boost::asio::buffer(data), sender_endpoint,
        boost::bind(&TestInterface::check_data, this,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred, ignore));
  }
  void check_data(const boost::system::error_code &e, std::size_t bytes_read,
                  bool ignore) {
    if (e) {
      return;
    }
    if (!ignore) {
      if (result && !is_data_correct(bytes_read)) {
        result = false;
      }
    } else {
      is_data_correct(bytes_read);
    }
    read_data(ignore);
  }
  void evaluate(bool ignore) {
    result = true;
    io_context.restart();
    start_timer();
    read_data(ignore);
    io_context.run();
    EXPECT_TRUE(result);
  }

protected:
  boost::asio::ip::udp::socket *socket;
  boost::asio::steady_timer *timer;
  boost::asio::io_context io_context;
  char data[1024];
  int timeout_seconds = 10;
  struct ceph_mount_info *cmount;
  bool result;
  CheckListContainer checker;
};

class TestDirectory : public TestInterface {
public:
  void do_test(bool ignore) override {
    // creating directory
    std::cout << "Testing notification for creation of a directory"
              << std::endl;
    std::string dir_path = create_random_directory("", 0777);
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);

    // creating xattr
    std::cout << "Testing notification for creation of xattr of a directory"
              << std::endl;
    uuid_d uid;
    uid.generate_random();
    std::string random_xattr = uid.to_string();
    EXPECT_EQ(0, ceph_setxattr(cmount, dir_path.c_str(), "user.randomxattr",
                               (void *)random_xattr.c_str(),
                               random_xattr.size(), 0));
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_ATTRIB | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);

    // removing xattr
    std::cout << "Testing notification for removal of xattr of a directory"
              << std::endl;
    EXPECT_EQ(0,
              ceph_removexattr(cmount, dir_path.c_str(), "user.randomxattr"));
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_ATTRIB | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);

    // setting attr
    std::cout << "Testing notification for setting of attribute of a directory"
              << std::endl;
    struct ceph_statx stx;
    struct timespec old_btime = {1, 2};
    stx.stx_btime = old_btime;
    EXPECT_EQ(0, ceph_setattrx(cmount, dir_path.c_str(), &stx,
                               CEPH_SETATTR_BTIME, 0));
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_ATTRIB | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);

    std::cout << "Testing notification for changing permissions of a directory"
              << std::endl;
    EXPECT_EQ(0, ceph_chmod(cmount, dir_path.c_str(), 0777));
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_ATTRIB | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);

    // deleting directory
    std::cout << "Testing notification for deletion of directory" << std::endl;
    EXPECT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", dir_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_DELETE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);
    // creating symlink
    std::cout << "Testing notification for creation of symlink of a directory"
              << std::endl;
    std::string parent_dir1 = create_random_directory("", 0777);
    std::string parent_dir2 = create_random_directory("", 0777);
    std::string target_path = create_random_directory(parent_dir1, 0777);
    uid.generate_random();
    std::string symlink_path = parent_dir2 + "/" + uid.to_string();
    EXPECT_EQ(0,
              ceph_symlink(cmount, target_path.c_str(), symlink_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", parent_dir1},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", parent_dir2},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", target_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", symlink_path},
         {"mask", std::to_string(CEPH_MDS_NOTIFY_CREATE)}},
        1);
    evaluate(ignore);

    // moving directory
    std::cout
        << "Testing notification for creation of renaming/moving a directory"
        << std::endl;
    parent_dir1 = create_random_directory("", 0777);
    parent_dir2 = create_random_directory("", 0777);
    std::string src_path = create_random_directory(parent_dir1, 0777);
    uid.generate_random();
    std::string dest_path = parent_dir2 + "/" + uid.to_string();
    EXPECT_EQ(0, ceph_rename(cmount, src_path.c_str(), dest_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", parent_dir1},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", parent_dir2},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", src_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"src_path", src_path},
         {"dest_path", dest_path},
         {"src_mask",
          std::to_string(CEPH_MDS_NOTIFY_MOVED_FROM | CEPH_MDS_NOTIFY_ONLYDIR)},
         {"dest_mask",
          std::to_string(CEPH_MDS_NOTIFY_MOVED_TO | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    evaluate(ignore);
  }
};

TEST(CephFSNotification, Directory) {
  TestDirectory test;
  test.do_test(false);
}

class TestFile : public TestInterface {
public:
  void do_test(bool ignore) override {

    // creating file
    std::cout << "Testing notification for creation of a file" << std::endl;
    std::string file_path = create_random_file("", O_CREAT, 0777);
    checker.clear();
    checker.expect_notification(
        {{"path", file_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_OPEN)}},
        1);
    evaluate(ignore);

    // opening file in O_RDWR mode
    std::cout << "Testing notification for opening of a file in O_RDWR mode"
              << std::endl;
    int fd = ceph_open(cmount, file_path.c_str(), O_RDWR, 0777);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(0, ceph_close(cmount, fd));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_OPEN)}},
        1);
    evaluate(ignore);

    // opening file in O_RDWR | O_TRUNC mode
    std::cout << "Testing notification for opening of a file in O_RDWR | "
                 "O_TRUNC  mode"
              << std::endl;
    fd = ceph_open(cmount, file_path.c_str(), O_RDWR | O_TRUNC, 0777);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(0, ceph_close(cmount, fd));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path},
         {"mask", std::to_string(CEPH_MDS_NOTIFY_OPEN | CEPH_MDS_NOTIFY_MODIFY |
                                 CEPH_MDS_NOTIFY_ACCESS)}},
        1);
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_MODIFY)}},
        1);
    evaluate(ignore);

    // writing data into file
    std::cout << "Testing notification for writing data into a file"
              << std::endl;
    fd = ceph_open(cmount, file_path.c_str(), O_RDWR, 0777);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(7, ceph_write(cmount, fd, "testing", 7, 0));
    EXPECT_EQ(0, ceph_close(cmount, fd));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_OPEN)}},
        1);
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_MODIFY)}},
        1);
    evaluate(ignore);

    // creating xattr
    std::cout << "Testing notification for creating xattr of a file"
              << std::endl;
    uuid_d uid;
    uid.generate_random();
    std::string random_xattr = uid.to_string();
    EXPECT_EQ(0, ceph_setxattr(cmount, file_path.c_str(), "user.randomxattr",
                               (void *)random_xattr.c_str(),
                               random_xattr.size(), 0));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)}},
        1);
    evaluate(ignore);

    // removing xattr
    std::cout << "Testing notification for removing a xattr of a file"
              << std::endl;
    EXPECT_EQ(0,
              ceph_removexattr(cmount, file_path.c_str(), "user.randomxattr"));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)}},
        1);
    evaluate(ignore);

    // setting attr
    std::cout << "Testing notification for setting attr of a file" << std::endl;
    struct ceph_statx stx;
    struct timespec old_btime = {1, 2};
    stx.stx_btime = old_btime;
    EXPECT_EQ(0, ceph_setattrx(cmount, file_path.c_str(), &stx,
                               CEPH_SETATTR_BTIME, 0));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)}},
        1);
    evaluate(ignore);

    // changing permission
    std::cout << "Testing notification for chaning permssion of a file"
              << std::endl;
    EXPECT_EQ(0, ceph_chmod(cmount, file_path.c_str(), 0777));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)}},
        1);
    evaluate(ignore);

    // move
    std::cout << "Testing notification for moving/renaming a file" << std::endl;
    std::string parent_dir1 = create_random_directory("", 0777);
    std::string parent_dir2 = create_random_directory("", 0777);
    std::string src_path = create_random_file(parent_dir1, O_CREAT, 0777);
    uid.generate_random();
    std::string dest_path = parent_dir2 + "/" + uid.to_string() + ".txt";
    EXPECT_EQ(0, ceph_rename(cmount, src_path.c_str(), dest_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", parent_dir1},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", parent_dir2},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", src_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_OPEN)}},
        1);
    checker.expect_notification(
        {{"src_path", src_path},
         {"dest_path", dest_path},
         {"src_mask", std::to_string(CEPH_MDS_NOTIFY_MOVED_FROM)},
         {"dest_mask", std::to_string(CEPH_MDS_NOTIFY_MOVED_TO)}},
        1);
    evaluate(ignore);

    // hard link
    std::cout << "Testing notification for creating hardlink a file"
              << std::endl;
    parent_dir1 = create_random_directory("", 0777);
    parent_dir2 = create_random_directory("", 0777);
    std::string target_path = create_random_file(parent_dir1, O_CREAT, 0777);
    uid.generate_random();
    std::string link_path = parent_dir2 + "/" + uid.to_string();
    EXPECT_EQ(0, ceph_link(cmount, target_path.c_str(), link_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", parent_dir1},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", parent_dir2},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_ONLYDIR)}},
        1);
    checker.expect_notification(
        {{"path", target_path},
         {"mask",
          std::to_string(CEPH_MDS_NOTIFY_CREATE | CEPH_MDS_NOTIFY_OPEN)}},
        1);
    checker.expect_notification(
        {{"target_path", target_path},
         {"link_path", link_path},
         {"target_mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)},
         {"link_mask", std::to_string(CEPH_MDS_NOTIFY_CREATE)}},
        1);
    evaluate(ignore);

    // creating hard link of hard link
    std::cout
        << "Testing notification for creating hardlink of a hardlink a file"
        << std::endl;
    uid.generate_random();
    std::string link_path2 = parent_dir2 + "/" + uid.to_string();
    EXPECT_EQ(0, ceph_link(cmount, link_path.c_str(), link_path2.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"target_path", target_path},
         {"link_path", link_path2},
         {"target_mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)},
         {"link_mask", std::to_string(CEPH_MDS_NOTIFY_CREATE)}},
        1);
    evaluate(ignore);

    // doing ops on hard link
    std::cout
        << "Testing notification for doing operations of hard link of a file"
        << std::endl;
    fd = ceph_open(cmount, link_path2.c_str(), O_RDWR, 0777);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(0, ceph_close(cmount, fd));

    std::cout << "Testing notification for opening hardlink of a file in "
                 "O_RDWR | O_TRUNC mode"
              << std::endl;
    fd = ceph_open(cmount, link_path2.c_str(), O_RDWR | O_TRUNC, 0777);
    std::cout << "Testing notification for opening hardlink of a file in "
                 "O_RDWR | O_TRUNC mode"
              << std::endl;
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(0, ceph_close(cmount, fd));

    std::cout << "Testing notification for wrting data into hardlink of a file"
              << std::endl;
    fd = ceph_open(cmount, link_path2.c_str(), O_RDWR, 0777);
    EXPECT_TRUE(fd >= 0);
    EXPECT_EQ(7, ceph_write(cmount, fd, "testing", 7, 0));
    EXPECT_EQ(0, ceph_close(cmount, fd));
    checker.clear();
    checker.expect_notification(
        {{"path", target_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_OPEN)}},
        2);
    checker.expect_notification(
        {{"path", target_path},
         {"mask", std::to_string(CEPH_MDS_NOTIFY_OPEN | CEPH_MDS_NOTIFY_MODIFY |
                                 CEPH_MDS_NOTIFY_ACCESS)}},
        1);
    checker.expect_notification(
        {{"path", target_path},
         {"mask", std::to_string(CEPH_MDS_NOTIFY_MODIFY)}},
        1);
    evaluate(ignore);

    // moving hard link
    std::cout << "Testing notification for moving/renaming hardlink of a file"
              << std::endl;
    src_path = link_path2;
    uid.generate_random();
    dest_path = parent_dir1 + "/" + uid.to_string();
    EXPECT_EQ(0, ceph_rename(cmount, src_path.c_str(), dest_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"src_path", src_path},
         {"dest_path", dest_path},
         {"src_mask", std::to_string(CEPH_MDS_NOTIFY_MOVED_FROM)},
         {"dest_mask", std::to_string(CEPH_MDS_NOTIFY_MOVED_TO)}},
        1);
    evaluate(ignore);
    link_path2 = dest_path;

    // removing hard link
    std::cout << "Testing notification for removing hardlink of a file"
              << std::endl;
    EXPECT_EQ(0, ceph_unlink(cmount, link_path2.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"target_path", target_path},
         {"link_path", link_path2},
         {"target_mask", std::to_string(CEPH_MDS_NOTIFY_ATTRIB)},
         {"link_mask", std::to_string(CEPH_MDS_NOTIFY_DELETE)}},
        1);
    evaluate(ignore);

    // deleting file
    std::cout << "Testing notification for deleting a file" << std::endl;
    EXPECT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
    checker.clear();
    checker.expect_notification(
        {{"path", file_path}, {"mask", std::to_string(CEPH_MDS_NOTIFY_DELETE)}},
        1);
    evaluate(ignore);
  }
};

TEST(CephFSNotification, File) {
  TestFile test;
  test.do_test(true);
}