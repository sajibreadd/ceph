#pragma once
#include "CDentry.h"
#include "CInode.h"
#include "MDSRank.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>

#ifdef WITH_CEPHFS_NOTIFICATION
#include "MDSKafka.h"
#include "MDSNotificationMessage.h"
#include "MDSUDPEndpoint.h"

class MDSKafkaManager;
class MDSUDPManager;
#endif

class MDSNotificationManager {
public:
  MDSNotificationManager(MDSRank *mds);
  void init();

#ifdef WITH_CEPHFS_NOTIFICATION
  int add_kafka_topic(const std::string &topic_name, const std::string &broker,
                      bool use_ssl, const std::string &user,
                      const std::string &password,
                      const std::optional<std::string> &ca_location,
                      const std::optional<std::string> &mechanism,
                      bool write_into_disk);
  int remove_kafka_topic(const std::string &topic_name, bool write_into_disk);
  int add_udp_endpoint(const std::string &name, const std::string &ip, int port,
                       bool write_into_disk);
  int remove_udp_endpoint(const std::string &name, bool write_into_disk);
#endif

  void push_notification(int32_t whoami, CInode *in, uint64_t notify_mask);
  void push_notification_link(int32_t whoami, CInode *targeti, CDentry *destdn,
                              uint64_t notify_mask_for_target,
                              uint64_t notify_mask_for_link);
  void push_notification_move(int32_t whoami, CDentry *srcdn, CDentry *destdn);
  void push_notification_snap(int32_t whoami, CInode *in,
                              const std::string &snapname,
                              uint64_t notify_mask);

private:
#ifdef WITH_CEPHFS_NOTIFICATION
  std::unique_ptr<MDSKafkaManager> kafka_manager;
  std::unique_ptr<MDSUDPManager> udp_manager;
  void
  push_notification(const std::shared_ptr<MDSNotificationMessage> &message);
#endif

  CephContext *cct;
  std::atomic<uint64_t> cur_notification_seq_id;
  std::string session_id;
};