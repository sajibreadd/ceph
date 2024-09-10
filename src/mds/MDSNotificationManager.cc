#include "MDSNotificationManager.h"
#include "include/uuid.h"
#define dout_subsys ceph_subsys_mds

MDSNotificationManager::MDSNotificationManager(CephContext *cct)
    : cct(cct), cur_notification_seq_id(0) {
#ifdef WITH_CEPHFS_NOTIFICATION
  uuid_d uid;
  uid.generate_random();
  session_id = uid.to_string();
  kafka_manager = std::make_unique<MDSKafkaManager>(cct);
  udp_manager = std::make_unique<MDSUDPManager>(cct);
#endif
}

#ifdef WITH_CEPHFS_NOTIFICATION
int MDSNotificationManager::add_kafka_topic(const std::string &topic_name,
                                            const connection_t &connection) {
  return kafka_manager->add_topic(topic_name, connection);
}

int MDSNotificationManager::remove_kafka_topic(const std::string &topic_name) {
  return kafka_manager->remove_topic(topic_name);
}

int MDSNotificationManager::add_udp_endpoint(const std::string &name,
                                             const std::string &ip, int port) {
  return udp_manager->add_endpoint(name, ip, port);
}

int MDSNotificationManager::remove_udp_endpoint(const std::string &name) {
  return udp_manager->remove_endpoint(name);
}

void MDSNotificationManager::push_notification(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  ldout(cct, 0) << std::string(message->message.c_str(),
                               message->message.length())
                << dendl;
  kafka_manager->send(message);
  udp_manager->send(message);
}
#endif

void MDSNotificationManager::push_notification(int32_t whoami, CInode *in,
                                               uint64_t notify_mask) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string path;
  in->make_path_string(path, true, nullptr);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  message->create_message(whoami, session_id, notify_mask, path);
  push_notification(message);
#endif
}

void MDSNotificationManager::push_notification_link(
    int32_t whoami, CInode *targeti, CDentry *destdn,
    uint64_t notify_mask_for_target, uint64_t notify_mask_for_link) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string target_path;
  targeti->make_path_string(target_path, true, nullptr);
  std::string link_path;
  destdn->make_path_string(link_path, true);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  if (target_path == link_path) {
    message->create_message(whoami, session_id, notify_mask_for_link,
                            target_path);
    push_notification(message);
    return;
  }
  message->create_link_message(whoami, session_id, notify_mask_for_target,
                               notify_mask_for_link, target_path, link_path);
  push_notification(message);
#endif
}

void MDSNotificationManager::push_notification_move(int32_t whoami,
                                                    CDentry *srcdn,
                                                    CDentry *destdn) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string dest_path, src_path;
  srcdn->make_path_string(src_path, true);
  destdn->make_path_string(dest_path, true);
  uint64_t src_mask = CEPH_MDS_NOTIFY_MOVED_FROM,
           dest_mask = CEPH_MDS_NOTIFY_MOVED_TO;
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  message->create_move_message(whoami, session_id, src_mask, dest_mask,
                               src_path, dest_path);
  push_notification(message);
#endif
}

void MDSNotificationManager::push_notification_snap(int32_t whoami, CInode *in,
                                                    const std::string &snapname,
                                                    uint64_t notify_mask) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string path;
  in->make_path_string(path, true, nullptr);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  message->create_snap_message(whoami, session_id, notify_mask, path,
                               std::string(snapname));
  push_notification(message);
#endif
}
