// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  // _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  /* Restore from checkpoint */
  // _persister->restore_checkpoint(im);

  // /* Get log entries from log file */
  // std::vector<chfs_command> log_entries;
  // _persister->restore_logdata();
  // _persister->get_log_entries(log_entries);
  // int log_size = log_entries.size();

  // /* Find last begin and commit */
  // int last_begin_pos = 0, last_commit_pos = 0, last_action_pos = 0;
  // for (int i = 0; i< log_size; ++i) {
  //   if (log_entries[i].type == chfs_command::CMD_BEGIN)
  //     last_begin_pos = i;
  //   else if (log_entries[i].type == chfs_command::CMD_COMMIT)
  //     last_commit_pos = i;
  // }
  // last_action_pos = (last_begin_pos < last_commit_pos) ? last_commit_pos : last_begin_pos - 1;
  
  // /* Redo actions until last action pos */ 
  // for (int i = 0; i< last_action_pos; ++i) {
  //   // printf("execute: %s\n", log_entries[i].toString().c_str());
  //   switch (log_entries[i].type) {
  //     case chfs_command::CMD_CREATE: {
  //       im->alloc_inode(log_entries[i].inode_type);
  //       break;
  //     }
  //     case chfs_command::CMD_PUT: {
  //       const char * cbuf = log_entries[i].new_value.c_str();
  //       int size = log_entries[i].new_value.size();
  //       im->write_file(log_entries[i].inum, cbuf, size);
  //       break;
  //     }
  //     case chfs_command::CMD_REMOVE: {
  //       im->remove_file(log_entries[i].inum);
  //       break;
  //     }
  //     /* For begin and commit */
  //     default: break;
  //   }
  // }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  /* Write logs */
  // chfs_command cmd(cur_txid, chfs_command::cmd_type::CMD_CREATE, type, id);
  /* Check if oversized */
  // bool is_trigger = _persister->is_checkpoint_trigger(cmd);
  // _persister->append_log(cmd);
  // if (is_trigger) {
  //   _persister->checkpoint(im);
  //   // return extent_protocol::IOERR;
  // }

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &status)
{
  id &= 0x7fffffff;

  /* Write logs */
  // std::string old_value;
  // get(id, old_value);
  // chfs_command cmd(cur_txid, chfs_command::cmd_type::CMD_PUT, id, old_value, buf);
  // bool is_trigger = _persister->is_checkpoint_trigger(cmd);
  // _persister->append_log(cmd);

  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  /* Check if oversized */
  // if (is_trigger) {
  //   _persister->checkpoint(im);
  //   // return extent_protocol::IOERR;
  // }
  
  status = extent_protocol::OK;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;
  
  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }
  
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;
  
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &status)
{
  printf("extent_server: write %lld\n", id);

  /* Write logs */
  // std::string old_value;
  // get(id, old_value);
  // chfs_command cmd(cur_txid, chfs_command::cmd_type::CMD_REMOVE, id, old_value);
  // bool is_trigger = _persister->is_checkpoint_trigger(cmd);
  // _persister->append_log(cmd);

  id &= 0x7fffffff;
  im->remove_file(id);

  /* Check if oversized */
  // if (is_trigger) {
  //   _persister->checkpoint(im);
  //   // return extent_protocol::IOERR;
  // }
 
  status = extent_protocol::OK;
  return extent_protocol::OK;
}

int extent_server::beginTX(int novalue, int &status)
{
  /* Write logs */
  // chfs_command cmd(cur_txid, chfs_command::cmd_type::CMD_BEGIN);
  // /* Check if oversized */
  // bool is_trigger = _persister->is_checkpoint_trigger(cmd);
  // _persister->append_log(cmd);
  // if (is_trigger) {
  //   _persister->checkpoint(im);
  //   // return extent_protocol::IOERR;
  // }

  status = extent_protocol::OK;
  return extent_protocol::OK;
}

int extent_server::commitTX(int novalue, int &status)
{
  // /* Write logs */
  // chfs_command cmd(cur_txid++, chfs_command::cmd_type::CMD_COMMIT);
  // /* Check if oversized */
  // bool is_trigger = _persister->is_checkpoint_trigger(cmd);
  // _persister->append_log(cmd);
  // if (is_trigger) {
  //   _persister->checkpoint(im);
  //   // return extent_protocol::IOERR;
  // }
  status = extent_protocol::OK;
  return extent_protocol::OK;  
}
