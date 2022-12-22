#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    cmd_tp = CMD_NONE;
    res = std::make_shared<result>();
    res->done = false;
    res->start = std::chrono::system_clock::now();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    return buf.size() + 16;
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    if (size != 16 + buf.size()) {
        printf("chfs_command_raft serialize failed!\n");
        return;
    }
    int cmd_tp_int = cmd_tp;
    memcpy(buf_out, &cmd_tp, sizeof(int));
    memcpy(buf_out + 4, &type, sizeof(uint32_t));
    memcpy(buf_out + 8, &id, sizeof(extent_protocol::extentid_t));
    memcpy(buf_out + 16, buf.c_str(), buf.size());
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int cmd_tp_int;
    memcpy(&cmd_tp_int, buf_in, sizeof(int));
    memcpy(&type, buf_in + 4, sizeof(uint32_t));
    memcpy(&id, buf_in + 8, sizeof(extent_protocol::extentid_t));
    memcpy(&buf[0], buf_in + 16, size - 16);
    cmd_tp = (command_type) cmd_tp_int;
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << (int) cmd.cmd_tp;
    m << cmd.type;
    m << cmd.id;
    m << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int cmd_tp_int;
    u >> cmd_tp_int;
    cmd.cmd_tp = (chfs_command_raft::command_type) cmd_tp_int;
    u >> cmd.type;
    u >> cmd.id;
    u >> cmd.buf;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT: {
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            break;
        }
        case chfs_command_raft::CMD_PUT: {
            int tmp;
            es.put(chfs_cmd.id, chfs_cmd.buf, tmp);
            break;
        }
        case chfs_command_raft::CMD_GET: {
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            break;
        }
        case chfs_command_raft::CMD_GETA: {
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            break;
        }
        case chfs_command_raft::CMD_RMV: {
            int tmp;
            es.remove(chfs_cmd.id, tmp);
            break;
        }
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


