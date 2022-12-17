#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int term;               // candidate's term
    int candidate_id;       // candidate requesting vote
    int last_log_index;     // index of candidate's last log entry
    int last_log_term;      // term of candidate's last log entry
    
    request_vote_args() = default;

    ~request_vote_args() = default;
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int term;               // currentTerm, for candidate to update itself
    bool vote_granted;      // true means candidate received vote
    
    request_vote_reply() = default;

    ~request_vote_reply() = default;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry {
public:
    // Lab3: Your code here
    command cmd;        // command for state machine
    int term;           // term when entry was received by leader

    log_entry() = default;

    ~log_entry() = default;
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    m << entry.cmd;
    m << entry.term;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    u >> entry.cmd;
    u >> entry.term;
    return u;
}

template <typename command>
class append_entries_args {
public:
    // Your code here
    int term;                                   // leader's term
    int leader_id;                              // so follower can redirect clients
    int prev_log_index;                         // index of log entry immediately preceding new ones
    int prev_log_term;                          // term of prev_log_index entry
    int entries_size;                           // size of log entries
    std::vector<log_entry<command>> entries;    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    int leader_commit;                          // leader's commit_index
    bool is_heartbeat;                          // whether this call is heartbeat from leader

    append_entries_args() = default;

    ~append_entries_args() {entries.clear();}
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m << args.term;
    m << args.leader_id;
    m << args.prev_log_index;
    m << args.prev_log_term;
    m << args.entries_size;
    for (int i = 0; i < args.entries.size(); ++i)
        m << args.entries[i];
    m << args.leader_commit;
    m << args.is_heartbeat;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    u >> args.term;
    u >> args.leader_id;
    u >> args.prev_log_index;
    u >> args.prev_log_term;
    u >> args.entries_size;
    for (int i = 0; i < args.entries_size; ++i) {
        log_entry<command> log;
        u >> log;
        args.entries.push_back(log);
    }
    u >> args.leader_commit;
    u >> args.is_heartbeat;
    return u;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    int term;                   // current_term, for leader to update itself
    bool success;               // true if follower contained entry matching prevLogIndex and prevLogTerm
    bool is_heartbeat;          // whether this is a reply from heartbeat call
    
    append_entries_reply() = default;

    ~append_entries_reply() = default;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
    int term;                   // leader's term
    int leader_id;              // so follower can redirect clients
    int last_included_index;    // the snapshot replaces all entries up through and including this index
    int last_included_term;     // term of last_included_index
    int offset;                 // byte offset where chunk is positioned in the snapshot file
    std::string data;           // raw bytes of the snapshot chunk, starting at offset
    bool done;                  // true if this is the last chunk
    
    install_snapshot_args() = default;

    ~install_snapshot_args() = default;
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
    int term;                   // current_term, for leader to update itself

    install_snapshot_reply() = default;

    ~install_snapshot_reply() = default;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h