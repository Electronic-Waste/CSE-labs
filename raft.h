#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

// #define RAFT_LOG(fmt, args...) \
//     do {                       \
//     } while (0);

    #define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int voted_for;                          // CandidateId that received vote in current term 
    std::vector<log_entry<command>> log;    // log entries; each entry contains command for state machine, and term when entry was received by leader
    
    /* ---- Volatile state on all server----  */
    int commit_index;                       // index of highest log entry known to be committed
    int last_applied;                       // index of highest log entry applied to state machine
    unsigned long last_rpc_time;            // the lastest time this node received rpc call

    /* ---- Volatile state on candidate ---- */
    std::atomic_int vote_counter;          // thread-safe counter for candidate

    /* ---- Volatile state on leader----  */
    std::vector<int> next_index;            // for each server, index of the next log entry to send to that server
    std::vector<int> match_index;           // for each server, index of the highest log entry known to be replicated on server

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    unsigned long get_time();

    int get_random_timer();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    srand(time(NULL));

    /* ----Persistent state on all server----  */
    voted_for = -1;
    log_entry<command> tmp;
    tmp.term = 0;
    log.push_back(tmp);

    /* ---- Volatile state on all server----  */
    commit_index = 0;
    last_applied = 0;
    last_rpc_time = get_time();

    /* ---- Volatile state on candidate ---- */
    /* ---- Initialized when a node becomes candidate ---- */

    /* ---- Volatile state on leader----  */
    /* ---- Reinitialized after elelction ---- */
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);

    /* Leader appends the new command to its log */
    if (role == leader) {
        RAFT_LOG("leader %d appends a new cmd", my_id);
        log_entry<command> new_cmd;
        new_cmd.term = current_term;
        new_cmd.cmd = cmd;
        log.push_back(new_cmd);
    }
    else {
        return false;
    }
    
    term = current_term;
    index = log.size() - 1;
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);

    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    
    reply.term = current_term;      // add server's current_term to reply
    last_rpc_time = get_time();     // update last received rpc time
    
    /* Voting server denies vote if it has voted for a candidate */
    if (voted_for >= 0 && current_term == args.term) {
        reply.vote_granted = false;
        return OK;
    }
    /* Voting server denies vote if its log is "more complete" */
    if (current_term > args.term ||
        log[log.size() - 1].term > args.last_log_term || 
        log[log.size() - 1].term == args.last_log_term && (log.size() - 1) > args.last_log_index) {
        reply.vote_granted = false;
        return OK;
    }
    /* Grant voting and update server state*/
    reply.vote_granted = true;
    current_term = args.term;
    voted_for = args.candidate_id;
    return OK;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    RAFT_LOG("%d received vote reply from %d, term: %d, voteGranted: %d", my_id, target, reply.term, reply.vote_granted);

    /* if this reply.term > current_term, reverts to follower */
    if (reply.term > current_term) {
        current_term = reply.term;
        voted_for = -1;
        role = follower;
        return;
    }

    /* Target node has granted voting */
    if (role == candidate && reply.vote_granted) {
        ++vote_counter;
        if (vote_counter >= (rpc_clients.size() + 1) / 2) {
            role = leader;
            /* Initialize to 0, increases monotonically */
            match_index = std::vector<int>(rpc_clients.size(), 0);
            /* Initialize to leader last log index + 1 */
            next_index = std::vector<int>(rpc_clients.size(), log.size());
            RAFT_LOG("%d has become new leader!", my_id);
        }
    }
    
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);

    /* We update commit_index to enable raft::run_background::apply */
    /* If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)*/
    if (arg.leader_commit > commit_index && arg.leader_commit < log.size()) {
        commit_index = (arg.leader_commit < log.size() - 1) ? arg.leader_commit : log.size() - 1;
    }

    /* Receive heart beat */
    if (arg.is_heartbeat) {
        /* Accept heartbeat */
        if (arg.term >= current_term) {
            current_term = arg.term;
            role = follower;
            last_rpc_time = get_time();

            reply.term = current_term;
            reply.success = true;
        }
        /* Deny heartbeat */
        else {
            last_rpc_time = get_time();

            reply.term = current_term;
            reply.success = false;
        }
    }
    /* Receive normal AppendEntry RPC call */
    else {
        // RAFT_LOG("Normal RPC call: arg.term %d, prev_index: %d, prev_term: %d", arg.term, arg.prev_log_index, arg.prev_log_term);
        /* Reply false if term < currentTerm */
        if (arg.term < current_term) {
            last_rpc_time = get_time();

            reply.term = current_term;
            reply.success = false;
            return 0;
        }

        /* Append entries */
        if (arg.prev_log_index <= log.size() - 1 &&
            arg.prev_log_term == log[arg.prev_log_index].term) {
            RAFT_LOG("leader %d append entries to %d", arg.leader_id, my_id);
            /* Copy entries from arg.entries to log */
            log.resize(arg.prev_log_index + arg.entries_size + 1);
            for (int i = arg.prev_log_index + 1; i <= arg.prev_log_index + arg.entries_size; ++i) {
                    log[i] = arg.entries[i - arg.prev_log_index - 1];
            }
            last_rpc_time = get_time();

            reply.term = current_term;
            reply.success = true;
        }
        /* Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm */
        else {
            last_rpc_time = get_time();

            reply.term = current_term;
            reply.success = false;
        }

    }
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    last_rpc_time = get_time();

    /* Check role == leader, else return */
    if (role != leader) return;

    /* If current_term < reply.term, reverts to follower */
    if (current_term < reply.term) {
        role = follower;
        voted_for = -1;
        current_term = reply.term;
        return;
    }

    /* If it's a reply from heartbeat, we do nothing */
    if (arg.is_heartbeat) return;

    /* If AppendEntry RPC was accepted */
    if (reply.success) {
        int reply_match_index = arg.prev_log_index + arg.entries_size;
        RAFT_LOG("Leader %d replicate log %d on %d succeesfully", my_id, reply_match_index, node);
        /* Update match_index[node] */
        match_index[node] = reply_match_index;
        /* Update next_index[node] */
        next_index[node] = match_index[node] + 1;
        /* Update commit_index */
        for (int i = commit_index + 1; i < log.size(); ++i) {
            int replicate_num = 0;
            if (log[i].term != current_term) continue;
            for (int j = 0; j < rpc_clients.size(); ++j) {
                if (j == my_id) ++replicate_num;
                else if (match_index[j] >= commit_index + 1) ++replicate_num;  
            }
            if (replicate_num >= (rpc_clients.size() + 1) / 2) {
                ++commit_index;
                RAFT_LOG("Leader commit log %d", commit_index);
            }
        }
    }
    /* If AppendEntry RPC was denied */
    else {
        next_index[node]--;
    }
    
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();

        unsigned long random_timeout = get_random_timer();
        if (role == follower && (get_time() - last_rpc_time) > random_timeout || 
            role == candidate && (get_time() - last_rpc_time) > random_timeout) {
            RAFT_LOG("%d has become candidate!", my_id);
            role = candidate;
            ++current_term;
            vote_counter = 1;       // initialize volatile candidate state
            voted_for = my_id;      // candidate vote for itself
            last_rpc_time = get_time();
            for (int i = 0; i < rpc_clients.size(); ++i) {
                if (i == my_id) continue;
                request_vote_args args;
                args.term = current_term;
                args.candidate_id = my_id;
                args.last_log_index = log.size() - 1;
                args.last_log_term = log[log.size() - 1].term;
                thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if (role == leader) {
            for (int i = 0; i < rpc_clients.size(); ++i) {
                if (i == my_id) continue;
                if (next_index[i] < log.size() || match_index[i] < log.size() - 1) {
                    append_entries_args<command> arg;
                    arg.term = current_term;
                    arg.leader_id = my_id;
                    arg.prev_log_index = next_index[i] - 1;
                    arg.prev_log_term = log[next_index[i] - 1].term;
                    arg.is_heartbeat = false;
                    arg.leader_commit = commit_index;
                    arg.entries_size = log.size() - next_index[i];
                    for (int j = 0; j < arg.entries_size; ++j) 
                        arg.entries.push_back(log[next_index[i] + j]);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }    
        

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
        if (last_applied < commit_index) {
            for (int i = last_applied + 1; i <= commit_index; ++i) 
                state->apply_log(log[i].cmd);
            RAFT_LOG("%d ~ %d applied", last_applied + 1, commit_index);
        }
        last_applied = commit_index;
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();

        if (role == leader) {
            for (int i = 0; i < rpc_clients.size(); ++i) {
                if (i == my_id) continue;
                append_entries_args<command> args;
                args.term = current_term;
                args.leader_id = leader_id;
                args.entries_size = 0;
                args.leader_commit = commit_index;
                args.is_heartbeat = true;
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
unsigned long raft<state_machine, command>::get_time() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

template<typename state_machine, typename command>
int raft<state_machine, command>::get_random_timer() {
    return 150 + (rand() % 150);
}

#endif // raft_h