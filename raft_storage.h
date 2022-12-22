#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void persist_meta(int current_term_, int voted_for_);

    void persist_log(const std::vector<log_entry<command>> &log_, int persist_to_index);

    void recover();

    bool can_be_recovered();

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string meta_log_path;
    std::string entry_log_path;
    std::fstream meta_log;
    std::fstream entry_log;

public:
    int current_term;
    int voted_for;
    int log_size;
    std::vector<log_entry<command>> log;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    meta_log_path = dir + "/meta.log";
    entry_log_path = dir + "/entry.log";
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::persist_meta(int current_term_, int voted_for_) {
    mtx.lock();
    meta_log.open(meta_log_path, std::ios::out | std::ios::binary);
    meta_log.seekg(0, std::ios::beg);
    meta_log.write((char *) &current_term_, sizeof(int));
    meta_log.write((char *) &voted_for_, sizeof(int));
    meta_log.close();
    mtx.unlock();
}

template <typename command>
void raft_storage<command>::persist_log(const std::vector<log_entry<command>> &log_, int persist_to_index) {
    mtx.lock();
    entry_log.open(entry_log_path, std::ios::out | std::ios::binary);
    entry_log.seekg(0, std::ios::beg);
    /* Write persist_size to entry.log */
    int persist_log_size = persist_to_index + 1;
    entry_log.write((char *) &persist_log_size, sizeof(int));
    for (int i = 0; i < persist_log_size; ++i) {
        int log_entry_term = log_[i].term;
        command log_entry_cmd = log_[i].cmd;
        int cmd_size = log_entry_cmd.size();
        /* Write log_entry's term */
        entry_log.write((char *) &log_entry_term, sizeof(int));
        /* Write log_entry's cmd size */
        entry_log.write((char *) &cmd_size, sizeof(int));
        /* Write log_entry's cmd */
        char *buf = new char[cmd_size];
        log_entry_cmd.serialize(buf, cmd_size);
        entry_log.write(buf, cmd_size);
        delete buf;
    }
    entry_log.close();
    mtx.unlock();
}

template <typename command>
void raft_storage<command>::recover() {
    mtx.lock();
    /* ---- Recover meta_log ---- */
    meta_log.open(meta_log_path, std::ios::in | std::ios::binary);
    meta_log.seekg(0, std::ios::beg);
    meta_log.read((char *) &current_term, sizeof(int));
    meta_log.read((char *) &voted_for, sizeof(int));
    meta_log.close();
    // printf("recover! current_term: %d, voted_for: %d\n", current_term, voted_for);

    /* ---- Recover entry log ---- */
    entry_log.open(entry_log_path, std::ios::in | std::ios::binary);
    entry_log.seekg(0, std::ios::beg);
    entry_log.read((char *) &log_size, sizeof(int));
    for (int i = 0; i < log_size; ++i) {
        log_entry<command> entry;
        /* Read entry's term */
        entry_log.read((char *) &entry.term, sizeof(int));
        /* Read entry's cmd size (tmp) */
        int cmd_size;
        entry_log.read((char *) &cmd_size, sizeof(int));
        /* Read entry's cmd */
        char *buf = new char[cmd_size];
        entry_log.read(buf, cmd_size);
        entry.cmd.deserialize(buf, cmd_size);
        delete buf;
        /* Push to log */
        log.push_back(entry);
    }
    entry_log.close();
    mtx.unlock();
}

template <typename command>
bool raft_storage<command>::can_be_recovered() {
    mtx.lock();
    std::cout << "test recover" << std::endl;
    meta_log.open(meta_log_path, std::ios::in);
    entry_log.open(entry_log_path, std::ios::in);

    uint64_t meta_start;
    uint64_t meta_end;
    uint64_t log_start;
    uint64_t log_end;
    meta_log.seekg(0, std::ios::beg);
    meta_start = meta_log.tellg();
    meta_log.seekg(0, std::ios::end);
    meta_end = meta_log.tellg();
    entry_log.seekg(0, std::ios::beg);
    log_start = entry_log.tellg();
    entry_log.seekg(0, std::ios::end);
    log_end = entry_log.tellg();

    entry_log.close();
    meta_log.close();
    mtx.unlock();
    /* Decide whether log files can be recovered by file size */
    // printf("meta_start: %d, meta_end: %d, log_start: %d, log_end: %d\n",
    //     meta_start, meta_end, log_start, log_end);
    if (meta_end - meta_start < 8 ||
        log_end - log_start < 4) {
        std::cout << "recover fail!" << std::endl;
        return false;
    }
    else return true;
    
}

#endif // raft_storage_h