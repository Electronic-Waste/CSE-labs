#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <vector>
#include "rpc.h"
#include "extent_protocol.h"
#include "inode_manager.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,   
        CMD_PUT,
        CMD_GET,
        CMD_GETATTR,
        CMD_REMOVE
    };

    txid_t id;
    cmd_type type;
    extent_protocol::extentid_t inum;
    uint32_t inode_type;
    std::string old_value;
    std::string new_value;


    // constructor
    /* Constructor for begin and commit */
    chfs_command(txid_t _id, cmd_type _type)
        : id(_id), type(_type) {}
    
    /* Constructor for create*/
    chfs_command(txid_t _id, cmd_type _type, uint32_t _inode_type, extent_protocol::extentid_t _inum)
        : id(_id), type(_type), inode_type(_inode_type), inum(_inum) {}

    /* Constructor for put */
    chfs_command(txid_t _id, cmd_type _type, extent_protocol::extentid_t _inum, std::string _old, std::string _new)
        : id(_id), type(_type), inum(_inum), old_value(_old), new_value(_new) {}
    /* Constructor for remove */
    chfs_command(txid_t _id, cmd_type _type, extent_protocol::extentid_t _inum, std::string _old)
        : id(_id), type(_type), inum(_inum), old_value(_old) {}

    /* Constructor for all (parsing string to chfs_command object) */
    chfs_command(std::string input_str) {
        int startPos = 0, stopPos = 0;
        int endPos = input_str.size();
        /* Get txid */
        while (input_str[stopPos] != ':') ++stopPos;
        startPos = ++stopPos;
        while (input_str[stopPos] != ',') ++stopPos;
        id = std::stoull(input_str.substr(startPos, stopPos - startPos));
        /* Get type */
        while (input_str[stopPos] != ':') ++stopPos;
        startPos = ++stopPos;
        while (input_str[stopPos] != ',' && stopPos < endPos) ++stopPos;
        type = (cmd_type) std::stoi(input_str.substr(startPos, stopPos - startPos));
        // printf("input_str: %s\n", input_str.c_str());
        // printf("->restore log: %d recovered!\n", type);
        /* Discuss each condition */
        /* For begin and commit */
        if (type == CMD_BEGIN | type == CMD_COMMIT) {}
        /* For create */
        else if (type == CMD_CREATE) {
            /* Get inode_type */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            while (input_str[stopPos] != ',') ++stopPos;
            inode_type = std::stoi(input_str.substr(startPos, stopPos - startPos));
            /* Get inum */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            inum = std::stoull(input_str.substr(startPos, endPos - startPos));
        }
        /* For put */
        else if (type == CMD_PUT) {
            /* Get inum */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            while (input_str[stopPos] != ',') ++stopPos;
            inum = std::stoull(input_str.substr(startPos, stopPos - startPos));
            // printf("Get inum: %d\n", inum);
            /* Get old_value */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            while (input_str[stopPos] != ',') ++stopPos;
            old_value = (stopPos > startPos) ? input_str.substr(startPos, stopPos - startPos) : "";
            // printf("Get old value: %s\n", old_value.c_str());
            /* Get new_value */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            new_value = (endPos > startPos) ? input_str.substr(startPos, endPos - startPos) : "";
            // printf("Get new value: %s\n", new_value.c_str());
        }
        /* For remove */
        else if (type == CMD_REMOVE) {
            /* Get inum */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            while (input_str[stopPos] != ',') ++stopPos;
            inum = std::stoull(input_str.substr(startPos, stopPos - startPos));
            /* Get old_value */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            old_value = (endPos > startPos) ? input_str.substr(startPos, endPos - startPos) : "";
        }
    }

    // uint64_t size() const {
    //     uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
    //     return s;
    // }

    std::string toString() const {
        std::string prefix = "cmd->";
        std::string typeInfo = "type:" + std::to_string(type);
        std::string idInfo = "id:" + std::to_string(id);
        std::string retStr = prefix + idInfo +"," + typeInfo;

        /* For begin and commit */
        if (type == CMD_BEGIN | type == CMD_COMMIT) {} 
        /* For create */
        else if (type == CMD_CREATE) {
            std::string inodeInfo = "inode_type:" + std::to_string(inode_type);
            std::string inumInfo = "inum:" + std::to_string(inum);
            retStr += "," + inodeInfo + "," + inumInfo;
        } 
        /* For put */
        else if (type == CMD_PUT) {
            std::string inumInfo = "inum:" + std::to_string(inum);
            std::string oldInfo = "old:" + old_value;
            std::string newInfo = "new:" + new_value;
            retStr += "," + inumInfo + "," + oldInfo + "," + newInfo;
        } 
        /* For remove */
        else if (type == CMD_REMOVE) {
            std::string inumInfo = "inum:" + std::to_string(inum);
            std::string oldInfo = "old:" + old_value;
            retStr += "," + inumInfo + "," + oldInfo;
        }
        
        retStr += '|';
        return retStr;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command& log);
    void checkpoint(inode_manager *im);

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint(inode_manager *im);

    void get_log_entries(std::vector<command>& log_entries_temp);
    bool is_checkpoint_trigger(const command& log);

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
    int log_file_size = 0;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
    log_entries.clear();
}

template<typename command>
void persister<command>::append_log(const command& log) {
    // Your code here for lab2A
    std::string input_string = log.toString();
    std::fstream log_file;
    log_file.open(this->file_path_logfile, std::ios::app | std::ios::out);
    log_file << input_string;
    log_file << std::endl;
    log_file.close();
    log_file_size += input_string.size() + 1;
}

template<typename command>
void persister<command>::checkpoint(inode_manager *im) {
    // Your code here for lab2A
    /* Get log entries */
    log_entries.clear();
    this->restore_logdata();
    /* Find last begin and commit */
    int last_commit_pos = 0;
    int log_size = log_entries.size();
    for (int i = 0; i< log_size; ++i) {
        if (log_entries[i].type == chfs_command::CMD_COMMIT)
            last_commit_pos = i;
    }
    /* Undo from the last entry to last_commit_pos */
    for (int i = log_size - 1; i >= last_commit_pos; --i) {
        switch (log_entries[i].type) {
            /* Undo create: remove */
            case chfs_command::CMD_CREATE: {
                im->remove_file(log_entries[i].inum);
            }
            /* Undo put: put old value */
            case chfs_command::CMD_PUT: {
                const char * cbuf = log_entries[i].old_value.c_str();
                int size = log_entries[i].old_value.size();
                im->write_file(log_entries[i].inum, cbuf, size);
            }
            /* Undo remove: create new node and put old value */
            case chfs_command::CMD_REMOVE: {
                int inum = im->alloc_inode(log_entries[i].inode_type);
                const char * cbuf = log_entries[i].old_value.c_str();
                int size = log_entries[i].old_value.size();
                im->write_file(inum, cbuf, size);
            }
            /* For begin and commit: do nothing */
            default: break;
        }
    }
    /* Write disk->blocks to checkpoint file */
    char *disk = im->get_disk();
    std::fstream checkpoint_file;
    checkpoint_file.open(file_path_checkpoint, std::ios::out | std::ios::binary);
    checkpoint_file.write(disk, DISK_SIZE);
    checkpoint_file.close();
    /* Clear logfile and update some variables */
    std::fstream log_file;
    log_file.open(file_path_logfile, std::ios::out);
    log_file.clear();
    log_file.close();
    log_file_size = 0;
    delete disk;
    /* Store the undo action into log file and redo */
    for (int i = last_commit_pos + 1; i < log_size; ++i) {
        append_log(log_entries[i]);
        switch (log_entries[i].type) {
            case chfs_command::CMD_CREATE: {
                im->alloc_inode(log_entries[i].inode_type);
                break;
            }
            case chfs_command::CMD_PUT: {
                const char * cbuf = log_entries[i].new_value.c_str();
                int size = log_entries[i].new_value.size();
                im->write_file(log_entries[i].inum, cbuf, size);
                break;
            }
            case chfs_command::CMD_REMOVE: {
                im->remove_file(log_entries[i].inum);
                break;
            }
            /* For begin and commit */
            default: break;
        }
    }
    log_entries.clear();
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    /* Parse string to log entry */
    std::string output_string;
    std::fstream log_file;
    log_file.open(this->file_path_logfile, std::ios::in);
    printf("Restore logdata: \n");
    while (std::getline(log_file, output_string, '|')) {
        if (output_string[0] == '\n') 
            output_string = output_string.substr(1);
        if (output_string == "") break;
        // printf("-> %s\n", output_string.c_str());
        this->log_entries.push_back(command(output_string));
    }
    log_file.close();
};

template<typename command>
void persister<command>::restore_checkpoint(inode_manager *im) {
    // Your code here for lab2A
    /* Read from checkpoint and set */
    std::fstream checkpoint_file;
    char *src = (char *) malloc(DISK_SIZE);
    memset(src, 0, DISK_SIZE);
    checkpoint_file.open(file_path_checkpoint, std::ios::in | std::ios::binary);
    checkpoint_file.read(src, DISK_SIZE);
    checkpoint_file.clear();
    checkpoint_file.close();
    im->set_disk(src);
    delete src;
};

template<typename command>
void persister<command>::get_log_entries(std::vector<command>& log_entries_temp) {
    log_entries_temp = log_entries;
    return;
}

template<typename command>
bool persister<command>::is_checkpoint_trigger(const command& log)
{
    std::string input_str = log.toString();
    return (input_str.size() + log_file_size + 1 >= MAX_LOG_SZ) ? true : false;
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h