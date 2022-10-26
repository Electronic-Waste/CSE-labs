#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <vector>
#include "rpc.h"
#include "extent_protocol.h"

#define MAX_LOG_SZ 1024

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
    chfs_command(txid_t _id, cmd_type _type, uint32_t _inode_type, void *identifier)
        : id(_id), type(_type), inode_type(_inode_type) {}

    chfs_command(txid_t _id, cmd_type _type, extent_protocol::extentid_t _inum)
        : id(_id), type(_type), inum(_inum) {}

    chfs_command(txid_t _id, cmd_type _type, extent_protocol::extentid_t _inum, std::string _old, std::string _new)
        : id(_id), type(_type), inum(_inum), old_value(_old), new_value(_new) {}
    
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
            inode_type = std::stoi(input_str.substr(startPos, endPos - startPos));
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
        /* For get, getattr and remove */ 
        else {
            /* Get inum */
            while (input_str[stopPos] != ':') ++stopPos;
            startPos = ++stopPos;
            inum = std::stoull(input_str.substr(startPos, endPos - startPos));
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
            retStr += "," + inodeInfo;
        } 
        /* For put */
        else if (type == CMD_PUT) {
            std::string inumInfo = "inum:" + std::to_string(inum);
            std::string oldInfo = "old:" + old_value;
            std::string newInfo = "new:" + new_value;
            retStr += "," + inumInfo + "," + oldInfo + "," + newInfo;
        } 
        /* For get, getattr and remove */
        else {
            std::string inumInfo = "inum:" + std::to_string(inum);
            retStr += "," + inumInfo;
        }
        retStr += '|';
        // printf("retStr: %s\n", retStr.c_str());
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
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

    void get_log_entries(std::vector<command>& log_entries_temp);

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
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

}

template<typename command>
void persister<command>::append_log(const command& log) {
    // Your code here for lab2A
    std::string input_string = log.toString();
    std::fstream logfile(this->file_path_logfile, std::ios::app | std::ios::out);
    logfile << input_string;
    logfile << std::endl;
    logfile.close();
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    /* Parse string to log entry */
    std::string output_string;
    std::fstream logfile(this->file_path_logfile, std::ios::in);
    printf("Restore logdata: \n");
    while (std::getline(logfile, output_string, '|')) {
        if (output_string[0] == '\n') 
            output_string = output_string.substr(1);
        if (output_string == "") break;
        // printf("-> %s\n", output_string.c_str());
        this->log_entries.push_back(command(output_string));
    }
    logfile.close();
};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

};

template<typename command>
void persister<command>::get_log_entries(std::vector<command>& log_entries_temp) {
    log_entries_temp = log_entries;
    return;
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h