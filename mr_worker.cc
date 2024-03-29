#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

#define DEBUG
#ifdef DEBUG
	#define WORKER_LOG(fmt, args...)														\
	do {																				\
		auto now =																		\
			std::chrono::duration_cast<std::chrono::milliseconds>(						\
				std::chrono::system_clock::now().time_since_epoch())					\
				.count();																\
		printf("[WORKER_LOG][%ld][%s:%d->%s]" fmt "\n", 								\
				now, __FILE__, __LINE__, __FUNCTION__, ##args);							\
	} while (0);
#else
	#define WORKER_LOG(fmt, args...)													\
	do {} while (0);
#endif

struct KeyVal {
    string key;
    string val;
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
	string content_tmp = content;
    int content_size = content_tmp.size();
    for (int i = 0; i < content_size; ++i) {
        if (content_tmp[i] >= 'a' && content_tmp[i] <= 'z' ||
            content_tmp[i] >= 'A' && content_tmp[i] <= 'Z') continue;
        else content_tmp[i] = ' ';
    }
    vector<KeyVal> ret;
    int ptr_start = 0;
    while (content_tmp[ptr_start] == ' ') ++ptr_start;    // skip blank
    for (int i = ptr_start; i < content_size; ++i) {
        if (content_tmp[i] == ' ') {
            if (content_tmp[ptr_start] != ' ') {
                KeyVal tmp;
                tmp.key = content_tmp.substr(ptr_start, i - ptr_start);
                tmp.val = "1";
                ret.push_back(tmp);
            }
            ptr_start = i + 1;      // set to next index
        }
    }
    return ret;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
	int sum = 0;
    for (auto value : values)
        sum += std::stoi(value);
    return std::to_string(sum);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	void doReduce(int index);
	void doSubmit(mr_tasktype taskType, int index);

	int hash(const std::string key);
	vector<string> get_reduce_files(int reduce_task_index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	// Lab4: Your code goes here.
	std::unique_lock<std::mutex> lock(mtx);
	std::string src_file_path = filenames[index];
	std::string src_file_content;
	WORKER_LOG("%d doMap->filename: %s, index: %d", id, src_file_path.c_str(), index);

	/* Generate KVA */
	getline(ifstream(src_file_path), src_file_content, '\0');
	vector<KeyVal> KVA = mapf(src_file_path, src_file_content);
	WORKER_LOG("KVA size: %d", KVA.size());

	/* Create files */
	vector<ofstream> dst_files;
	for (int i = 0; i < REDUCER_COUNT; ++i) {
		std::string dst_file_path = basedir + "mr-" + std::to_string(index) + "-" + std::to_string(i) + ".txt";
		dst_files.push_back(ofstream(dst_file_path));
	}

	/* Write to files according to the hash of key */
	for (auto kv : KVA) {
		std::string output = kv.key + " " + kv.val + "\n";
		dst_files[hash(kv.key)] << output;
	}

	/* Close files */
	int dst_file_size = dst_files.size();
	for (int i = 0; i < dst_file_size; ++i)
		dst_files[i].close();
}

void Worker::doReduce(int index)
{
	// Lab4: Your code goes here.
	std::unique_lock<std::mutex> lock(mtx);
	vector<string> src_files_path = get_reduce_files(index);
	WORKER_LOG("%d doReduce->index: %d", id, index);

	/* Open source files and parse string to KeyVal */
	vector<KeyVal> KVA;
	for (auto path : src_files_path) {
		WORKER_LOG("Open reduce file: %s", path.c_str());
		ifstream src_file(path);
		std::string src_file_line;
		while (getline(src_file, src_file_line)) {
			int pos = src_file_line.find(' ');
			KeyVal kv;
			kv.key = src_file_line.substr(0, pos);
			kv.val= src_file_line.substr(pos + 1, src_file_line.size() - pos - 1);
			KVA.push_back(kv);
		}
	}
	
	/* Sort KVA */
	sort(KVA.begin(), KVA.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});

	/* Create dst file */
	std::string dst_file_path = basedir + "mr-out-" + std::to_string(index) + ".txt";
	ofstream dst_file(dst_file_path);

	/* Call Reduce */
	for (unsigned int i = 0; i < KVA.size();) {
        unsigned int j = i + 1;
        for (; j < KVA.size() && KVA[j].key == KVA[i].key;) j++;

        vector<string> values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(KVA[k].val);
        }

        string output = KVA[i].key + " " + Reduce(KVA[i].key, values) + "\n";
        dst_file << output;

        i = j;
    }

	/* Close dst file */
	dst_file.close();

}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	WORKER_LOG("%d doSubmit->taskType: %d, index: %d", id, taskType, index);
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab4: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		mr_protocol::AskTaskResponse reply;
		mr_protocol::status ret = this->cl->call(mr_protocol::asktask, id, reply);
		WORKER_LOG("%d ask for task!", id);
		if (ret != mr_protocol::OK) {
			fprintf(stderr, "ask task failed\n");
			exit(-1);
		}

		/* mr_tasktype::MAP, then doMap and doSubmit */
		if (reply.task_type == mr_tasktype::MAP) {
			doMap(reply.index, reply.filenames);
			doSubmit(mr_tasktype::MAP, reply.index);
		}
		/* mr_tasktype::REDUCE, then doReduce and doSubmit */
		else if (reply.task_type == mr_tasktype::REDUCE) {
			doReduce(reply.index);
			doSubmit(mr_tasktype::REDUCE, reply.index);
		}
		/* mr_tasktype::NONE, meaning currently no worker is needed, then sleep */
		else {
			if (reply.index == -1) break;	// The signal for end
			WORKER_LOG("%d get NONE: sleep 10ms", id);
			usleep(10 * 1000);
		}
	}
}

int Worker::hash(const std::string key) {
	return (int) key[0] % REDUCER_COUNT;
}

vector<string> Worker::get_reduce_files(int reduce_task_index) {
	vector<string> reduce_files_path;
	int map_task_index = 0;
	WORKER_LOG("%d get reduce files", reduce_task_index);
	while (true) {
		std::string path = basedir + "mr-" + std::to_string(map_task_index) + "-" + std::to_string(reduce_task_index) + ".txt";
		ifstream test_file(path);
		if (test_file.good()) {
			reduce_files_path.push_back(path);
			test_file.close();
			++map_task_index;
		}
		else break;
	}
	return reduce_files_path;
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

