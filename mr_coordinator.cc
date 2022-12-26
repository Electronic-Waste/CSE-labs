#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

#define DEBUG
#ifdef DEBUG
	#define COORDINATOR_LOG(fmt, args...)												\
	do {																				\
		auto now =																		\
			std::chrono::duration_cast<std::chrono::milliseconds>(						\
				std::chrono::system_clock::now().time_since_epoch())					\
				.count();																\
		printf("[COORDINATOR_LOG][%ld][%s:%d->%s]" fmt "\n", 							\
				now, __FILE__, __LINE__, __FUNCTION__, ##args);							\
	} while (0);
#else
	#define COORDINATOR_LOG(fmt, args...)												\
	do {} while (0);
#endif

struct Task {
	int taskType;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;
	vector<unsigned long> mapTasksTime;
	vector<unsigned long> reduceTasksTime;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;
	
	string getFile(int index);
	unsigned long getTime();
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab4 : Your code goes here.
	reply.task_type = mr_tasktype::NONE;

	/* Priority1: Assign Map Task */
	if (!this->isFinishedMap()) {
		mtx.lock();
		int map_tasks_size = this->mapTasks.size();
		unsigned long current_time = getTime();
		for (int i = 0; i < map_tasks_size; ++i) {
			/* If the task has not been assigned or timeout, assgin the task */
			if (this->mapTasks[i].isCompleted) continue;;
			if (!this->mapTasks[i].isAssigned || 
				this->mapTasks[i].isAssigned && current_time - this->mapTasksTime[i] >= 80){
				COORDINATOR_LOG("Assign a map task!");
				this->mapTasks[i].isAssigned = true;
				this->mapTasksTime[i] = current_time;
				reply.task_type = mr_tasktype::MAP;
				reply.index = i;
				reply.filenames = this->files;
				break;
			}
		}
		if (reply.task_type == mr_tasktype::NONE) {
			COORDINATOR_LOG("Can't get Map work");
		}
		mtx.unlock();
	}
	/* Priority2: Assign Reduce Task */
	else if (!this->isFinishedReduce()) {
		mtx.lock();
		int reduce_tasks_size = this->reduceTasks.size();
		unsigned long current_time = getTime();
		for (int i = 0; i < reduce_tasks_size; ++i) {
			/* If the task has not been assigned or timeout, assgin the task */
			if (this->reduceTasks[i].isCompleted) continue;
			if (!this->reduceTasks[i].isAssigned || 
				this->reduceTasks[i].isAssigned && current_time - this->reduceTasksTime[i] >= 80){
				COORDINATOR_LOG("Assign a reduce task!");
				this->reduceTasks[i].isAssigned = true;
				this->reduceTasksTime[i] = current_time;
				reply.task_type = mr_tasktype::REDUCE;
				reply.index = i;
				break;
			}
		}
		if (reply.task_type == mr_tasktype::NONE) 
			COORDINATOR_LOG("Can't get Reduce work");
		mtx.unlock();
	}
	/* All Works Done: update isFinished */
	else {
		mtx.lock();
		COORDINATOR_LOG("All works done!");
		this->isFinished = true;
		reply.index = -1;
		mtx.unlock();
	}
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	// Lab4 : Your code goes here.
	std::unique_lock<std::mutex> lock(this->mtx);
	if (taskType == mr_tasktype::MAP) {
		++completedMapCount;
		this->mapTasks[index].isCompleted = true;
		success = true;
	}
	else {
		++completedReduceCount;
		this->reduceTasks[index].isCompleted = true;
		success = true;
	}
	return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

unsigned long Coordinator::getTime() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

bool Coordinator::isFinishedMap() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}

	/* Initialize mapTasksTime & reduceTasksTime */
	mapTasksTime.resize(filesize, 0);
	reduceTasksTime.resize(nReduce, 0);
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];
	
	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);
	
	//
	// Lab4: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	// 
	COORDINATOR_LOG("Register askTask and submitTask");
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
	server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while(!c.Done()) {
		sleep(1);
	}

	return 0;
}


