#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.
		int task_type;					// Task type allocated to worker
		int index;						// MAP: The index to file | REDUCE: The reduce task num
		vector<string> filenames;		// MAP: file needs doing maping
	};

	struct AskTaskRequest {
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

marshall &operator<<(marshall &m, const mr_protocol::AskTaskResponse &response) {
	m << response.task_type;
	m << response.index;
	m << response.filenames;
	return m;
}

unmarshall &operator>>(unmarshall &u, mr_protocol::AskTaskResponse &response) {
	u >> response.task_type;
	u >> response.index;
	u >> response.filenames;
	return u;
}

#endif

