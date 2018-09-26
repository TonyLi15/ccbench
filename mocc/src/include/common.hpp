#pragma once

#include "int64byte.hpp"
#include "procedure.hpp"
#include "random.hpp"
#include "tuple.hpp"
#include <atomic>

#ifdef GLOBAL_VALUE_DEFINE
	#define GLOBAL

GLOBAL std::atomic<unsigned int> Running(0);
GLOBAL std::atomic<uint64_t> GlobalEpoch(1);
GLOBAL std::atomic<bool> Finish(false);

#else
	#define GLOBAL extern

GLOBAL std::atomic<int> Running;
GLOBAL std::atomic<uint64_t> GlobalEpoch;
GLOBAL std::atomic<bool> Finish;

#endif

GLOBAL uint64_t_64byte *ThLocalEpoch;

// run-time args
GLOBAL unsigned int TUPLE_NUM;
GLOBAL unsigned int MAX_OPE;
GLOBAL unsigned int THREAD_NUM;
GLOBAL unsigned int WORKLOAD;
GLOBAL uint64_t CLOCK_PER_US;
GLOBAL uint64_t EPOCH_TIME;
GLOBAL unsigned int EXTIME;
GLOBAL unsigned int UNSTABLE_WORKLOAD;

GLOBAL uint64_t_64byte *ThRecentTID;
GLOBAL uint64_t *FinishTransactions;
GLOBAL uint64_t *UnstaFinishTransactions;	// use for unstable workload
GLOBAL uint64_t *AbortCounts;

GLOBAL RWLock CtrLock;

// ログ永続化エミュレーション用
GLOBAL uint64_t_64byte *Start;	
GLOBAL uint64_t_64byte *Stop;

GLOBAL uint64_t Bgn;
GLOBAL uint64_t End;

GLOBAL Tuple *Table;
