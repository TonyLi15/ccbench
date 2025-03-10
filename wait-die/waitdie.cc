
#include <ctype.h>  //isdigit,
#include <pthread.h>
#include <string.h>       //strlen,
#include <sys/syscall.h>  //syscall(SYS_gettid),
#include <sys/types.h>    //syscall(SYS_gettid),
#include <unistd.h>       //syscall(SYS_gettid),
#include <x86intrin.h>

#include <iostream>
#include <string>  //string
#include <thread>

#define GLOBAL_VALUE_DEFINE

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/fence.hh"
#include "../include/int64byte.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/procedure.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"
#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

// #define NONTS
// #define INTERACTIVE

#ifndef NONTS
long long int central_timestamp = 0; //*** added by tatsu
#endif

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Result &myres = std::ref(SS2PLResult[thid]);
  Xoroshiro128Plus rnd;
  rnd.init();
  TxExecutor trans(thid, (Result *) &myres);
  FastZipf zipf(&rnd, FLAGS_zipf_skew, FLAGS_tuple_num);
  Backoff backoff(FLAGS_clocks_per_us);

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

#ifdef Linux
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", *myid, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %ld\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif  // Linux

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  while (!loadAcquire(quit)) {
    makeProcedure(trans.pro_set_, rnd, zipf, FLAGS_tuple_num, FLAGS_max_ope, FLAGS_thread_num,
                  FLAGS_rratio, FLAGS_rmw, FLAGS_ycsb, false, thid, myres);
#ifndef NONTS
    trans.txid_ = __atomic_add_fetch(&central_timestamp, 1, __ATOMIC_SEQ_CST); //*** added by tatsu
#endif
RETRY:
    if (loadAcquire(quit)) break;
    if (thid == 0) leaderBackoffWork(backoff, SS2PLResult);

    trans.begin();
    for (auto itr = trans.pro_set_.begin(); itr != trans.pro_set_.end();
         ++itr) {
      if ((*itr).ope_ == Ope::READ) {
#ifdef INTERACTIVE
        usleep(1);
#endif
        trans.read((*itr).key_);
      } else if ((*itr).ope_ == Ope::WRITE) {
#ifdef INTERACTIVE
        usleep(1);
#endif
        trans.write((*itr).key_);
      } else if ((*itr).ope_ == Ope::READ_MODIFY_WRITE) {
#ifdef INTERACTIVE
        usleep(1);
#endif
        trans.readWrite((*itr).key_);
      } else {
        ERR;
      }

      if (trans.status_ == TransactionStatus::aborted) {
        trans.abort();
        goto RETRY;
      }
    }

    trans.commit();
    /**
     * local_commit_counts is used at ../include/backoff.hh to calcurate about
     * backoff.
     */
    storeRelease(myres.local_commit_counts_,
                 loadAcquire(myres.local_commit_counts_) + 1);
  }

  return;
}

void touchTuples([[maybe_unused]] size_t thid, uint64_t start, uint64_t end) {
  Result &myres = std::ref(SS2PLResult[thid]);
  TxExecutor trans(thid, (Result *)&myres);
  for (auto i = start; i <= end; ++i) {
    trans.warmupTuple(i);
  } 
}

void warmup() {
  cout << "begin warm up" << endl;
  size_t maxthread = decideParallelBuildNumber(FLAGS_tuple_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < maxthread; ++i) {
    thv.emplace_back(touchTuples, i, i * (FLAGS_tuple_num / maxthread),
                    (i + 1) * (FLAGS_tuple_num / maxthread) - 1);
  }
  for (auto &th : thv) th.join();
  cout << "finish warm up" << endl;
}

int main(int argc, char *argv[]) try {
  gflags::SetUsageMessage("2PL benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  makeDB();
  for (int i = 0; i < FLAGS_thread_num; i++){ //*** added by tatsu
    thread_stats[i] = 0;                      //*** added by tatsu
    thread_timestamp[i] = 0;                  //*** added by tatsu
  }                                           //*** added by tatsu
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  warmup();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv) th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    SS2PLResult[0].addLocalAllResult(SS2PLResult[i]);
  }
  ShowOptParameters();
  SS2PLResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, FLAGS_thread_num);

  return 0;
} catch (bad_alloc) {
  ERR;
}
