#pragma once

#include <vector>

#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/rwlock.hh"
#include "../../include/string.hh"
#include "../../include/util.hh"
#include "ss2pl_op_element.hh"
#include "tuple.hh"

enum class TransactionStatus : uint8_t {
  inFlight,
  committed,
  aborted,
};

extern void writeValGenerator(char *writeVal, size_t val_size, size_t thid);

struct myVector {
  int8_t size;
#if FAIR
  int arr[224];
#else
  int8_t arr[224];
#endif
  myVector() {
    size = 0;
    for (int i = 0; i < 224; i++) arr[i] = 0;
  }
};

class TxExecutor {
public:
  alignas(CACHE_LINE_SIZE) int thid_;
  int txid_;
  TransactionStatus status_ = TransactionStatus::inFlight;
  Result *sres_;
  vector <SetElement<Tuple>> read_set_;
  vector <SetElement<Tuple>> write_set_;
  vector <Procedure> pro_set_;
  myVector all_owners;
  char write_val_[VAL_SIZE];
  char return_val_[VAL_SIZE];

  TxExecutor(int thid, Result *sres) : thid_(thid), sres_(sres), txid_(thid) {
    read_set_.reserve(FLAGS_max_ope);
    write_set_.reserve(FLAGS_max_ope);
    pro_set_.reserve(FLAGS_max_ope);

    genStringRepeatedNumber(write_val_, VAL_SIZE, thid);
    genStringRepeatedNumber(return_val_, VAL_SIZE, thid);
  }

  SetElement<Tuple> *searchReadSet(uint64_t key);

  SetElement<Tuple> *searchWriteSet(uint64_t key);

  void warmupTuple(uint64_t key);

  void begin();

  void read(uint64_t key);

  void write(uint64_t key, bool should_retire);

  void readWrite(uint64_t key, bool should_retire);

  void commit();

  void abort();

  void unlockList(bool is_abort);

  // inline
  Tuple *get_tuple(Tuple *table, uint64_t key) { return &table[key]; }

  bool conflict(LockType x, LockType y);

  void checkWound(vector<int> &list, LockType lock_type, Tuple *tuple, uint64_t key);

  void PromoteWaiters(Tuple *tuple);

  void writelockAcquire(LockType lock_type, uint64_t key, Tuple *tuple);

  bool LockRelease(bool is_abort, uint64_t key, Tuple *tuple);

  void LockRetire(uint64_t key, Tuple *tuple);

  bool spinWait(uint64_t key, Tuple *tuple);

  bool lockUpgrade(uint64_t key, Tuple *tuple);

  void checkLists(uint64_t key, Tuple *tuple);

  void eraseFromLists(Tuple *tuple); // erase txn from waiters and owners lists in case of abort during spinwait

  vector<int>::iterator woundRelease(int txn, Tuple *tuple, uint64_t key);
  
  void cascadeAbort(int txn, Tuple *tuple, uint64_t key);

  void concat(vector<int> &r, vector<int> &o);

  void addCommitSemaphore(int t, LockType t_type, Tuple *tuple);

  bool readlockAcquire(LockType lock_type, uint64_t key, Tuple *tuple);

  bool readWait(Tuple *tuple, uint64_t key);
  
  bool adjustFollowingSemaphore(Tuple *tuple, int txn);
};
