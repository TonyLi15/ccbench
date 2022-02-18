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

class TxExecutor {
public:
  alignas(CACHE_LINE_SIZE) int thid_;
  TransactionStatus status_ = TransactionStatus::inFlight;
  Result *sres_;
  vector <SetElement<Tuple>> read_set_;
  vector <SetElement<Tuple>> write_set_;
  vector <Procedure> pro_set_;

  char write_val_[VAL_SIZE];
  char return_val_[VAL_SIZE];

  TxExecutor(int thid, Result *sres) : thid_(thid), sres_(sres) {
    read_set_.reserve(FLAGS_max_ope);
    write_set_.reserve(FLAGS_max_ope);
    pro_set_.reserve(FLAGS_max_ope);

    genStringRepeatedNumber(write_val_, VAL_SIZE, thid);
  }

  SetElement<Tuple> *searchReadSet(uint64_t key);

  SetElement<Tuple> *searchWriteSet(uint64_t key);

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

  void LockAcquire(Tuple *tuple, LockType lock_type, uint64_t key);

  void LockRelease(Tuple *tuple, bool is_abort, uint64_t key);

  void LockRetire(Tuple *tuple, uint64_t key);

  bool spinWait(Tuple *tuple, uint64_t key);

  bool lockUpgrade(Tuple *tuple, uint64_t key);

  void checkLists(uint64_t key);

  void eraseFromLists(Tuple *tuple); // erase txn from waiters and owners lists in case of abort during spinwait

  bool woundSuccess(Tuple *tuple, const int killer, const LockType my_type);

  vector<int>::iterator woundRelease(int txn, Tuple *tuple, uint64_t key);
  
  void cascadeAbort(int txn, vector<int> all_owners, Tuple *tuple, uint64_t key);

  void addCommitSemaphore(Tuple *tuple, int t, LockType t_type);
};
