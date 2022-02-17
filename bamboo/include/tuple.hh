#pragma once

#include <atomic>
#include <mutex>

#include "../../include/cache_line_size.hh"
#include "../../include/inline.hh"
#include "../../include/rwlock.hh"

// int thread_timestamp[224];

using namespace std;

enum LockType 
{
    SH = -1,
    EX = 1
};

class Tuple
{
public:
  alignas(CACHE_LINE_SIZE) RWLock lock_;
  char val_[VAL_SIZE];
  char prev_val_[224][VAL_SIZE];
  int req_type[224] = {0}; // read -1 : write 1 : no touch 0

  vector<int> retired; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> owners;  // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> waiters; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple

  bool sortAdd(int txn, vector<int> &list);
  void ownersAdd(int txn);
  bool remove(int txn, vector<int> &list);
};
