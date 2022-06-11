#pragma once

#include <atomic>
#include <mutex>
#include <bitset>

#include "../../include/cache_line_size.hh"
#include "../../include/inline.hh"
#include "../../include/rwlock.hh"

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
  vector<int> owners;  // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> retired; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> waiters; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  char val_[VAL_SIZE];
  int8_t req_type[224] = {0}; // read -1 : write 1 : no touch 0
  // bitset<224> req_stats;  
  // bitset<224> req_type;  
  // even index => read 0 : write 1
  // odd index => no lock 0 : hold lock 1
  // check holding lock or not
  // bit[2*thid_+1] == 1 => hold lock, else no lock
  // check lock type
  // bit[2*thid] == 1 => write lock, else read lock



  
  Tuple() {
    retired.reserve(224);
    owners.reserve(224);
    waiters.reserve(224);
  }

  bool sortAdd(int txn, vector<int> &list);
  void ownersAdd(int txn);
  bool ownersRemove(int txn);
  bool remove(int txn, vector<int> &list);
  vector<int>::iterator itrRemove(int txn);
};
