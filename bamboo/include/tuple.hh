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

enum ListType 
{
    RETIRED,
    WAITER
};

class Tuple
{
public:
  alignas(CACHE_LINE_SIZE) RWLock lock_;
  char val_[VAL_SIZE];
  int readers[224] = {0}; // *** added by tatsu: readers[i] = 1 means thread i is reading this tuple
  int writers[224] = {0}; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  int req_type[224] = {0}; // read -1 : write 1 : no touch 0

  vector<int> retired; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> owners;  // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
  vector<int> waiters; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple

  template <typename T> bool sortAdd(int txn, T &list);
  void ownersAdd(int txn);
  template <typename T> bool remove(int txn, T &list);

  // template <typename T>
  // void sortAdd(int txn, T &list)
  // {
  //   if (list.size() == 0)
  //   {
  //     list.push_back(txn);
  //     return;
  //   }
  //   for (auto tid = list.begin(); tid != list.end(); tid++)
  //   { // reverse_iterator might be better
  //     if (thread_timestamp[txn] < thread_timestamp[(*tid)])
  //     {
  //       list.insert(tid, txn);
  //       return;
  //     }
  //   }
  // }

  // template <typename T>
  // void remove(int txn, T &list)
  // {
  //   for (int i = 0; i < list.size(); i++)
  //   {
  //     if (txn == list[i])
  //     {
  //       list.erase(list.begin() + i);
  //       return;
  //     }
  //   }
  // }
};
