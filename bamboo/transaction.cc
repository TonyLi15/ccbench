#if NONTS == 0

#include <stdio.h>
#include <string.h>
#include <signal.h>

#include <atomic>

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/procedure.hh"
#include "../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"

#define OPT1

using namespace std;

int checkDuplicate(vector<int> &x, int t, int key)
{
  int index = key;
  while (thread_timestamp[x[index]] == thread_timestamp[t])
  {
    if (x[index] == t)
      return index;
    index--;
  }
  index = key;
  while (thread_timestamp[x[index]] == thread_timestamp[t])
  {
    if (x[index] == t)
      return index;
    index++;
  }
  return -1;
}

int myBinarySearch(vector<int> &x, int goal, int tail)
{
  int head = 0;
  tail = tail - 1;
  while (1)
  {
    int search_key = floor((head + tail) / 2);
    if (thread_timestamp[x[search_key]] == thread_timestamp[goal])
    {
#if RANDOM
      assert(x[search_key] == goal);
#endif
      return search_key;
    }
    else if (thread_timestamp[goal] > thread_timestamp[x[search_key]])
    {
      head = search_key + 1;
    }
    else if (thread_timestamp[goal] < thread_timestamp[x[search_key]])
    {
      tail = search_key - 1;
    }
    if (tail < head)
    {
      return -1;
    }
  }
}
int myBinaryInsert(vector<int> &x, int goal, int tail)
{
  int head = 0;
  tail = tail - 1;
  while (1)
  {
    int search_key = floor((head + tail) / 2);
    if (thread_timestamp[x[search_key]] == thread_timestamp[goal])
    {
      return search_key;
    }
    else if (thread_timestamp[goal] > thread_timestamp[x[search_key]])
    {
      head = search_key + 1;
    }
    else if (thread_timestamp[goal] < thread_timestamp[x[search_key]])
    {
      tail = search_key - 1;
    }
    if (tail < head)
    {
      return head;
    }
  }
}

extern void display_procedure_vector(std::vector<Procedure> &pro);

/**
 * @brief Search xxx set
 * @detail Search element of local set corresponding to given key.
 * In this prototype system, the value to be updated for each worker thread
 * is fixed for high performance, so it is only necessary to check the key match.
 * @param Key [in] the key of key-value
 * @return Corresponding element of local set
 */
inline SetElement<Tuple> *TxExecutor::searchReadSet(uint64_t key)
{
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    if ((*itr).key_ == key)
      return &(*itr);
  }

  return nullptr;
}

/**
 * @brief Search xxx set
 * @detail Search element of local set corresponding to given key.
 * In this prototype system, the value to be updated for each worker thread
 * is fixed for high performance, so it is only necessary to check the key match.
 * @param Key [in] the key of key-value
 * @return Corresponding element of local set
 */
inline SetElement<Tuple> *TxExecutor::searchWriteSet(uint64_t key)
{
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    if ((*itr).key_ == key)
      return &(*itr);
  }

  return nullptr;
}

void TxExecutor::warmupTuple(uint64_t key) {
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#else
  tuple = get_tuple(Table, key);
#endif
  tuple->retired.emplace_back(thid_);
  tuple->remove(thid_, tuple->retired);
  tuple->owners.emplace_back(thid_);
  tuple->remove(thid_, tuple->owners);
  tuple->waiters.emplace_back(thid_);
  tuple->remove(thid_, tuple->waiters);

  memcpy(tuple->val_, write_val_, VAL_SIZE);
  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    tuple->req_type[i] = 1;
    tuple->req_type[i] = 0;
  }
}

uint64_t get_sys_clock() {
  static atomic<uint64_t> fake_clock;
  return fake_clock.fetch_add(100, memory_order_seq_cst);
}

/**
 * @brief function about abort.
 * Clean-up local read/write set.
 * Release locks.
 * @return void
 */
void TxExecutor::abort()
{
  /**
   * Release locks
   */
  unlockList(true);

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
  ++sres_->local_abort_counts_;

#if BACK_OFF
#if ADD_ANALYSIS
  uint64_t start(rdtscp());
#endif

  Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
  sres_->local_backoff_latency_ += rdtscp() - start;
#endif
#endif
}

/**
 * @brief success termination of transaction.
 * @return void
 */
void TxExecutor::commit()
{

  /**
   * Release locks.
   */
  unlockList(false);
  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
}

/**
 * @brief Initialize function of transaction.
 * Allocate timestamp.
 * @return void
 */
void TxExecutor::begin() { this->status_ = TransactionStatus::inFlight;}

/**
 * @brief Transaction read function.
 * @param [in] key The key of key-value
 */
void TxExecutor::read(uint64_t key)
{
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif // ADD_ANALYSIS
  /**
   * read-own-writes or re-read from local read set.
   */
  if (searchWriteSet(key) || searchReadSet(key))
    goto FINISH_READ;

  /**
   * Search tuple from data structure.
   */
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif
  if (readlockAcquire(LockType::SH, key, tuple)) goto FINISH_READ;
  spinWait(key, tuple);

FINISH_READ:
#if ADD_ANALYSIS
  sres_->local_read_latency_ += rdtscp() - start;
#endif
  return;
}

/**
 * @brief transaction write operation
 * @param [in] key The key of key-value
 * @return void
 */
void TxExecutor::write(uint64_t key, bool should_retire)
{
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif
  // if it already wrote the key object once.
  // if (searchWriteSet(key) || searchReadSet(key))
  //   goto FINISH_WRITE;
  if (searchWriteSet(key))
  {
    goto FINISH_WRITE;
  }
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif

  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    {
      if (lockUpgrade(key, tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(key, tuple))
        {
          read_set_.erase(rItr);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(key, tuple);
        }
      }
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  writelockAcquire(LockType::EX, key, tuple);
  if (spinWait(key, tuple))
  {
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(key, tuple);
  }

  /**
   * Register the contents to write lock list and write set.
   */

FINISH_WRITE:
#if ADD_ANALYSIS
  sres_->local_write_latency_ += rdtscp() - start;
#endif // ADD_ANALYSIS
  return;
}

/**
 * @brief transaction readWrite (RMW) operation
 */
void TxExecutor::readWrite(uint64_t key, bool should_retire)
{
  // if it already wrote the key object once.
  if (searchWriteSet(key))
  {
    goto FINISH_WRITE;
  }
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    {
      if (lockUpgrade(key, tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(key, tuple))
        {
          read_set_.erase(rItr);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(key, tuple);
        }
      }
      goto FINISH_WRITE;
    }
  }
  /**
   * Search tuple from data structure.
   */
  writelockAcquire(LockType::EX, key, tuple);
  if (spinWait(key, tuple))
  {
    // read payload
    memcpy(this->return_val_, tuple->val_, VAL_SIZE);
    // finish read.
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(key, tuple);
  }

FINISH_WRITE:
  return;
}

/**
 * @brief unlock and clean-up local lock set.
 * @return void
 */
void TxExecutor::unlockList(bool is_abort)
{
  Tuple *tuple;
  bool shouldRollback;
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    LockRelease(is_abort, (*itr).key_, tuple);
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    if (tuple->req_type[thid_] == 0)
    {
      continue;
    }
    shouldRollback = LockRelease(is_abort, (*itr).key_, tuple);
    if (is_abort && shouldRollback) 
      memcpy(tuple->val_, (*itr).val_, VAL_SIZE);
  }
}

bool TxExecutor::conflict(LockType x, LockType y)
{
  if ((x == LockType::EX) || (y == LockType::EX))
    return true;
  else
    return false;
}

void TxExecutor::addCommitSemaphore(int t, LockType t_type, Tuple *tuple)
{
  int r;
  LockType retired_type;
  for (int i = 0; i < tuple->retired.size(); i++)
  {
    r = tuple->retired[i];
    retired_type = (LockType)tuple->req_type[r];
    if (conflict(t_type, retired_type))
    {
      __atomic_add_fetch(&commit_semaphore[t], 1, __ATOMIC_SEQ_CST);
      break;
    }
  }
}

void TxExecutor::PromoteWaiters(Tuple *tuple)
{
  int t;
  int owner;
  LockType t_type;
  LockType owners_type;
  bool owner_exists = false;

  int r;
  LockType retired_type;

  while (tuple->waiters.size())
  {
    if (tuple->owners.size())
    {
      owner = tuple->owners[0];
      owners_type = (LockType)tuple->req_type[owner];
      owner_exists = true;
    }
    t = tuple->waiters[0];
    t_type = (LockType)tuple->req_type[t];
    if (owner_exists && conflict(t_type, owners_type))
    {
      break;
    }
    tuple->remove(t, tuple->waiters);
    tuple->ownersAdd(t);
    addCommitSemaphore(t, t_type, tuple);
  }
}

void TxExecutor::checkWound(vector<int> &list, LockType lock_type, Tuple *tuple, uint64_t key)
{
  int t;
  bool has_conflicts;
  LockType type;
  for (auto it = list.begin(); it != list.end();)
  {
    t = (*it);
    type = (LockType)tuple->req_type[t];
    has_conflicts = false;
    if (thid_ != t && conflict(lock_type, type))
    {
      has_conflicts = true;
    }
    if (has_conflicts == true && thread_timestamp[thid_] <= thread_timestamp[t])
    {
      thread_stats[t] = 1;
      it = woundRelease(t, tuple, key);
    }
    else
    {
      ++it;
    }
  }
}

void TxExecutor::writelockAcquire(LockType EX_lock, uint64_t key, Tuple *tuple)
{
  tuple->req_type[thid_] = EX_lock;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, EX_lock, tuple, key);
      checkWound(tuple->owners, EX_lock, tuple, key);
      if (tuple->owners.size() == 0 && 
      (tuple->waiters.size() == 0 || thread_timestamp[thid_] < thread_timestamp[tuple->waiters[0]]))
      {
        tuple->ownersAdd(thid_);
        addCommitSemaphore(thid_, EX_lock, tuple);
      }
      else
      {
        tuple->sortAdd(thid_, tuple->waiters);
        PromoteWaiters(tuple);
      }
      tuple->lock_.w_unlock();
      return;
    }
    usleep(1);
  }
}

void TxExecutor::cascadeAbort(int txn, Tuple *tuple, uint64_t key)
{
  int t;
  concat(tuple->retired, tuple->owners);
  for (int i = 0; i < all_owners.size; i++)
  {
    if (txn == all_owners.arr[i])
    {
      for (int j = i + 1; j < all_owners.size; j++)
      {
        t = all_owners.arr[j];
        thread_stats[t] = 1;
        if (tuple->remove(t, tuple->retired) == false &&
            tuple->ownersRemove(t) == false)
        {
          exit(1);
        }
        tuple->req_type[t] = 0;
      }
      return;
    }
  }
}

void TxExecutor::concat(vector<int> &r, vector<int> &o)
{
  for (int i = 0; i < all_owners.size; i++) all_owners.arr[i] = 0;

  int i, j;
  for (i = 0; i < r.size(); i++) {
    all_owners.arr[i] = r[i];
  }
  for (j = 0; j < o.size(); j++, i++) {
    all_owners.arr[i] = o[j];
  }
  all_owners.size = i;
}

vector<int>::iterator TxExecutor::woundRelease(int txn, Tuple *tuple, uint64_t key)
{
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[txn];
  int head;
  LockType head_type;
  // auto all_owners = concat(tuple->retired, tuple->owners);

  if (tuple->retired.size() && tuple->retired[0] == txn)
  {
    was_head = true;
  }
  if (type == LockType::EX)
  {
    cascadeAbort(txn, tuple, key);
    TxExecutor *trans = TxPointers[txn];
    for (auto itr = trans->write_set_.begin(); itr != trans->write_set_.end(); ++itr)
    {
      if ((*itr).key_ == key) {
        memcpy(tuple->val_, (*itr).val_, VAL_SIZE);
        break;
      }
    }
  }
  auto it = tuple->itrRemove(txn);
  concat(tuple->retired, tuple->owners);
  if (all_owners.size)
  {
    head = all_owners.arr[0];
    head_type = (LockType)tuple->req_type[head];
    if (was_head && conflict(type, head_type))
    {
      for (int i = 0; i < all_owners.size; i++)
      {
        __atomic_add_fetch(&commit_semaphore[all_owners.arr[i]], -1, __ATOMIC_SEQ_CST);
        if ((i + 1) < all_owners.size &&
            conflict((LockType)tuple->req_type[all_owners.arr[i]], (LockType)tuple->req_type[all_owners.arr[i + 1]])) // CAUTION: may be wrong
          break;
      }
    }
  }
  tuple->req_type[txn] = 0;
  return it;
}

bool TxExecutor::LockRelease(bool is_abort, uint64_t key, Tuple *tuple)
{
  bool was_head;
  LockType type = (LockType)tuple->req_type[thid_];
  int head;
  LockType head_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      if (tuple->req_type[thid_] == 0)
      {
        tuple->lock_.w_unlock();
        return false;
      }
      // auto all_owners = concat(tuple->retired, tuple->owners);
      was_head = false;
      if (tuple->retired.size() > 0 && tuple->retired[0] == thid_)
      {
        was_head = true;
      }
      if (is_abort && type == LockType::EX)
      {
        cascadeAbort(thid_, tuple, key);
      }
      if (tuple->remove(thid_, tuple->retired) == false &&
          tuple->ownersRemove(thid_) == false)
      {
        exit(1);
      }
      concat(tuple->retired, tuple->owners);
      if (all_owners.size)
      {
        head = all_owners.arr[0];
        head_type = (LockType)tuple->req_type[head];
        if (was_head && conflict(type, head_type))
        {
          for (int i = 0; i < all_owners.size; i++)
          {
            __atomic_add_fetch(&commit_semaphore[all_owners.arr[i]], -1, __ATOMIC_SEQ_CST);
            if ((i + 1) < all_owners.size &&
                conflict((LockType)tuple->req_type[all_owners.arr[i]], (LockType)tuple->req_type[all_owners.arr[i + 1]])) // CAUTION: may be wrong
              break;
          }
        }
      }
      tuple->req_type[thid_] = 0;
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return true;
    }
    if (tuple->req_type[thid_] == 0)
      return false;
    usleep(1);
  }
}

void TxExecutor::LockRetire(uint64_t key, Tuple *tuple)
{
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      if (tuple->req_type[thid_] == 0)
      {
        tuple->lock_.w_unlock();
        return;
      }
      tuple->ownersRemove(thid_);
      tuple->sortAdd(thid_, tuple->retired);
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    if (tuple->req_type[thid_] == 0)
      return;
    usleep(1);
  }
}

vector<int>::iterator Tuple::itrRemove(int txn)
{
  vector<int>::iterator it;
  int i;
  if (retired.size() > 0) {
    i = myBinarySearch(retired, txn, retired.size());
    if (i != -1) {
      assert(txn == *(list.begin() + i));
      it = retired.erase(retired.begin() + i);
      return it;
    }
  }
  for (i = 0; i < owners.size(); i++)
  {
    if (txn == owners[i])
    {
      it = owners.erase(owners.begin() + i);
      return it;
    }
  }
  printf("ERROR: itrRemove FAILURE\n");
  exit(1);
}

bool Tuple::ownersRemove(int txn)
{
  for (int i = 0; i < owners.size(); i++)
  {
    if (txn == owners[i])
    {
      owners.erase(owners.begin() + i);
      return true;
    }
  }
  return false;
}

bool Tuple::remove(int txn, vector<int> &list)
{
  if (list.size() == 0)
    return false;
  int i = myBinarySearch(list, txn, list.size());
  if (i == -1)
    return false;
  assert(txn == *(list.begin() + i));
  list.erase(list.begin() + i);
  return true;
}

bool Tuple::sortAdd(int txn, vector<int> &list)
{
  if (list.size() == 0)
  {
    list.emplace_back(txn);
    return true;
  }
  int i = myBinaryInsert(list, txn, list.size());
  list.insert(list.begin() + i, txn);
  return true;
}

bool TxExecutor::spinWait(uint64_t key, Tuple *tuple)
{
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      for (int i = 0; i < tuple->owners.size(); i++)
      {
        if (thid_ == tuple->owners[i])
        {
#ifdef OPT1
          // optimization 1: read lock retire without latch
          if (tuple->req_type[thid_] == -1)
          {
            read_set_.emplace_back(key, tuple, tuple->val_);
            tuple->ownersRemove(thid_);
            tuple->sortAdd(thid_, tuple->retired);
            PromoteWaiters(tuple);
          }
#endif
          else if (tuple->req_type[thid_] == 1) 
          {
            write_set_.emplace_back(key, tuple, tuple->val_);
          }
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (thread_stats[thid_] == 1)
      {
        eraseFromLists(tuple);
        PromoteWaiters(tuple);
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}

void TxExecutor::eraseFromLists(Tuple *tuple)
{
  tuple->req_type[thid_] = 0;
  if (tuple->remove(thid_, tuple->waiters)) return;
  tuple->ownersRemove(thid_);
}

bool TxExecutor::lockUpgrade(uint64_t key, Tuple *tuple)
{
  bool is_retired;
  const LockType my_type = LockType::SH;
  int i;

  int r;
  LockType retired_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, LockType::EX, tuple, key);
      checkWound(tuple->owners, LockType::EX, tuple, key);
      is_retired = false;
      for (i = 0; i < tuple->retired.size(); i++)
      {
        if (thid_ == tuple->retired[i])
        {
          is_retired = true;
          break;
        }
      }
      if (is_retired)
      {
        if (tuple->owners.size() == 0)
        {
          if (i > 0)
          {
            for (int j = 0; j < i; j++)
            {
              r = tuple->retired[j];
              retired_type = (LockType)tuple->req_type[r];
              if (thread_timestamp[thid_] > thread_timestamp[r] &&
                  conflict(my_type, retired_type))
              {
                break;
              }
              if (j + 1 == i)
              {
                __atomic_add_fetch(&commit_semaphore[thid_], 1, __ATOMIC_SEQ_CST);
              }
            }
          }
          tuple->remove(thid_, tuple->retired);
          tuple->ownersAdd(thid_);
          tuple->req_type[thid_] = LockType::EX;
          tuple->lock_.w_unlock();
          return true;
        }
      }
      else
      {
        if (tuple->owners.size() == 1 && tuple->owners[0] == thid_)
        {
          tuple->req_type[thid_] = LockType::EX;
          for (int i = 0; i < tuple->retired.size(); i++)
          {
            r = tuple->retired[i];
            retired_type = (LockType)tuple->req_type[r];
            if (thread_timestamp[thid_] > thread_timestamp[r] &&
                retired_type == LockType::SH)
            {
              __atomic_add_fetch(&commit_semaphore[thid_], 1, __ATOMIC_SEQ_CST);
              break;
            }
          }
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (thread_stats[thid_] == 1)
      {
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}

bool TxExecutor::readlockAcquire(LockType SH_lock, uint64_t key, Tuple *tuple)
{
  tuple->req_type[thid_] = SH_lock;
  bool is_retired = false;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, SH_lock, tuple, key);
      checkWound(tuple->owners, SH_lock, tuple, key);
      if (tuple->owners.size() == 0 && 
      (tuple->waiters.size() == 0 || thread_timestamp[thid_] < thread_timestamp[tuple->waiters[0]]))
      {
        read_set_.emplace_back(key, tuple, tuple->val_);
        tuple->sortAdd(thid_, tuple->retired);
        addCommitSemaphore(thid_, SH_lock, tuple);
        is_retired = true;
      }
      else
      {
        tuple->sortAdd(thid_, tuple->waiters);
        PromoteWaiters(tuple);
      }
      tuple->lock_.w_unlock();
      return is_retired;
    }
    usleep(1);
  }
}

// bool TxExecutor::adjustFollowingSemaphore(Tuple *tuple, int txn) {
//   LockType t_type = LockType::SH;
//   int follower;
//   LockType f_type;

//   if (tuple->retired.size() && thread_timestamp[txn] < thread_timestamp[tuple->retired[0]]) {
//     follower = tuple->retired[0];
//     f_type = (LockType)tuple->req_type[follower];
//     if (f_type == LockType::EX) {
//       if(pending_commit[follower] == 1) return false;
//       __atomic_add_fetch(&commit_semaphore[follower], 1, __ATOMIC_SEQ_CST);
//     }
//     return true;
//   }
//   if (tuple->owners.size() && thread_timestamp[txn] < thread_timestamp[tuple->owners[0]]) {
//     follower = tuple->owners[0];
//     f_type = (LockType)tuple->req_type[follower];
//     if (f_type == LockType::EX) {
//       if(pending_commit[follower] == 1) return false;
//       __atomic_add_fetch(&commit_semaphore[follower], 1, __ATOMIC_SEQ_CST);
//     }
//     return true;
//   }
//   return true;
// }

bool TxExecutor::readWait(Tuple *tuple, uint64_t key)
{
  // this implementation cannot guarantee correctness yet
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      for (int i = 0; i < tuple->retired.size(); i++)
      {
        if (thid_ == tuple->retired[i])
        {
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (thread_stats[thid_] == 1)
      {
        eraseFromLists(tuple);
        PromoteWaiters(tuple);
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}

#else

#include <stdio.h>
#include <string.h>
#include <signal.h>

#include <atomic>

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/procedure.hh"
#include "../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"

#define OPT1

using namespace std;

int myBinarySearch(vector<int> &x, int goal, int tail)
{
  int head = 0;
  tail = tail - 1;
  while (1)
  {
    int search_key = floor((head + tail) / 2);
    if (x[search_key] == goal)
    {
      return search_key;
    }
    else if (goal > x[search_key])
    {
      head = search_key + 1;
    }
    else if (goal < x[search_key])
    {
      tail = search_key - 1;
    }
    if (tail < head)
    {
      return -1;
    }
  }
}
int myBinaryInsert(vector<int> &x, int goal, int tail)
{
  int head = 0;
  tail = tail - 1;
  while (1)
  {
    int search_key = floor((head + tail) / 2);
    if (x[search_key] == goal)
    {
      printf("ERROR: myBinaryInsert\n");
      exit(1);
    }
    else if (goal > x[search_key])
    {
      head = search_key + 1;
    }
    else if (goal < x[search_key])
    {
      tail = search_key - 1;
    }
    if (tail < head)
    {
      return head;
    }
  }
}

extern void display_procedure_vector(std::vector<Procedure> &pro);

/**
 * @brief Search xxx set
 * @detail Search element of local set corresponding to given key.
 * In this prototype system, the value to be updated for each worker thread
 * is fixed for high performance, so it is only necessary to check the key match.
 * @param Key [in] the key of key-value
 * @return Corresponding element of local set
 */
inline SetElement<Tuple> *TxExecutor::searchReadSet(uint64_t key)
{
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    if ((*itr).key_ == key)
      return &(*itr);
  }

  return nullptr;
}

/**
 * @brief Search xxx set
 * @detail Search element of local set corresponding to given key.
 * In this prototype system, the value to be updated for each worker thread
 * is fixed for high performance, so it is only necessary to check the key match.
 * @param Key [in] the key of key-value
 * @return Corresponding element of local set
 */
inline SetElement<Tuple> *TxExecutor::searchWriteSet(uint64_t key)
{
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    if ((*itr).key_ == key)
      return &(*itr);
  }

  return nullptr;
}

void TxExecutor::warmupTuple(uint64_t key) {
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#else
  tuple = get_tuple(Table, key);
#endif
  tuple->retired.emplace_back(thid_);
  tuple->remove(thid_, tuple->retired);
  tuple->owners.emplace_back(thid_);
  tuple->remove(thid_, tuple->owners);
  tuple->waiters.emplace_back(thid_);
  tuple->remove(thid_, tuple->waiters);

  memcpy(tuple->val_, write_val_, VAL_SIZE);
  for (unsigned int i = 0; i < FLAGS_thread_num; ++i) {
    tuple->req_type[i] = 1;
    tuple->req_type[i] = 0;
  }
}

/**
 * @brief function about abort.
 * Clean-up local read/write set.
 * Release locks.
 * @return void
 */
void TxExecutor::abort()
{
  /**
   * Release locks
   */
  unlockList(true);

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
  ++sres_->local_abort_counts_;

#if BACK_OFF
#if ADD_ANALYSIS
  uint64_t start(rdtscp());
#endif

  Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
  sres_->local_backoff_latency_ += rdtscp() - start;
#endif
#endif
}

/**
 * @brief success termination of transaction.
 * @return void
 */
void TxExecutor::commit()
{

  /**
   * Release locks.
   */
  unlockList(false);
  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
#if FAIR == 1
  txid_ += FLAGS_thread_num;
#endif
}

/**
 * @brief Initialize function of transaction.
 * Allocate timestamp.
 * @return void
 */
void TxExecutor::begin() { this->status_ = TransactionStatus::inFlight; }

/**
 * @brief Transaction read function.
 * @param [in] key The key of key-value
 */
void TxExecutor::read(uint64_t key)
{
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif // ADD_ANALYSIS

  /**
   * read-own-writes or re-read from local read set.
   */
  if (searchWriteSet(key) || searchReadSet(key))
    goto FINISH_READ;

  /**
   * Search tuple from data structure.
   */
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif
  if (readlockAcquire(LockType::SH, key, tuple)) goto FINISH_READ;
  spinWait(key, tuple);

FINISH_READ:
#if ADD_ANALYSIS
  sres_->local_read_latency_ += rdtscp() - start;
#endif
  return;
}

/**
 * @brief transaction write operation
 * @param [in] key The key of key-value
 * @return void
 */
void TxExecutor::write(uint64_t key, bool should_retire)
{
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif

  // if it already wrote the key object once.
  if (searchWriteSet(key))
  {
    goto FINISH_WRITE;
  }
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif

  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    {
      if (lockUpgrade(key, tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(key, tuple))
        {
          read_set_.erase(rItr);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(key, tuple);
        }
      }
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  writelockAcquire(LockType::EX, key, tuple);
  if (spinWait(key, tuple))
  {
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(key, tuple);
  }

  /**
   * Register the contents to write lock list and write set.
   */

FINISH_WRITE:
#if ADD_ANALYSIS
  sres_->local_write_latency_ += rdtscp() - start;
#endif // ADD_ANALYSIS
  return;
}

/**
 * @brief transaction readWrite (RMW) operation
 */
void TxExecutor::readWrite(uint64_t key, bool should_retire)
{
  // if it already wrote the key object once.
  if (searchWriteSet(key))
  {
    goto FINISH_WRITE;
  }
#ifdef NOUPGRADE
  if (searchReadSet(key))
  {
    goto FINISH_WRITE;
  }
#endif
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  tuple = get_tuple(Table, key);
#endif
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    {
      if (lockUpgrade(key, tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(key, tuple))
        {
          read_set_.erase(rItr);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(key, tuple);
        }
      }
      goto FINISH_WRITE;
    }
  }
  /**
   * Search tuple from data structure.
   */
  writelockAcquire(LockType::EX, key, tuple);
  if (spinWait(key, tuple))
  {
    // read payload
    memcpy(this->return_val_, tuple->val_, VAL_SIZE);
    // finish read.
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(key, tuple);
  }

FINISH_WRITE:
  return;
}

/**
 * @brief unlock and clean-up local lock set.
 * @return void
 */
void TxExecutor::unlockList(bool is_abort)
{
  Tuple *tuple;
  bool shouldRollback;
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    LockRelease(is_abort, (*itr).key_, tuple);
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    if (tuple->req_type[thid_] == 0)
    {
      continue;
    }
    shouldRollback = LockRelease(is_abort, (*itr).key_, tuple);
    if (is_abort && shouldRollback)
      memcpy(tuple->val_, (*itr).val_, VAL_SIZE);
  }
}

bool TxExecutor::conflict(LockType x, LockType y)
{
  if ((x == LockType::EX) || (y == LockType::EX))
    return true;
  else
    return false;
}

void TxExecutor::addCommitSemaphore(int t, LockType t_type, Tuple *tuple)
{
  int r, rThread;
  LockType retired_type;
  int tThread = t % FLAGS_thread_num;
  for (int i = 0; i < tuple->retired.size(); i++)
  {
    r = tuple->retired[i];
    rThread = r % FLAGS_thread_num;
    retired_type = (LockType)tuple->req_type[rThread];
    if (t > r && conflict(t_type, retired_type))
    {
      __atomic_add_fetch(&commit_semaphore[tThread], 1, __ATOMIC_SEQ_CST);
      break;
    }
  }
}

void TxExecutor::PromoteWaiters(Tuple *tuple)
{
  int t, tThread;
  int owner, ownerThread;
  LockType t_type;
  LockType owners_type;
  bool owner_exists = false;

  int r, rThread;
  LockType retired_type;

  while (tuple->waiters.size())
  {
    if (tuple->owners.size())
    {
      owner = tuple->owners[0];
      ownerThread = owner % FLAGS_thread_num;
      owners_type = (LockType)tuple->req_type[ownerThread];
      owner_exists = true;
    }
    t = tuple->waiters[0];
    tThread = t % FLAGS_thread_num;
    t_type = (LockType)tuple->req_type[tThread];
    if (owner_exists && conflict(t_type, owners_type))
    {
      break;
    }
    tuple->remove(t, tuple->waiters);
    tuple->ownersAdd(t);
    addCommitSemaphore(t, t_type, tuple);
  }
}

void TxExecutor::checkWound(vector<int> &list, LockType lock_type, Tuple *tuple, uint64_t key)
{
  int t, tThread;
  bool has_conflicts = false;
  LockType type;
  for (auto it = list.begin(); it != list.end();)
  {
    t = (*it);
    tThread = t % FLAGS_thread_num;
    type = (LockType)tuple->req_type[tThread];
    has_conflicts = false;
    if (txid_ != t && conflict(lock_type, type))
    {
      has_conflicts = true;
    }
    if (has_conflicts == true && txid_ < t)
    {
      TxPointers[tThread]->status_ = TransactionStatus::aborted;
      it = woundRelease(t, tuple, key);
    }
    else
    {
      ++it;
    }
  }
}

void TxExecutor::writelockAcquire(LockType EX_lock, uint64_t key, Tuple *tuple)
{
  tuple->req_type[thid_] = EX_lock;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, EX_lock, tuple, key);
      checkWound(tuple->owners, EX_lock, tuple, key);
      if (tuple->owners.size() == 0 && 
      (tuple->waiters.size() == 0 || txid_ < tuple->waiters[0]))
      {
        tuple->ownersAdd(txid_);
        addCommitSemaphore(txid_, EX_lock, tuple);
      }
      else
      {
        tuple->sortAdd(txid_, tuple->waiters);
        PromoteWaiters(tuple);
      }
      tuple->lock_.w_unlock();
      return;
    }
    usleep(1);
  }
}

void TxExecutor::cascadeAbort(int txn, Tuple *tuple, uint64_t key)
{
  int t, tThread;
  concat(tuple->retired, tuple->owners);
  for (int i = 0; i < all_owners.size; i++)
  {
    if (txn == all_owners.arr[i])
    {
      for (int j = i + 1; j < all_owners.size; j++)
      {
        t = all_owners.arr[j];
        tThread = t % FLAGS_thread_num;
        TxPointers[tThread]->status_ = TransactionStatus::aborted;
        if (tuple->remove(t, tuple->retired) == false &&
            tuple->ownersRemove(t) == false)
        {
          printf("REMOVE FAILURE: tx%d cascade abort tx%d\n", txn, t);
          exit(1);
        }
        tuple->req_type[tThread] = 0;
      }
      return;
    }
  }
}

void TxExecutor::concat(vector<int> &r, vector<int> &o)
{
  for (int i = 0; i < all_owners.size; i++) all_owners.arr[i] = 0;

  int i, j;
  for (i = 0; i < r.size(); i++) {
    all_owners.arr[i] = r[i];
  }
  for (j = 0; j < o.size(); j++, i++) {
    all_owners.arr[i] = o[j];
  }
  all_owners.size = i;
}

vector<int>::iterator TxExecutor::woundRelease(int txn, Tuple *tuple, uint64_t key)
{
  bool was_head = false;
  int txnThread = txn % FLAGS_thread_num;
  LockType type = (LockType)tuple->req_type[txnThread];
  int head, headThread;
  LockType head_type;
  
  if (tuple->retired.size() && tuple->retired[0] == txn)
  {
    was_head = true;
  }
  if (type == LockType::EX)
  {
    cascadeAbort(txn, tuple, key);
    TxExecutor *trans = TxPointers[txnThread];
    for (auto itr = trans->write_set_.begin(); itr != trans->write_set_.end(); ++itr)
    {
      if ((*itr).key_ == key) {
        memcpy(tuple->val_, (*itr).val_, VAL_SIZE);
        break;
      }
    }
  }
  auto it = tuple->itrRemove(txn);
  concat(tuple->retired, tuple->owners);
  if (all_owners.size)
  {
    head = all_owners.arr[0];
    headThread = head % FLAGS_thread_num;
    head_type = (LockType)tuple->req_type[headThread];
    if (was_head && conflict(type, head_type))
    {
      for (int i = 0; i < all_owners.size; i++)
      {
        __atomic_add_fetch(&commit_semaphore[all_owners.arr[i] % FLAGS_thread_num], -1, __ATOMIC_SEQ_CST);
        if ((i + 1) < all_owners.size &&
            conflict((LockType)tuple->req_type[all_owners.arr[i] % FLAGS_thread_num], (LockType)tuple->req_type[all_owners.arr[i + 1] % FLAGS_thread_num])) // CAUTION: may be wrong
          break;
      }
    }
  }
  tuple->req_type[txnThread] = 0;
  return it;
}

bool TxExecutor::LockRelease(bool is_abort, uint64_t key, Tuple *tuple)
{
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[thid_];
  int head, headThread;
  LockType head_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      if (tuple->req_type[thid_] == 0)
      {
        tuple->lock_.w_unlock();
        return false;
      }
      if (tuple->retired.size() > 0 && tuple->retired[0] == txid_)
      {
        was_head = true;
      }
      if (is_abort && type == LockType::EX)
      {
        cascadeAbort(txid_, tuple, key); // lock is released here
      }
      if (tuple->remove(txid_, tuple->retired) == false &&
          tuple->ownersRemove(txid_) == false)
      {
        printf("REMOVE FAILURE: LockRelease tx%d\n", txid_);
        exit(1);
      }
      concat(tuple->retired, tuple->owners);
      if (all_owners.size)
      {
        head = all_owners.arr[0];
        headThread = head % FLAGS_thread_num;
        head_type = (LockType)tuple->req_type[headThread];
        if (was_head && conflict(type, head_type))
        {
          for (int i = 0; i < all_owners.size; i++)
          {
            __atomic_add_fetch(&commit_semaphore[all_owners.arr[i] % FLAGS_thread_num], -1, __ATOMIC_SEQ_CST);
            if ((i + 1) < all_owners.size &&
                conflict((LockType)tuple->req_type[all_owners.arr[i] % FLAGS_thread_num], (LockType)tuple->req_type[all_owners.arr[i + 1] % FLAGS_thread_num])) // CAUTION: may be wrong
              break;
          }
        }
      }
      tuple->req_type[thid_] = 0;
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return true;
    }
    if (tuple->req_type[thid_] == 0)
      return false;
    usleep(1);

  }
}

void TxExecutor::LockRetire(uint64_t key, Tuple *tuple)
{
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      if (tuple->req_type[thid_] == 0)
      {
        tuple->lock_.w_unlock();
        return;
      }
      tuple->ownersRemove(txid_);
      tuple->sortAdd(txid_, tuple->retired);
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    if (tuple->req_type[thid_] == 0)
      return;
    usleep(1);
  }
}

vector<int>::iterator Tuple::itrRemove(int txn)
{
  vector<int>::iterator it;
  int i;
  if (retired.size() > 0) {
    i = myBinarySearch(retired, txn, retired.size());
    if (i != -1) {
      assert(txn == *(list.begin() + i));
      it = retired.erase(retired.begin() + i);
      return it;
    }
  }
  for (i = 0; i < owners.size(); i++)
  {
    if (txn == owners[i])
    {
      it = owners.erase(owners.begin() + i);
      return it;
    }
  }
  printf("ERROR: itrRemove FAILURE\n");
  exit(1);
}

bool Tuple::ownersRemove(int txn)
{
  for (int i = 0; i < owners.size(); i++)
  {
    if (txn == owners[i])
    {
      owners.erase(owners.begin() + i);
      return true;
    }
  }
  return false;
}

bool Tuple::remove(int txn, vector<int> &list)
{
  if (list.size() == 0)
    return false;
  int i = myBinarySearch(list, txn, list.size());
  if (i == -1)
    return false;
  assert(txn == *(list.begin() + i));
  list.erase(list.begin() + i);
  return true;
}

bool Tuple::sortAdd(int txn, vector<int> &list)
{
  if (list.size() == 0)
  {
    list.emplace_back(txn);
    return true;
  }
  int i = myBinaryInsert(list, txn, list.size());
  list.insert(list.begin() + i, txn);
  return true;
}

bool TxExecutor::spinWait(uint64_t key, Tuple *tuple)
{
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      for (int i = 0; i < tuple->owners.size(); i++)
      {
        if (txid_ == tuple->owners[i])
        {
#ifdef OPT1
          // optimization 1: read lock retire without latch
          if (tuple->req_type[thid_] == -1)
          {
            read_set_.emplace_back(key, tuple, tuple->val_);
            tuple->ownersRemove(txid_);
            tuple->sortAdd(txid_, tuple->retired);
            PromoteWaiters(tuple);
          }
#endif
          else if (tuple->req_type[thid_] == 1) 
          {
            write_set_.emplace_back(key, tuple, tuple->val_);
          }
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (status_ == TransactionStatus::aborted)
      {
        eraseFromLists(tuple);
        PromoteWaiters(tuple);
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}

void TxExecutor::eraseFromLists(Tuple *tuple)
{
  tuple->req_type[thid_] = 0;
  if (tuple->remove(txid_, tuple->waiters)) return;
  tuple->ownersRemove(txid_);
}

bool TxExecutor::lockUpgrade(uint64_t key, Tuple *tuple)
{
  bool is_retired;
  const LockType my_type = LockType::SH;
  int i;

  int r, rThread;
  LockType retired_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, LockType::EX, tuple, key);
      checkWound(tuple->owners, LockType::EX, tuple, key);
      is_retired = false;
      for (i = 0; i < tuple->retired.size(); i++)
      {
        if (txid_ == tuple->retired[i])
        {
          is_retired = true;
          break;
        }
      }
      if (is_retired)
      {
        if (tuple->owners.size() == 0)
        {
          if (i > 0)
          {
            for (int j = 0; j < i; j++)
            {
              r = tuple->retired[j];
              rThread = r % FLAGS_thread_num;
              retired_type = (LockType)tuple->req_type[rThread];
              if (txid_ > r && conflict(my_type, retired_type))
              {
                break;
              }
              if (j + 1 == i)
              {
                __atomic_add_fetch(&commit_semaphore[thid_], 1, __ATOMIC_SEQ_CST);
              }
            }
          }
          tuple->remove(txid_, tuple->retired);
          tuple->ownersAdd(txid_);
          tuple->req_type[thid_] = LockType::EX;
          tuple->lock_.w_unlock();
          return true;
        }
      }
      else
      {
        if (tuple->owners.size() == 1 && tuple->owners[0] == txid_)
        {
          tuple->req_type[thid_] = LockType::EX;
          for (int i = 0; i < tuple->retired.size(); i++)
          {
            r = tuple->retired[i];
            rThread = r % FLAGS_thread_num;
            retired_type = (LockType)tuple->req_type[rThread];
            if (txid_ > r && retired_type == LockType::SH)
            {
              __atomic_add_fetch(&commit_semaphore[thid_], 1, __ATOMIC_SEQ_CST);
              break;
            }
          }
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (status_ == TransactionStatus::aborted)
      {
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
      usleep(1);
    }
    usleep(1);

  }
}

bool TxExecutor::readlockAcquire(LockType SH_lock, uint64_t key, Tuple *tuple)
{
  tuple->req_type[thid_] = SH_lock;
  bool is_retired = false;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, SH_lock, tuple, key);
      checkWound(tuple->owners, SH_lock, tuple, key);
      if (tuple->owners.size() == 0 && 
      (tuple->waiters.size() == 0 || txid_ < tuple->waiters[0]))
      {
        read_set_.emplace_back(key, tuple, tuple->val_);
        tuple->sortAdd(txid_, tuple->retired);
        addCommitSemaphore(txid_, SH_lock, tuple);
        is_retired = true;
      }
      else
      {
        tuple->sortAdd(txid_, tuple->waiters);
        PromoteWaiters(tuple);
      }
      tuple->lock_.w_unlock();
      return is_retired;
    }
    usleep(1);
  }
}
#endif