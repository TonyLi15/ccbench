
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

// #define PRINTF
#define OPT1

using namespace std;

int myBinarySearch(vector<int> &x, int goal, int tail)
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
  // usleep(1);
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
  if (readAcquire(tuple, LockType::SH, key)) goto FINISH_READ;
  readWait(tuple, key);

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
      if (lockUpgrade(tuple, key))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(tuple, key))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          read_set_.erase(rItr);
          memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(tuple, key);
        }
      }
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  LockAcquire(tuple, LockType::EX, key);
  if (spinWait(tuple, key))
  {
    write_set_.emplace_back(key, tuple);
    memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(tuple, key);
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
      if (lockUpgrade(tuple, key))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(tuple, key))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          read_set_.erase(rItr);
          memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(tuple, key);
        }
      }
      goto FINISH_WRITE;
    }
  }
  /**
   * Search tuple from data structure.
   */
  LockAcquire(tuple, LockType::EX, key);
  if (spinWait(tuple, key))
  {
    // read payload
    memcpy(this->return_val_, tuple->val_, VAL_SIZE);
    // finish read.
    write_set_.emplace_back(key, tuple);
    memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(tuple, key);
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
    LockRelease(tuple, is_abort, (*itr).key_);
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    if (tuple->req_type[thid_] == 0)
    {
      continue;
    }
    shouldRollback = LockRelease(tuple, is_abort, (*itr).key_);
    if (is_abort && shouldRollback)
      memcpy(tuple->val_, tuple->prev_val_[thid_], VAL_SIZE);
  }
}

bool TxExecutor::conflict(LockType x, LockType y)
{
  if ((x == LockType::EX) || (y == LockType::EX))
    return true;
  else
    return false;
}

void TxExecutor::addCommitSemaphore(Tuple *tuple, int t, LockType t_type)
{
  int r;
  LockType retired_type;
  for (int i = 0; i < tuple->retired.size(); i++)
  {
    r = tuple->retired[i];
    retired_type = (LockType)tuple->req_type[r];
    if (thread_timestamp[t] > thread_timestamp[r] &&
        conflict(t_type, retired_type))
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
    if (t_type == LockType::SH) {
      tuple->sortAdd(t, tuple->retired);
      // adjustFollowingSemaphore(tuple);
    }
    else {tuple->ownersAdd(t);}
    addCommitSemaphore(tuple, t, t_type);
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
    if (conflict(lock_type, type))
    {
      has_conflicts = true;
    }
    if (thid_ != t && has_conflicts == true && thread_timestamp[thid_] < thread_timestamp[t])
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

void TxExecutor::LockAcquire(Tuple *tuple, LockType lock_type, uint64_t key)
{
  int owner;
  LockType owners_type;
  bool owner_exists;
  tuple->req_type[thid_] = lock_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, lock_type, tuple, key);
      checkWound(tuple->owners, lock_type, tuple, key);
      owner_exists = false;
      if (tuple->owners.size())
      {
        owner = tuple->owners[0];
        owners_type = (LockType)tuple->req_type[owner];
        owner_exists = true;
      }
      if (tuple->waiters.size() == 0 &&
          (owner_exists == false || conflict(lock_type, owners_type) == false))
      {
        tuple->ownersAdd(thid_);
        addCommitSemaphore(tuple, thid_, lock_type);
      }
      else
      {
        tuple->sortAdd(thid_, tuple->waiters);
      }
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    usleep(1);
  }
}

void TxExecutor::cascadeAbort(int txn, vector<int> all_owners, Tuple *tuple, uint64_t key)
{
  int t;
  for (int i = 0; i < all_owners.size(); i++)
  {
    if (txn == all_owners[i])
    {
      for (int j = i + 1; j < all_owners.size(); j++)
      {
        t = all_owners[j];
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

vector<int> concat(vector<int> r, vector<int> o)
{
  auto c = r;
  c.insert(c.end(), o.begin(), o.end());
  return c;
}

vector<int>::iterator TxExecutor::woundRelease(int txn, Tuple *tuple, uint64_t key)
{
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[txn];
  int head;
  LockType head_type;
  auto all_owners = concat(tuple->retired, tuple->owners);

  if (tuple->retired.size() && tuple->retired[0] == txn)
  {
    was_head = true;
  }
  if (type == LockType::EX)
  {
    cascadeAbort(txn, all_owners, tuple, key);
    memcpy(tuple->val_, tuple->prev_val_[txn], VAL_SIZE);
  }
  auto it = tuple->itrRemove(txn);
  all_owners = concat(tuple->retired, tuple->owners);
  if (all_owners.size())
  {
    head = all_owners[0];
    head_type = (LockType)tuple->req_type[head];
    if (was_head && conflict(type, head_type))
    {
      for (int i = 0; i < all_owners.size(); i++)
      {
        __atomic_add_fetch(&commit_semaphore[all_owners[i]], -1, __ATOMIC_SEQ_CST);
        if ((i + 1) < all_owners.size() &&
            conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]])) // CAUTION: may be wrong
          break;
      }
    }
  }
  tuple->req_type[txn] = 0;
  return it;
}

bool TxExecutor::LockRelease(Tuple *tuple, bool is_abort, uint64_t key)
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
      auto all_owners = concat(tuple->retired, tuple->owners);
      was_head = false;
      if (tuple->retired.size() > 0 && tuple->retired[0] == thid_)
      {
        was_head = true;
      }
      if (is_abort && type == LockType::EX)
      {
        cascadeAbort(thid_, all_owners, tuple, key);
      }
      if (tuple->remove(thid_, tuple->retired) == false &&
          tuple->ownersRemove(thid_) == false)
      {
        exit(1);
      }
      all_owners = concat(tuple->retired, tuple->owners);
      if (all_owners.size())
      {
        head = all_owners[0];
        head_type = (LockType)tuple->req_type[head];
        if (was_head && conflict(type, head_type))
        {
          for (int i = 0; i < all_owners.size(); i++)
          {
            __atomic_add_fetch(&commit_semaphore[all_owners[i]], -1, __ATOMIC_SEQ_CST);
            if ((i + 1) < all_owners.size() &&
                conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]])) // CAUTION: may be wrong
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

void TxExecutor::LockRetire(Tuple *tuple, uint64_t key)
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
    list.push_back(txn);
    return true;
  }
  int i = myBinaryInsert(list, txn, list.size());
  list.insert(list.begin() + i, txn);
  return true;
}

bool TxExecutor::spinWait(Tuple *tuple, uint64_t key)
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
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (thread_stats[thid_] == 1)
      {
        eraseFromLists(tuple);
        PromoteWaiters(tuple);
        status_ = TransactionStatus::aborted; //*** added by tatsu
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

bool TxExecutor::lockUpgrade(Tuple *tuple, uint64_t key)
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
        status_ = TransactionStatus::aborted; //*** added by tatsu
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}

bool TxExecutor::readAcquire(Tuple *tuple, LockType lock_type, uint64_t key)
{
  int owner;
  LockType owners_type;
  bool owner_exists;
  tuple->req_type[thid_] = lock_type;
  bool is_retired = false;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      checkWound(tuple->retired, lock_type, tuple, key);
      checkWound(tuple->owners, lock_type, tuple, key);
      owner_exists = false;
      if (tuple->owners.size() > 0)
      {
        owner = tuple->owners[0];
        owners_type = (LockType)tuple->req_type[owner];
        owner_exists = true;
      }
      if (tuple->waiters.size() == 0 && owner_exists == false)
      {
        read_set_.emplace_back(key, tuple, tuple->val_);
        tuple->sortAdd(thid_, tuple->retired);
        addCommitSemaphore(tuple, thid_, lock_type);
        // adjustFollowingSemaphore(tuple);
        is_retired = true;
      }
      else
      {
        tuple->sortAdd(thid_, tuple->waiters);
      }
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return is_retired;
    }
    usleep(1);
  }
}

void TxExecutor::adjustFollowingSemaphore(Tuple *tuple, int txn) {
  LockType t_type = LockType::SH;
  int follower;
  LockType f_type;

  if (txn == tuple->retired[0]) {
    for (int i = 1; i < tuple->retired.size(); i++) {
      follower = tuple->retired[i];
      f_type = (LockType)tuple->req_type[follower];
      if (f_type == LockType::EX && pending_commit[follower] == 0) {
        __atomic_add_fetch(&commit_semaphore[follower], 1, __ATOMIC_SEQ_CST);
        return;
      }
    }
    if (tuple->owners.size()) {
      follower = tuple->owners[0];
      f_type = (LockType)tuple->req_type[follower];
      if (f_type == LockType::EX && pending_commit[follower] == 0) {
        __atomic_add_fetch(&commit_semaphore[follower], 1, __ATOMIC_SEQ_CST);
        return;
      }
    }
  }
}

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
        status_ = TransactionStatus::aborted; //*** added by tatsu
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
    }
    usleep(1);
  }
}