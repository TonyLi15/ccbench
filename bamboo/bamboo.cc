
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

#define BAMBOO
// #define NONTS
// #define OPT1

using namespace std;

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

void TxExecutor::checkLists(uint64_t key)
{
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#else
  tuple = get_tuple(Table, key);
#endif
  printf("tx%d checks tuple %d lists\n", thid_, (int)key);
  for (int i = 0; i < tuple->retired.size(); i++)
  {
    printf("tuple %d retired[%d] is tx%d ts %d locktype = %d abort %d sema %d\n", (int)key, i, tuple->retired[i], thread_timestamp[tuple->retired[i]], tuple->req_type[tuple->retired[i]], thread_stats[tuple->retired[i]], commit_semaphore[tuple->retired[i]]);
  }
  for (int i = 0; i < tuple->owners.size(); i++)
  {
    printf("tuple %d owners[%d] is tx%d ts %d locktype = %d abort %d sema %d\n", (int)key, i, tuple->owners[i], thread_timestamp[tuple->owners[i]], tuple->req_type[tuple->owners[i]], thread_stats[tuple->owners[i]], commit_semaphore[tuple->owners[i]]);
  }
  for (int i = 0; i < tuple->waiters.size(); i++)
  {
    printf("tuple %d waiters[%d] is tx%d ts %d locktype = %d abort %d\n", (int)key, i, tuple->waiters[i], thread_timestamp[tuple->waiters[i]], tuple->req_type[tuple->waiters[i]], thread_stats[tuple->waiters[i]]);
  }
  for (int i = 0; i < FLAGS_thread_num; i++)
  {
    printf("tx%d commit semaphore %d\n", i, commit_semaphore[i]);
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
  usleep(1);
}

/**
 * @brief success termination of transaction.
 * @return void
 */
void TxExecutor::commit()
{
#ifndef BAMBOO
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    /**
     * update payload.
     */
    memcpy((*itr).rcdptr_->val_, write_val_, VAL_SIZE);
  }
#endif

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
  LockAcquire(tuple, LockType::SH, key);
  if (spinWait(tuple, key))
  {
    read_set_.emplace_back(key, tuple, tuple->val_);
#ifdef BAMBOO
    LockRetire(tuple, key);
#endif
  }

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
#ifdef BAMBOO
          memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(tuple, key);
#endif
        }
      }
      goto FINISH_WRITE;
    }
  }

  LockAcquire(tuple, LockType::EX, key);
  if (spinWait(tuple, key))
  {
    write_set_.emplace_back(key, tuple);
#ifdef BAMBOO
    memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(tuple, key);
#endif
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
#ifdef BAMBOO
          memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
          memcpy(tuple->val_, write_val_, VAL_SIZE);
          if (should_retire)
            LockRetire(tuple, key);
#endif
        }
      }
      goto FINISH_WRITE;
    }
  }

  LockAcquire(tuple, LockType::EX, key);
  if (spinWait(tuple, key))
  {
    // read payload
    memcpy(this->return_val_, tuple->val_, VAL_SIZE);
    // finish read.
    write_set_.emplace_back(key, tuple);
#ifdef BAMBOO
    memcpy(tuple->prev_val_[thid_], tuple->val_, VAL_SIZE);
    memcpy(tuple->val_, write_val_, VAL_SIZE);
    if (should_retire)
      LockRetire(tuple, key);
#endif
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
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    LockRelease(tuple, is_abort, (*itr).key_);
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    tuple = (*itr).rcdptr_;
    LockRelease(tuple, is_abort, (*itr).key_);
#ifdef BAMBOO
    if (is_abort)
      memcpy(tuple->val_, tuple->prev_val_[thid_], VAL_SIZE);
#endif
  }
}

bool TxExecutor::conflict(LockType x, LockType y)
{
  if ((x == LockType::EX) || (y == LockType::EX))
    return true;
  else
    return false;
}

void TxExecutor::PromoteWaiters(Tuple *tuple)
{
  int t;
  int owner;
  LockType t_type;
  LockType owners_type;
  bool owner_exists = false;

#ifdef BAMBOO
  int r;
  LockType retired_type;
#endif

  while (tuple->waiters.size() > 0)
  {
    if (tuple->owners.size() > 0)
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
#ifdef BAMBOO
    for (int i = 0; i < tuple->retired.size(); i++)
    {
      r = tuple->retired[i];
      retired_type = (LockType)tuple->req_type[r];
#ifndef NONTS
      if (thread_timestamp[t] > thread_timestamp[r] &&
#else
      if (t > r &&
#endif
          conflict(t_type, retired_type))
      {
        __atomic_add_fetch(&commit_semaphore[t], 1, __ATOMIC_SEQ_CST);
        break;
      }
    }
#endif
  }
}

void TxExecutor::checkWound(vector<int> &list, LockType lock_type, Tuple *tuple, uint64_t key)
{
  int t;
  bool has_conflicts = false;
  LockType type;
  int i = 0;
  while (list.size() > 0)
  {
    t = list[i];
    type = (LockType)tuple->req_type[t];
    if (conflict(lock_type, type))
    {
      has_conflicts = true;
    }
    else
    {
      has_conflicts = false;
    }
#ifndef NONTS
    if (has_conflicts == true && thread_timestamp[thid_] < thread_timestamp[t])
#else
    if (has_conflicts == true && thid_ < t)
#endif
    {
      thread_stats[t] = 1;
      woundRelease(t, tuple, key);
      i = 0;
      continue;
    }
    i++;
    if (i >= list.size())
      return;
  }
}

void TxExecutor::LockAcquire(Tuple *tuple, LockType lock_type, uint64_t key)
{
  tuple->req_type[thid_] = lock_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
#ifdef BAMBOO
      checkWound(tuple->retired, lock_type, tuple, key);
#endif
      checkWound(tuple->owners, lock_type, tuple, key);
      tuple->sortAdd(thid_, tuple->waiters);
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    else
    {
      usleep(1);
    }
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
            tuple->remove(t, tuple->owners) == false)
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
#ifdef BAMBOO
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[txn];
  int head;
  LockType head_type;
#endif
#ifdef BAMBOO
  auto all_owners = concat(tuple->retired, tuple->owners);
  if (tuple->retired.size() > 0 && tuple->retired[0] == txn)
  {
    was_head = true;
  }
  if (type == LockType::EX)
  {
    cascadeAbort(txn, all_owners, tuple, key);
    memcpy(tuple->val_, tuple->prev_val_[txn], VAL_SIZE);
  }
  if (tuple->remove(txn, tuple->retired) == false &&
      tuple->remove(txn, tuple->owners) == false)
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
            conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]]))
          break;
      }
    }
  }
#endif
#ifndef BAMBOO
  tuple->remove(txn, tuple->owners);
#endif
  tuple->req_type[txn] = 0;
}

void TxExecutor::LockRelease(Tuple *tuple, bool is_abort, uint64_t key)
{
#ifdef BAMBOO
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[thid_];
  int head;
  LockType head_type;
#endif
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      if (tuple->req_type[thid_] == 0)
      {
        tuple->lock_.w_unlock();
        return;
      }
#ifdef BAMBOO
      auto all_owners = concat(tuple->retired, tuple->owners);
      if (tuple->retired.size() > 0 && tuple->retired[0] == thid_)
      {
        was_head = true;
      }
      if (is_abort && type == LockType::EX)
      {
        cascadeAbort(thid_, all_owners, tuple, key);
      }
      if (tuple->remove(thid_, tuple->retired) == false &&
          tuple->remove(thid_, tuple->owners) == false)
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
                conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]]))
              break;
          }
        }
      }
#endif
#ifndef BAMBOO
      tuple->remove(thid_, tuple->owners);
#endif
      tuple->req_type[thid_] = 0;
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    else
    {
      if (tuple->req_type[thid_] == 0)
        return;
      usleep(1);
    }
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
      tuple->remove(thid_, tuple->owners);
      tuple->sortAdd(thid_, tuple->retired);
      PromoteWaiters(tuple);
      tuple->lock_.w_unlock();
      return;
    }
    else
    {
      if (tuple->req_type[thid_] == 0)
        return;
      usleep(1);
    }
  }
}

bool Tuple::remove(int txn, vector<int> &list)
{
  for (int i = 0; i < list.size(); i++)
  {
    if (txn == list[i])
    {
      list.erase(list.begin() + i);
      return true;
    }
  }
  return false;
}

bool Tuple::sortAdd(int txn, vector<int> &list)
{
  for (auto tid = list.begin(); tid != list.end(); tid++)
  { // reverse_iterator might be better
#ifndef NONTS
    if (thread_timestamp[txn] < thread_timestamp[(*tid)])
#else
    if (txn < (*tid))
#endif
    {
      list.insert(tid, txn);
      return true;
    }
  }
  list.push_back(txn);
  return true;
  //   for (auto tid = list.rbegin(); tid != list.rend(); tid++)
  //   { // reverse_iterator
  // #ifndef NONTS
  //     if (thread_timestamp[txn] > thread_timestamp[(*tid)])
  // #else
  //     if (txn > (*tid))
  // #endif
  //     {
  //       list.insert(tid.base(), txn);
  //       break;
  //     }
  //   }
  //   list.insert(list.begin(), txn);
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
          tuple->lock_.w_unlock();
          return true;
        }
      }
      if (thread_stats[thid_] == 1)
      {
        eraseFromLists(tuple);
        PromoteWaiters(tuple);
        status_ = TransactionStatus::aborted;
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
      usleep(1);
    }
    else
    {
      usleep(1);
    }
  }
}

void TxExecutor::eraseFromLists(Tuple *tuple)
{
  for (int i = 0; i < tuple->waiters.size(); i++)
  {
    if (thid_ == tuple->waiters[i])
    {
      tuple->waiters.erase(tuple->waiters.begin() + i);
      tuple->req_type[thid_] = 0;
    }
  }
  for (int i = 0; i < tuple->owners.size(); i++)
  {
    if (thid_ == tuple->owners[i])
    {
      tuple->owners.erase(tuple->owners.begin() + i);
      tuple->req_type[thid_] = 0;
    }
  }
}

bool TxExecutor::lockUpgrade(Tuple *tuple, uint64_t key)
{
  bool is_retired = false;
  const LockType my_type = LockType::SH;
  int i;

  int r;
  LockType retired_type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      is_retired = false;
#ifdef BAMBOO
      checkWound(tuple->retired, LockType::EX, tuple, key);
#endif
      checkWound(tuple->owners, LockType::EX, tuple, key);
#ifdef BAMBOO
      for (i = 0; i < tuple->retired.size(); i++)
      {
        if (thid_ == tuple->retired[i])
        {
          is_retired = true;
          break;
        }
      }
#endif
#ifdef BAMBOO
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
#ifndef NONTS
              if (thread_timestamp[thid_] > thread_timestamp[r] &&
#else
              if (thid_ > r &&
#endif
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
          if (tuple->remove(thid_, tuple->retired) == false)
          {
            exit(1);
          }
          tuple->ownersAdd(thid_);
          tuple->req_type[thid_] = LockType::EX;
          tuple->lock_.w_unlock();
          return true;
        }
      }
      else
      {
#endif
        if (tuple->owners.size() == 1 && tuple->owners[0] == thid_)
        {
          tuple->req_type[thid_] = LockType::EX;
#ifdef BAMBOO
          for (int i = 0; i < tuple->retired.size(); i++)
          {
            r = tuple->retired[i];
            retired_type = (LockType)tuple->req_type[r];
#ifndef NONTS
            if (thread_timestamp[thid_] > thread_timestamp[r] &&
#else
            if (thid_ > r &&
#endif
                retired_type == LockType::SH)
            {
              __atomic_add_fetch(&commit_semaphore[thid_], 1, __ATOMIC_SEQ_CST);
              break;
            }
          }
#endif
          tuple->lock_.w_unlock();
          return true;
        }
#ifdef BAMBOO
      }
#endif
      if (thread_stats[thid_] == 1)
      {
        status_ = TransactionStatus::aborted;
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
      usleep(1);
    }
    else
    {
      usleep(1);
    }
  }
}