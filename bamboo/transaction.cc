
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
// #define BAMBOO
#define NONTS
// #define OPT1
// #define NOUPGRADE

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
  for (int i = 0; i < tuple->retired.size(); i++)
  {
    printf("tuple %d retired[%d] is tx%d ts %d locktype = %d abort %d\n", (int)key, i, tuple->retired[i], thread_timestamp[tuple->retired[i]], tuple->req_type[tuple->retired[i]], thread_stats[tuple->retired[i]]);
  }
  for (int i = 0; i < tuple->owners.size(); i++)
  {
    printf("tuple %d owners[%d] is tx%d ts %d locktype = %d abort %d\n", (int)key, i, tuple->owners[i], thread_timestamp[tuple->owners[i]], tuple->req_type[tuple->owners[i]], thread_stats[tuple->owners[i]]);
  }
  for (int i = 0; i < tuple->waiters.size(); i++)
  {
    printf("tuple %d waiters[%d] is tx%d ts %d locktype = %d abort %d\n", (int)key, i, tuple->waiters[i], thread_timestamp[tuple->waiters[i]], tuple->req_type[tuple->waiters[i]], thread_stats[tuple->waiters[i]]);
  }
  for (int i = 0; i < FLAGS_thread_num; i++)
    printf("tx%d commit semaphore %d\n", i, commit_semaphore[i]);
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
  // #ifdef PRINTF
  // printf("tx%d abort ts %d\n", thid_, thread_timestamp[thid_]);
  // #endif
  unlockList(true);

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
#ifdef PRINTF
  printf("tx%d ts %d abort complete\n", thid_, thread_timestamp[thid_]);
#endif
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
  //printf("tx%d commit\n", thid_);
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    /**
     * update payload.
     */
    memcpy((*itr).rcdptr_->val_, write_val_, VAL_SIZE);
  }

  /**
   * Release locks.
   */
  unlockList(false);

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
#ifdef PRINTF
  printf("tx%d ts %d commit complete\n", thid_, thread_timestamp[thid_]);
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
  LockAcquire(tuple, LockType::SH);
  if (spinWait(tuple))
  {
#ifdef PRINTF
    printf("tx%d read lock acquired\n", thid_);
#endif
    read_set_.emplace_back(key, tuple, tuple->val_);
#ifdef BAMBOO
    LockRetire(tuple);
#endif
  }

FINISH_READ:
#ifdef PRINTF
  checkLists(key);
#endif
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

#ifndef NOUPGRADE
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    {
      if (lockUpgrade(tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(tuple))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          read_set_.erase(rItr);
#ifdef BAMBOO
          if (should_retire)
            LockRetire(tuple);
#endif
        }
      }
      // printf("tx%d goes to finish_write\n", thid_);
      goto FINISH_WRITE;
    }
  }
#endif

  /**
   * Search tuple from data structure.
   */
  //Tuple *tuple;
#if MASSTREE_USE
  // tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  //tuple = get_tuple(Table, key);
#endif
  LockAcquire(tuple, LockType::EX);
  if (spinWait(tuple))
  {
#ifdef PRINTF
    printf("tx%d write lock acquired\n", thid_);
#endif
    write_set_.emplace_back(key, tuple);
#ifdef BAMBOO
    if (should_retire)
      LockRetire(tuple);
#endif
  }

  /**
   * Register the contents to write lock list and write set.
   */

FINISH_WRITE:
#ifdef PRINTF
  checkLists(key);
#endif
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
      if (lockUpgrade(tuple))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinWait(tuple))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          read_set_.erase(rItr);
#ifdef BAMBOO
          if (should_retire)
            LockRetire(tuple);
#endif
        }
      }
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  LockAcquire(tuple, LockType::EX);
  if (spinWait(tuple))
  {
  // read payload
  memcpy(this->return_val_, tuple->val_, VAL_SIZE);
  // finish read.
    write_set_.emplace_back(key, tuple);
#ifdef BAMBOO
    if (should_retire)
      LockRetire(tuple);
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
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    LockRelease((*itr).rcdptr_, is_abort);
    (*itr).rcdptr_->req_type[this->thid_] = 0;
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    LockRelease((*itr).rcdptr_, is_abort);
    (*itr).rcdptr_->req_type[this->thid_] = 0;
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
#ifdef PRINTF
  // printf("tx %d tuple %d promotewaiters\n", thid_, (int)key);
#endif
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
#ifdef BAMBOO
    if (woundSuccess(tuple, t, t_type) == false)
    {
      break;
    }
#endif
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
#ifdef PRINTF
        printf("tx%d lock type %d commit semaphore %d\n", t, t_type, commit_semaphore[t]);
        // checkLists(key);
#endif
        break;
      }
    }
#endif
  }
}

template <typename T>
void TxExecutor::checkWound(T &list, LockType lock_type, Tuple *tuple)
{
  int t;
  bool has_conflicts = false;
  LockType type;
  for (int i = 0; i < list.size(); i++)
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
#ifdef PRINTF
      printf("tx%d wounds tx%d\n", thid_, t);
#endif
      thread_stats[t] = 1;
    }
  }
}

void TxExecutor::LockAcquire(Tuple *tuple, LockType lock_type)
{
#ifdef OPT1
  int owner;
  LockType owners_type;
  bool owner_exists = false;
#endif

  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
#ifdef PRINTF
      printf("tx%d lock acquire tuple %d, ts %d\n", thid_, (int)key, thread_timestamp[thid_]);
#endif
#ifdef BAMBOO
      checkWound(tuple->retired, lock_type, tuple);
#endif
      checkWound(tuple->owners, lock_type, tuple);
#ifdef OPT1
      if (tuple->owners.size() > 0)
      {
        owner = tuple->owners[0];
        owners_type = (LockType)tuple->req_type[owner];
        owner_exists = true;
      }
      if (tuple->waiters.size() == 0 &&
          (owner_exists == false || conflict(lock_type, owners_type) == false))
      {
        tuple->ownersAdd(thid_);
        for (int i = 0; i < tuple->retired.size(); i++)
        {
          if (thread_timestamp[thid_] > thread_timestamp[tuple->retired[i]] &&
              conflict(lock_type, (LockType)tuple->req_type[tuple->retired[i]]))
          {
            commit_semaphore[thid_]++;
#ifdef PRINTF
            printf("tx%d locktype %d, tx%d locktype %d\n", thid_, lock_type, tuple->retired[i], tuple->req_type[tuple->retired[i]]);
            printf("tx%d lock type %d commit semaphore %d\n", thid_, lock_type, commit_semaphore[thid_]);
            // checkLists(key);
#endif
            break;
          }
        }
      }
      else
      {
#ifdef PRINTF
        printf("tx%d added to waiter list\n", thid_);
#endif
        tuple->sortAdd(thid_, tuple->waiters);
      }
#endif
#ifndef OPT1
      tuple->sortAdd(thid_, tuple->waiters);
#endif
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

void cascadeAbort(int txn, vector<int> all_owners)
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
#ifdef PRINTF
        printf("tx%d cascade abort tx %d\n", txn, all_owners[j]);
#endif
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

void TxExecutor::LockRelease(Tuple *tuple, bool is_abort)
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
#ifdef PRINTF
      // printf("tx%d ts %d abort %d threadstats %d locktype %d LockRelease tuple %d\n", thid_, thread_timestamp[thid_], is_abort, thread_stats[thid_], (int)type, (int)key);
#endif
#ifdef BAMBOO
      auto all_owners = concat(tuple->retired, tuple->owners);
      if (tuple->retired.size() > 0 && tuple->retired[0] == thid_) // CAUTION
      {
        was_head = true;
      }
      if (is_abort && type == LockType::EX)
      {
        cascadeAbort(thid_, all_owners); // lock is released here
      }
      if (tuple->remove(thid_, tuple->retired) == false &&
          tuple->remove(thid_, tuple->owners) == false)
      {
        printf("REMOVE FAILURE: LockRelease tx%d\n", thid_);
        exit(1);
      }
#ifdef PRINTF
      printf("tx%d ts %d LockRelease tuple %d complete\n", thid_, thread_timestamp[thid_], (int)key);
      checkLists(key);
#endif
      all_owners = concat(tuple->retired, tuple->owners);
      if (all_owners.size())
      {
        head = all_owners[0];
        head_type = (LockType)tuple->req_type[head];
        if (was_head && conflict(type, head_type))
        {
          for (int i = 0; i < all_owners.size(); i++)
          {
#ifdef PRINTF
            printf("tx%d decrement tx%d commit semaphore from %d\n", thid_, all_owners[i], commit_semaphore[all_owners[i]]);
#endif
            __atomic_add_fetch(&commit_semaphore[all_owners[i]], -1, __ATOMIC_SEQ_CST);
#ifdef PRINTF
            printf("tx%d decrement tx%d commit semaphore to %d\n", thid_, all_owners[i], commit_semaphore[all_owners[i]]);
#endif
            if ((i + 1) < all_owners.size() &&
                conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]])) // CAUTION: may be wrong
              break;
          }
        }
      }
#endif
#ifndef BAMBOO
      tuple->remove(thid_, tuple->owners);
#endif
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

void TxExecutor::LockRetire(Tuple *tuple)
{
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
#ifdef PRINTF
      printf("tx%d lockRetire tuple %d\n", thid_, key);
#endif
      tuple->remove(thid_, tuple->owners);
      tuple->sortAdd(thid_, tuple->retired);
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

template <typename T>
bool Tuple::remove(int txn, T &list)
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

template <typename T>
bool Tuple::sortAdd(int txn, T &list)
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
}

bool TxExecutor::spinWait(Tuple *tuple)
{
#ifdef PRINTF
  // printf("tx%d ts %d getLock: tuple %d\n", thid_, thread_timestamp[thid_], (int)key);
#endif
  while (1)
  {
    for (int i = 0; i < tuple->owners.size(); i++)
    {
      if (thid_ == tuple->owners[i])
      {
#ifdef PRINTF
        printf("tx%d ts %d found tuple %d\n", thid_, thread_timestamp[thid_], (int)key);
#endif
        return true;
      }
    }
#ifdef PRINTF
    printf("tx%d ts %d not found: tuple is %d\n", thid_, thread_timestamp[thid_], (int)key);
    checkLists(key);
#endif
    if (tuple->lock_.w_trylock())
    {
      // not sure if these two lines are following the protocol
      // checkWound(tuple->owners, (LockType)tuple->req_type[thid_], tuple);
      // PromoteWaiters(key);
      if (thread_stats[thid_] == 1)
      {
#ifdef PRINTF
        printf("tx%d ts %d should abort in spin lock\n", thid_, thread_timestamp[thid_]); //*** added by tatsu
#endif
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
  for (int i = 0; i < tuple->waiters.size(); i++)
  {
    if (thid_ == tuple->waiters[i])
      tuple->waiters.erase(tuple->waiters.begin() + i);
  }
  for (int i = 0; i < tuple->owners.size(); i++)
  {
    if (thid_ == tuple->owners[i])
      tuple->owners.erase(tuple->owners.begin() + i);
  }
}

bool TxExecutor::woundSuccess(Tuple *tuple, const int killer, const LockType my_type)
{
  int r;
  LockType retired_type;

  for (int i = 0; i < tuple->retired.size(); i++)
  {
    r = tuple->retired[i];
    retired_type = (LockType)tuple->req_type[r];
#ifndef NONTS
    if (thread_timestamp[r] > thread_timestamp[killer])
#else
    if (r > killer)
#endif
    {
      if (conflict(my_type, retired_type))
      {
        if (thread_stats[r] == 1)
        {
          return false;
        }
        else
        {
          printf("thread_stats[%d] = %d\n", r, thread_stats[r]);
          printf("thread_timestamp[%d] = %d\n", thid_, thread_stats[r]);
          printf("EXCEPTION IN WOUNDSUCCESS\n");
          // checkLists(key);
          exit(1);
        }
      }
    }
  }
  return true;
}

#ifndef NOUPGRADE
bool TxExecutor::lockUpgrade(Tuple *tuple)
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
#ifdef PRINTF
      printf("tx%d ts %d lockUpgrade tuple %d start\n", thid_, thread_timestamp[thid_], (int)key);
#endif
#ifdef BAMBOO
      checkWound(tuple->retired, LockType::EX, tuple);
#endif
      checkWound(tuple->owners, LockType::EX, tuple);
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
      if (woundSuccess(tuple, thid_, LockType::EX))
      {
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
#ifdef PRINTF
                  printf("tx%d increment commit semaphore to %d\n", thid_, commit_semaphore[thid_]);
                  checkLists(key);
#endif
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
#endif
          if (tuple->owners.size() == 1)
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
#ifdef PRINTF
                // checkLists(key);
#endif
                break;
              }
            }
#endif
            tuple->lock_.w_unlock();
            return true;
          }
#ifdef BAMBOO
        }
      }
#endif
#ifdef PRINTF
      printf("tx%d ts %d lockUpgrade failed\n", thid_, thread_timestamp[thid_]);
      checkLists(key);
#endif
      if (thread_stats[thid_] == 1)
      {
#ifdef PRINTF
        printf("tx%d ts %d should abort\n", thid_, thread_timestamp[thid_]); //*** added by tatsu
#endif
        status_ = TransactionStatus::aborted; //*** added by tatsu
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
#endif

/*
template <typename T>
void TxExecutor::checkWound(T &list, LockType lock_type, Tuple *tuple)
{
  int t;
  bool has_conflicts = false;
  LockType type;
  for (int i = 0; i < list.size(); i++)
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
#ifdef PRINTF
      printf("tx%d wounds tx%d\n", thid_, t);
#endif
      thread_stats[t] = 1;
      LockRelease(tuple, isabort);

    }
  }
}
*/