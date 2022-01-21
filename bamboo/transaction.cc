
#include <stdio.h>
#include <string.h>

#include <atomic>

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/procedure.hh"
#include "../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"

#define PRINTF
// #define BAMBOO

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
  unlockList();

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();

  printf("tx%d ts %d abort complete\n", thid_, thread_timestamp[thid_]);
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
  unlockList();

  /**
   * Clean-up local read/write set.
   */
  read_set_.clear();
  write_set_.clear();
  printf("tx%d ts %d commit complete\n", thid_, thread_timestamp[thid_]);
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

#ifdef DLR0
  /**
   * Acquire lock with wait.
   */
  tuple->lock_.r_lock();
  r_lock_list_.emplace_back(&tuple->lock_);
  read_set_.emplace_back(key, tuple, tuple->val_);
#elif defined(DLR1)
  // *** added by tatsu from here
  // while (1)
  // {
  //   if (tuple->lock_.r_trylock())
  //   {
  //     r_lock_list_.emplace_back(&tuple->lock_);
  //     read_set_.emplace_back(key, tuple, tuple->val_);
  //     tuple->readers[this->thid_] = 1;
  //     break;
  //   }
  //   else
  //   {
  //     /**
  //      * wound wait
  //      */
  //     for (int i = 0; i < FLAGS_thread_num; i++)
  //     {
  //       if (tuple->writers[i] == 1 &&
  //           thread_timestamp[i] > thread_timestamp[this->thid_])
  //       {
  //         thread_stats[i] = 1;
  //       }
  //     }
  //     if (thread_stats[thid_] == 1)
  //       goto FINISH_READ;
  //     usleep(1);
  //   }
  // }

// to here ***
#endif
  LockAcquire(key, LockType::SH);
  if (spinLock(thid_, key))
  {
    read_set_.emplace_back(key, tuple, tuple->val_);
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
void TxExecutor::write(uint64_t key)
{
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif

  // if it already wrote the key object once.
  if (searchWriteSet(key))
    goto FINISH_WRITE;
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    { // hit
#if DLR0
      (*rItr).rcdptr_->lock_.upgrade();
#elif defined(DLR1)
      // *** added by tatsu from here
      // while (1)
      // {
      //   if (!(*rItr).rcdptr_->lock_.tryupgrade())
      //   {
      //     for (int i = 0; i < FLAGS_thread_num; i++)
      //     {
      //       if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) &&
      //           thread_timestamp[i] > thread_timestamp[this->thid_])
      //       {
      //         thread_stats[i] = 1;
      //       }
      //     }
      //     if (thread_stats[thid_] == 1)
      //       goto FINISH_WRITE;
      //     usleep(1);
      //   }
      //   else
      //   {
      //     break;
      //   }
      // }
// to here ***
#endif
      if (lockUpgrade(thid_, key))
      {
        // upgrade success
        // remove old element of read lock list.
        if (spinLock(thid_, key))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          read_set_.erase(rItr);
        }
      }
      // printf("tx%d goes to finish_write\n", thid_);
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  //Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  //tuple = get_tuple(Table, key);
#endif

#if DLR0
  /**
   * Lock with wait.
   */
  tuple->lock_.w_lock();
#elif defined(DLR1)
  // *** added by tatsu from here
  // while (1)
  // {
  //   if (!tuple->lock_.w_trylock())
  //   {
  //     for (int i = 0; i < FLAGS_thread_num; i++)
  //     {
  //       if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) &&
  //           thread_timestamp[i] > thread_timestamp[this->thid_])
  //       {
  //         thread_stats[i] = 1;
  //       }
  //     }
  //     if (thread_stats[thid_] == 1)
  //       goto FINISH_WRITE;
  //     usleep(1);
  //   }
  //   else
  //   {
  //     break;
  //   }
  // }
// to here ***
#endif
  LockAcquire(key, LockType::EX);
  if (spinLock(thid_, key))
  {
    tuple->writers[this->thid_] = 1; // *** added by tatsu
    w_lock_list_.emplace_back(&tuple->lock_);
    write_set_.emplace_back(key, tuple);
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
void TxExecutor::readWrite(uint64_t key)
{
  // if it already wrote the key object once.
  if (searchWriteSet(key))
    goto FINISH_WRITE;
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr)
  {
    if ((*rItr).key_ == key)
    { // hit
#if DLR0
      (*rItr).rcdptr_->lock_.upgrade();
#elif defined(DLR1)
      // *** added by tatsu from here
      while (1)
      {
        if (!(*rItr).rcdptr_->lock_.tryupgrade())
        {
          for (int i = 0; i < FLAGS_thread_num; i++)
          {
            if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) &&
                thread_timestamp[i] > thread_timestamp[this->thid_])
            {
              thread_stats[i] = 1;
            }
          }
          if (thread_stats[thid_] == 1)
            goto FINISH_WRITE;
          usleep(1);
        }
        else
        {
          break;
        }
      }
// to here ***
#endif

      // upgrade success
      // remove old element of read set.
      tuple->readers[this->thid_] = 0; // *** added by tatsu
      tuple->writers[this->thid_] = 1; // *** added by tatsu
      for (auto lItr = r_lock_list_.begin(); lItr != r_lock_list_.end();
           ++lItr)
      {
        if (*lItr == &((*rItr).rcdptr_->lock_))
        {
          write_set_.emplace_back(key, (*rItr).rcdptr_);
          w_lock_list_.emplace_back(&(*rItr).rcdptr_->lock_);
          r_lock_list_.erase(lItr);
          break;
        }
      }

      read_set_.erase(rItr);
      goto FINISH_WRITE;
    }
  }

  /**
   * Search tuple from data structure.
   */
  //Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#if ADD_ANALYSIS
  ++sres_->local_tree_traversal_;
#endif
#else
  //tuple = get_tuple(Table, key);
#endif

#if DLR0
  /**
   * Lock with wait.
   */
  tuple->lock_.w_lock();
#elif defined(DLR1)
  // *** added by tatsu from here
  while (1)
  {
    if (!tuple->lock_.w_trylock())
    {
      for (int i = 0; i < FLAGS_thread_num; i++)
      {
        if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) &&
            thread_timestamp[i] > thread_timestamp[this->thid_])
        {
          thread_stats[i] = 1;
        }
      }
      if (thread_stats[thid_] == 1)
        goto FINISH_WRITE;
      usleep(1);
    }
    else
    {
      break;
    }
  }
// to here ***
#endif

  // read payload
  memcpy(this->return_val_, tuple->val_, VAL_SIZE);
  // finish read.

  /**
   * Register the contents to write lock list and write set.
   */
  w_lock_list_.emplace_back(&tuple->lock_);
  write_set_.emplace_back(key, tuple);

FINISH_WRITE:
  return;
}

/**
 * @brief unlock and clean-up local lock set.
 * @return void
 */
void TxExecutor::unlockList()
{
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
  {
    LockRelease(thid_, (*itr).key_, thread_stats[thid_]);
    (*itr).rcdptr_->req_type[this->thid_] = 0;
  }
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
  {
    LockRelease(thid_, (*itr).key_, thread_stats[thid_]);
    (*itr).rcdptr_->req_type[this->thid_] = 0;
  }

  // for (auto itr = r_lock_list_.begin(); itr != r_lock_list_.end(); ++itr)
  //   (*itr)->r_unlock();

  // for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) // *** added by tatsu
  //   (*itr).rcdptr_->readers[this->thid_] = 0;                       // *** added by tatsu

  // for (auto itr = w_lock_list_.begin(); itr != w_lock_list_.end(); ++itr)
  //   (*itr)->w_unlock();

  // for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) // *** added by tatsu
  //   (*itr).rcdptr_->writers[this->thid_] = 0;                         // *** added by tatsu

  /**
   * Clean-up local lock set.
   */
  r_lock_list_.clear();
  w_lock_list_.clear();
}

bool TxExecutor::conflict(LockType x, LockType y)
{
  if ((x == LockType::EX) || (y == LockType::EX))
    return true;
  else
    return false;
}

void TxExecutor::PromoteWaiters(uint64_t key)
{
  // printf("lets promote\n");
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  int t;
  int owner = tuple->owners[0];
  LockType t_type;
  LockType owners_type;
  LockType retired_type;
  bool owner_exists = false;
  // bool holds_shared_lock = false;

  if (tuple->owners.size() > 0)
  {
    owners_type = (LockType)tuple->req_type[owner];
    owner_exists = true;
  }
  // printf("tuple %d waiter_list size is %lu\n", tuple->key, tuple->waiters->wait_list.size());
  for (int i = 0; i < tuple->waiters.size(); i++)
  {
    // printf("promote\n");
    t = tuple->waiters[i];
    t_type = (LockType)tuple->req_type[t];
    if (owner_exists && conflict(t_type, owners_type))
    {
#ifdef PRINTF
      // printf("Promotion break\n");
#endif
      break;
    }
    // else if (t_type == LockType::SH)
    // {
    //   if (tuple->sharedLock() == false)
    //     break;
    //   else
    //     holds_shared_lock = true;
    // }
    if (tuple->remove(t, tuple->waiters) == false)
    {
      printf("REMOVE FAILURE: PromoteWaiters tx%d\n", thid_);
      exit(1);
    }
    tuple->ownersAdd(t);
// if (holds_shared_lock == true)
// {
//   tuple->sharedUnlock();
// }
#ifdef BAMBOO
    for (auto retired : tuple->retired)
    {
      retired_type = (LockType)tuple->req_type[retired];
      if (conflict(t_type, retired_type))
      {
        commit_semaphore[t]++;
        break;
      }
    }
#endif
  }
  // printf("tuple %d waiter_list size is %lu\n", tuple->key, tuple->waiters->wait_list.size());
  // printf("tuple %d owner_list size is %lu\n", tuple->key, tuple->owners->owner_list.size());
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
    if (has_conflicts == true && thread_timestamp[thid_] < thread_timestamp[t])
    {
#ifdef PRINTF
      printf("tx%d wounds tx%d\n", thid_, t);
#endif
      thread_stats[t] = 1;
    }
  }
}

void TxExecutor::LockAcquire(uint64_t key, LockType lock_type)
{
  int t;
  bool has_conflicts = false;
  bool already_acquired = false;
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  tuple->req_type[thid_] = lock_type;
  LockType type;
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
#ifdef PRINTF
      // printf("tx%d lock acquire tuple %d, ts %d\n", thid_, (int)key, thread_timestamp[thid_]);
#endif
      has_conflicts = false;
#ifdef BAMBOO
      checkWound(tuple->retired, lock_type, tuple);
#endif
      checkWound(tuple->owners, lock_type, tuple);

      // tuple->waitersAdd(thid_);
      if (tuple->sortAdd(thid_, tuple->waiters) == false)
      {
        printf("SORTADD FAILURE\n");
        exit(1);
      }
// printf("tuple->waiters.wait_list size is %lu\n", tuple->waiters->wait_list.size());
#ifdef PRINTF
      // printf("tx%d ts %d PromoteWaiters\n", thid_, thread_timestamp[thid_]);
#endif
      PromoteWaiters(key);

      tuple->lock_.w_unlock();
      return;
    }
    else
    {
      // printf("tx%d NO LATCH FOR LOCKACQUIRE tuple %d\n", thid_,  (int)key);
      usleep(1);
    }
  }
}

void wound(int txn, vector<int> all_owners)
{
  for (int i = 0; i < all_owners.size(); i++)
  {
    if (txn == all_owners[i])
    {
      for (int j = i + 1; j < all_owners.size(); j++) // int j = i + 1 or int j = i?
      {
        thread_stats[all_owners[j]] = 1;
        // TODO actually abort
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

void TxExecutor::LockRelease(int txn, uint64_t key, bool is_abort)
{
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  bool was_head = false;
  LockType type = (LockType)tuple->req_type[txn];
  while (1)
  {

    if (tuple->lock_.w_trylock())
    {
      auto all_owners = concat(tuple->retired, tuple->owners);
#ifdef BAMBOO
      if (tuple->retired.size() > 0 && tuple->retired[0] == txn) // CAUTION
      {
        was_head = true;
      }
#endif
      if (is_abort && type == LockType::EX)
      {
        wound(txn, all_owners); // lock is released here
      }
#ifdef BAMBOO
      tuple->remove(txn, tuple->retired);
#endif
      if (tuple->remove(txn, tuple->owners) == false)
      {
        printf("REMOVE FAILURE: LockRelease tx%d\n", thid_);
        exit(1);
      }
#ifdef PRINTF
      printf("tx%d ts %d LockRelease tuple %d complete\n", thid_, thread_timestamp[thid_], (int)key);
#endif
#ifdef BAMBOO
      all_owners = concat(tuple->retired, tuple->owners);
      if (was_head && conflict(type, (LockType)tuple->req_type[all_owners[0]]))
      {
        for (int i = 0; i < all_owners.size(); i++)
        {
          commit_semaphore[all_owners[i]]--;
          if ((i + 1) < all_owners.size() && conflict((LockType)tuple->req_type[all_owners[i]], (LockType)tuple->req_type[all_owners[i + 1]])) // CAUTION: may be wrong
            break;
        }
      }
#endif
      PromoteWaiters(key);
      tuple->lock_.w_unlock();
      return;
    }
    else
    {
      // printf("tx%d NO LATCH FOR LOCKRELEASE tuple %d\n", thid_,  (int)key);
      usleep(1);
    }
  }
}

void TxExecutor::LockRetire(uint64_t key)
{
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  while (1)
  {
    if (tuple->lock_.w_trylock())
    {
      tuple->remove(thid_, tuple->owners);
      tuple->sortAdd(thid_, tuple->retired);
      PromoteWaiters(key);
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
    if (thread_timestamp[txn] < thread_timestamp[(*tid)])
    {
      list.insert(tid, txn);
      return true;
    }
  }
  list.push_back(txn);
  return true;
}

bool TxExecutor::spinLock(int txn, uint64_t key)
{
  Tuple *tuple;
  tuple = get_tuple(Table, key);
#ifdef PRINTF
  // printf("tx%d ts %d getLock: tuple %d\n", thid_, thread_timestamp[thid_], (int)key);
#endif
  // printf("tuple->owners->owner_list.size() = %lu\n", tuple->owners->owner_list.size());
  // printf("txn address = ");
  // cout << txn << endl;
  // printf("tuple->owners->owner_list[0] = ");
  // cout << tuple->owners->owner_list[0] << endl;
  while (1)
  {
    for (int i = 0; i < tuple->owners.size(); i++)
    {
      if (txn == tuple->owners[i])
      {
#ifdef PRINTF
        // printf("tx%d ts %d found\n", thid_, thread_timestamp[thid_]);
#endif
        return true;
      }
    }
#ifdef PRINTF
    // printf("tx%d ts %d not found: tuple is %d\n", thid_, thread_timestamp[thid_], (int)key);
    for (int i = 0; i < tuple->owners.size(); i++)
    {
      // printf("tuple %d owners[%d] is tx%d ts %d\n", (int)key, i, tuple->owners[i], thread_timestamp[tuple->owners[i]]);
    }
    for (int i = 0; i < tuple->waiters.size(); i++)
    {
      // printf("tuple %d waiters[%d] is tx%d ts %d\n", (int)key, i, tuple->waiters[i], thread_timestamp[tuple->waiters[i]]);
    }
#endif
    if (thread_stats[txn] == 1)
    {
#ifdef PRINTF
      // printf("tx%d ts %d should abort\n", thid_, thread_timestamp[thid_]); //*** added by tatsu
#endif
      for (int i = 0; i < tuple->waiters.size(); i++)
      {
        if(thid_ == tuple->waiters[i])
          tuple->waiters.erase(tuple->waiters.begin() + i);
      }
      for (int i = 0; i < tuple->owners.size(); i++)
      {
        if(thid_ == tuple->owners[i])
          tuple->owners.erase(tuple->owners.begin() + i);
      }
      // LockRelease(thid_, key, true);
      status_ = TransactionStatus::aborted; //*** added by tatsu
      return false;
    }
    #ifdef PRINTF
    // usleep(1000000);
    #endif
    usleep(1);
  }
}

bool TxExecutor::lockUpgrade(int txn, uint64_t key)
{
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  while (1)
  {
    if (tuple->lock_.w_trylock()) // might need acquire_lock as well
    {
#ifdef PRINTF
      printf("tx%d ts %d lockUpgrade tuple %d start\n", txn, thread_timestamp[thid_], (int)key);
#endif
      if (tuple->owners.size() == 1)
      {
        if (tuple->req_type[txn] == LockType::SH)
        {
          tuple->req_type[txn] = LockType::EX;
          tuple->lock_.w_unlock();
          return true;
        }
      }
#ifdef PRINTF
      printf("tx%d ts %d lockUpgrade failed\n", txn, thread_timestamp[thid_]);
      for (int i = 0; i < tuple->owners.size(); i++)
      {
        printf("tuple %d owners[%d] is tx%d ts %d\n", (int)key, i, tuple->owners[i], thread_timestamp[tuple->owners[i]]);
      }
#endif
      checkWound(tuple->owners, LockType::EX, tuple);
      if (thread_stats[txn] == 1)
      {
#ifdef PRINTF
        printf("tx%d ts %d should abort\n", thid_, thread_timestamp[thid_]); //*** added by tatsu
#endif
        status_ = TransactionStatus::aborted; //*** added by tatsu
        tuple->lock_.w_unlock();
        return false;
      }
      tuple->lock_.w_unlock();
      #ifdef PRINTF
      // usleep(1000000);
      #endif
      usleep(1);
    }
    else
    {
      // printf("tx%d NO LATCH FOR LOCKUPGRADE tuple %d\n", thid_, (int)key);
      usleep(1);
    }
  }
}