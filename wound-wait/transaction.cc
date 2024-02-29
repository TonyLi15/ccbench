
#include <stdio.h>
#include <string.h>

#include <atomic>

#include "../include/backoff.hh"
#include "../include/debug.hh"
#include "../include/procedure.hh"
#include "../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"

#define NONTS

using namespace std;

extern void display_procedure_vector(std::vector<Procedure> &pro);

void TxExecutor::warmupTuple(uint64_t key) {
  Tuple *tuple;
#if MASSTREE_USE
  tuple = MT.get_value(key);
#else
  tuple = get_tuple(Table, key);
#endif
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    tuple->readers[i] = 0;
    tuple->writers[i] = 0;
  }
  memcpy(tuple->val_, write_val_, VAL_SIZE);
}

/**
 * @brief Search xxx set
 * @detail Search element of local set corresponding to given key.
 * In this prototype system, the value to be updated for each worker thread 
 * is fixed for high performance, so it is only necessary to check the key match.
 * @param Key [in] the key of key-value
 * @return Corresponding element of local set
 */
inline SetElement<Tuple> *TxExecutor::searchReadSet(uint64_t key) {
  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
    if ((*itr).key_ == key) return &(*itr);
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
inline SetElement<Tuple> *TxExecutor::searchWriteSet(uint64_t key) {
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    if ((*itr).key_ == key) return &(*itr);
  }

  return nullptr;
}

/**
 * @brief function about abort.
 * Clean-up local read/write set.
 * Release locks.
 * @return void
 */
void TxExecutor::abort() {
  /**
   * Release locks
   */
  unlockList();

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
void TxExecutor::commit() {
  //printf("tx%d commit\n", thid_);
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
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
#ifdef NONTS
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
void TxExecutor::read(uint64_t key) {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif  // ADD_ANALYSIS

  /**
   * read-own-writes or re-read from local read set.
   */
  if (searchWriteSet(key) || searchReadSet(key)) goto FINISH_READ;

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

// *** added by tatsu from here
  while (1) {
    if (tuple->lock_.r_trylock()) {
      r_lock_list_.emplace_back(&tuple->lock_);
      read_set_.emplace_back(key, tuple, tuple->val_);
      tuple->readers[thid_] = txid_;
      break;
    }
    else {
      /**
       * wound wait
       */
      for (int i = 0; i < FLAGS_thread_num; i++) {
        if (tuple->writers[i] >= 0 && 
        #ifndef NONTS
        thread_timestamp[i] > thread_timestamp[thid_]) {
        #else
        tuple->writers[i] > txid_) {
        #endif
          thread_stats[i] = 1;
        }
      }
      if (thread_stats[thid_] == 1) goto FINISH_READ;
      usleep(1);
    }
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
void TxExecutor::write(uint64_t key) {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif

  // if it already wrote the key object once.
  if (searchWriteSet(key)) goto FINISH_WRITE;
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
  int owner;
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr) {
    if ((*rItr).key_ == key) {  // hit
// *** added by tatsu from here
      while (1) {
        if (!(*rItr).rcdptr_->lock_.tryupgrade()) {
          for (int i = 0; i < FLAGS_thread_num; i++) {
            owner = tuple->writers[i] + tuple->readers[i] + 1;
            if (owner >= 0 && 
        #ifndef NONTS
            thread_timestamp[i] > thread_timestamp[thid_]) {
        #else
            owner > thid_) {
        #endif
              thread_stats[i] = 1;
            }
          }
          if (thread_stats[thid_] == 1) goto FINISH_WRITE;
          usleep(1);      
        }
        else {
          break;
        }
      }

      // upgrade success
      // remove old element of read lock list.
      tuple->readers[thid_] = 0; // *** added by tatsu
      tuple->writers[thid_] = txid_; // *** added by tatsu
      for (auto lItr = r_lock_list_.begin(); lItr != r_lock_list_.end();
           ++lItr) {
        if (*lItr == &((*rItr).rcdptr_->lock_)) {
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

  while (1) {
    if (!tuple->lock_.w_trylock()) {
      for (int i = 0; i < FLAGS_thread_num; i++) {
        owner = tuple->writers[i] + tuple->readers[i] + 1;
        if (owner && 
#ifndef NONTS
        thread_timestamp[i] > thread_timestamp[this->thid_]) {
#else
        owner > thid_) {
#endif
          thread_stats[i] = 1;
        }
      }
      if (thread_stats[thid_] == 1) goto FINISH_WRITE;
      usleep(1);
    }
    else {
      break;
    }
  }

  /**
   * Register the contents to write lock list and write set.
   */
  tuple->writers[thid_] = txid_; // *** added by tatsu
  w_lock_list_.emplace_back(&tuple->lock_);
  write_set_.emplace_back(key, tuple);

FINISH_WRITE:
#if ADD_ANALYSIS
  sres_->local_write_latency_ += rdtscp() - start;
#endif  // ADD_ANALYSIS
  return;
}

/**
 * @brief transaction readWrite (RMW) operation
 */
void TxExecutor::readWrite(uint64_t key) {
  // if it already wrote the key object once.
  if (searchWriteSet(key)) goto FINISH_WRITE;
  Tuple *tuple;
  tuple = get_tuple(Table, key);
  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr) {
    if ((*rItr).key_ == key) {  // hit

      while (1) {
        if (!(*rItr).rcdptr_->lock_.tryupgrade()) {
          for (int i = 0; i < FLAGS_thread_num; i++) {
            if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) && 
            thread_timestamp[i] > thread_timestamp[this->thid_]) {
              thread_stats[i] = 1;
            }
          }     
          if (thread_stats[thid_] == 1) goto FINISH_WRITE; 
          usleep(1);
        }
        else {
          break;
        }
      }

      // upgrade success
      // remove old element of read set.
      tuple->readers[this->thid_] = 0; // *** added by tatsu
      tuple->writers[this->thid_] = 1; // *** added by tatsu
      for (auto lItr = r_lock_list_.begin(); lItr != r_lock_list_.end();
           ++lItr) {
        if (*lItr == &((*rItr).rcdptr_->lock_)) {
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

  while (1) {
    if (!tuple->lock_.w_trylock()) {
      for (int i = 0; i < FLAGS_thread_num; i++) {
        if ((tuple->writers[i] == 1 || tuple->readers[i] == 1) && 
        thread_timestamp[i] > thread_timestamp[this->thid_]) {
          thread_stats[i] = 1;
        }
      }
      if (thread_stats[thid_] == 1) goto FINISH_WRITE;
      usleep(1);
    }
    else {
      break;
    }
  }

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
void TxExecutor::unlockList() {
  for (auto itr = r_lock_list_.begin(); itr != r_lock_list_.end(); ++itr)
    (*itr)->r_unlock();

  for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) // *** added by tatsu
    (*itr).rcdptr_->readers[this->thid_] = -1;                       // *** added by tatsu

  for (auto itr = w_lock_list_.begin(); itr != w_lock_list_.end(); ++itr)
    (*itr)->w_unlock();

  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) // *** added by tatsu
    (*itr).rcdptr_->writers[this->thid_] = -1;                         // *** added by tatsu

  /**
   * Clean-up local lock set.
   */
  r_lock_list_.clear();
  w_lock_list_.clear();
}
