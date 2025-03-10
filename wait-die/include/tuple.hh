#pragma once

#include <atomic>
#include <mutex>

#include "../../include/cache_line_size.hh"
#include "../../include/inline.hh"
#include "../../include/rwlock.hh"

using namespace std;

#define UNLOCKED INT32_MAX

class Tuple {
public:
  alignas(CACHE_LINE_SIZE) RWLock lock_;
  RWLock latch_; // for lock, writer_, reader_, writeflag_
  char val_[VAL_SIZE];
  uint32_t writer_; //txid
  uint32_t reader_; //txid
  bool writeflag_; //writeflag_ = true, writer_ = 1 => tx1 is acquiring write lock
};
