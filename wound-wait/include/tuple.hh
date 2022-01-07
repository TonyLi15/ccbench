#pragma once

#include <atomic>
#include <mutex>

#include "../../include/cache_line_size.hh"
#include "../../include/inline.hh"
#include "../../include/rwlock.hh"

using namespace std;

class Tuple {
public:
  alignas(CACHE_LINE_SIZE) RWLock lock_;
  char val_[VAL_SIZE];
  int readers[224] = {0}; // *** added by tatsu: readers[i] = 1 means thread i is reading this tuple
  int writers[224] = {0}; // *** added by tatsu: writers[i] = 1 means thread i is writing this tuple
};
