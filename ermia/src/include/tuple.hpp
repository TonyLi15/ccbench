#pragma once

#include <atomic>
#include <cstdint>
#include "version.hpp"

class Tuple {
public: 
	std::atomic<Version *> latest;
	unsigned int key;
	int8_t padding[48];
};
