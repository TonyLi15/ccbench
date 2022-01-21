#include <iostream>

enum lock_type
{
    SH = -1,
    EX = 1
};

class Bamboo
{
private:
    /* data */
public:
    Bamboo(/* args */);
    ~Bamboo();
    bool conflict(int t1, int t2, uint64_t key);
};

Bamboo::Bamboo(/* args */)
{
}

Bamboo::~Bamboo()
{
}
