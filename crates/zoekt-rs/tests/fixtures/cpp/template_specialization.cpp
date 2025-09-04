#include "templates_simple.h"

template<>
class Holder<int> {
public:
    Holder(int v) : val(v) {}
    int get() const { return val; }
private:
    int val;
};

struct Outer {
    template<typename T>
    struct Inner {
        void do_it();
    };
};
