#pragma once

template <typename T>
class Holder {
public:
    Holder(T v) : val(v) {}
    T get() const { return val; }
private:
    T val;
};

void free_function();
