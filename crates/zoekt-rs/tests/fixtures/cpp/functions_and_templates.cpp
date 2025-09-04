#include <vector>

template<typename T>
T add(T a, T b) { return a + b; }

template<typename T>
T sum_vec(const std::vector<T>& v) {
    T s = T();
    for (auto &x : v) s = s + x;
    return s;
}

class UsesTemplates {
public:
    void call_add() {
        auto x = add<int>(1,2);
    }
};
