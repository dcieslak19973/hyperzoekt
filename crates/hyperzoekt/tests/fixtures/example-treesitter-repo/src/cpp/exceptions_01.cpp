#include <stdexcept>
void ex_func_01()
{
    throw std::runtime_error("ex 01");
}

int main01()
{
    try
    {
        ex_func_01();
    }
    catch (const std::exception &e)
    {
        return 1;
    }
    return 0;
}
