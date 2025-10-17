#include <iostream>

void ex_func_02()
{
    try
    {
        throw std::logic_error("logic");
    }
    catch (...)
    {
        std::cerr << "caught" << std::endl;
    }
}
