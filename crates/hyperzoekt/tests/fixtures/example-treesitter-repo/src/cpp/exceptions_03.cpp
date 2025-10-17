#include <exception>
struct MyErr : public std::exception
{
    const char *what() const noexcept override { return "myerr"; }
};

void ex_func_03() noexcept(false)
{
    throw MyErr();
}
