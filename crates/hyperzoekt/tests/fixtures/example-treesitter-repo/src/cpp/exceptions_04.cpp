int may_fail(int x)
{
    if (x == 0)
        throw 42;
    return x;
}
