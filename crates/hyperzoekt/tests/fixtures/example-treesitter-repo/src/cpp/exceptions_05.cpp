class CppEx
{
public:
    void run()
    {
        try
        {
            throw "err";
        }
        catch (const char *s)
        {
            (void)s;
        }
    }
};
