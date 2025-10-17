using System.Collections.Generic;

public class GenericHolder<T> {
    public T Value { get; set; }
}

public static class Util {
    public static T Id<T>(T x) => x;
}
