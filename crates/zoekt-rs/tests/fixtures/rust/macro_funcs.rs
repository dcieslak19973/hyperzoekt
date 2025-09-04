macro_rules! make_fn {
    ($name:ident) => {
        pub fn $name() -> i32 {
            1
        }
    };
}

make_fn!(generated);

pub fn wrapper() -> i32 {
    generated()
}
