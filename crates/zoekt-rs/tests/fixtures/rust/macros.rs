macro_rules! make_struct {
    ($name:ident) => {
        pub struct $name {
            pub v: i32,
        }
    };
}

make_struct!(AutoGen);

impl AutoGen {
    pub fn new() -> Self {
        AutoGen { v: 0 }
    }
}
