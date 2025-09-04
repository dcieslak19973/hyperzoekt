pub struct Wrapper<T> {
    value: T,
}

impl<T> Wrapper<T>
where
    T: Clone + Default,
{
    pub fn new() -> Self {
        Wrapper {
            value: T::default(),
        }
    }
}

pub fn uses_wrapper() {
    let _ = Wrapper::<i32>::new();
}
