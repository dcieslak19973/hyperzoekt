pub struct Complex<T, U> {
    t: T,
    u: U,
}

impl<T, U> Complex<T, U>
where
    T: Clone + Default,
    U: Iterator<Item = T>,
    for<'a> &'a U: IntoIterator<Item = T>,
{
    pub fn new() -> Self {
        Complex {
            t: T::default(),
            u: Default::default(),
        }
    }
}
