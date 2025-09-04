pub trait IteratorLike {
    type Item;
    fn next(&mut self) -> Option<Self::Item>;
}

pub struct MyIter;

impl IteratorLike for MyIter {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
