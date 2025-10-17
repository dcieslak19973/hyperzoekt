pub enum Direction {
    North,
    South,
    East,
    West,
}

pub enum Result<T> {
    Ok(T),
    Err(String),
}
