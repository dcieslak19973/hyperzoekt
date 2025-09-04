pub struct Person {
    pub name: String,
}

impl Person {
    pub fn new(name: String) -> Self {
        Person { name }
    }
    pub fn greet(&self) {
        println!("hello {}", self.name);
    }
}

// additional impl block (extension-like)
impl Person {
    pub fn salute(&self) {
        println!("salute {}", self.name);
    }

    pub fn ext_static() {}
}
