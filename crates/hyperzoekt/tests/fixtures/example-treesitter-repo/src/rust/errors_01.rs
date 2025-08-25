pub fn may_fail(b: bool) -> Result<i32, &'static str> {
    if b {
        Err("fail")
    } else {
        Ok(1)
    }
}

pub fn caller() {
    match may_fail(true) {
        Ok(v) => println!("ok {}", v),
        Err(e) => eprintln!("err {}", e),
    }
}
