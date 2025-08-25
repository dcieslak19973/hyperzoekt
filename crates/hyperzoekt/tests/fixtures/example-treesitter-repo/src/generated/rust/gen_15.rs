// generated
pub fn func_15() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S15 {{ pub v: i32 }}

impl S15 {{ pub fn new() -> Self {{ S15 {{ v: 0 }} }} }}
