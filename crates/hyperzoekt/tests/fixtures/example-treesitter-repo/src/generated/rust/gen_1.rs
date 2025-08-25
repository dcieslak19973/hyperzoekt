// generated
pub fn func_1() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S1 {{ pub v: i32 }}

impl S1 {{ pub fn new() -> Self {{ S1 {{ v: 0 }} }} }}
