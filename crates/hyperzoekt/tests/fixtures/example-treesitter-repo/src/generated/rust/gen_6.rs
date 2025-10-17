// generated
pub fn func_6() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S6 {{ pub v: i32 }}

impl S6 {{ pub fn new() -> Self {{ S6 {{ v: 0 }} }} }}
