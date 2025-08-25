// generated
pub fn func_10() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S10 {{ pub v: i32 }}

impl S10 {{ pub fn new() -> Self {{ S10 {{ v: 0 }} }} }}
