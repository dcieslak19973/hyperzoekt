// generated
pub fn func_7() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S7 {{ pub v: i32 }}

impl S7 {{ pub fn new() -> Self {{ S7 {{ v: 0 }} }} }}
