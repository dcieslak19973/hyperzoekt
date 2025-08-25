// generated
pub fn func_8() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S8 {{ pub v: i32 }}

impl S8 {{ pub fn new() -> Self {{ S8 {{ v: 0 }} }} }}
