// generated
pub fn func_32() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S32 {{ pub v: i32 }}

impl S32 {{ pub fn new() -> Self {{ S32 {{ v: 0 }} }} }}
