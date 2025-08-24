// generated
pub fn func_4() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S4 {{ pub v: i32 }}

impl S4 {{ pub fn new() -> Self {{ S4 {{ v: 0 }} }} }}
