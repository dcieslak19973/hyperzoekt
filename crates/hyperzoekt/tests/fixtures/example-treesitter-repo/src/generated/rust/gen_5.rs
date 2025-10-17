// generated
pub fn func_5() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S5 {{ pub v: i32 }}

impl S5 {{ pub fn new() -> Self {{ S5 {{ v: 0 }} }} }}
