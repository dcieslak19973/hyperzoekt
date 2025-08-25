// generated
pub fn func_12() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S12 {{ pub v: i32 }}

impl S12 {{ pub fn new() -> Self {{ S12 {{ v: 0 }} }} }}
