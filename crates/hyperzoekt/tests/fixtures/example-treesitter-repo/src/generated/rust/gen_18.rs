// generated
pub fn func_18() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S18 {{ pub v: i32 }}

impl S18 {{ pub fn new() -> Self {{ S18 {{ v: 0 }} }} }}
