// generated
pub fn func_3() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S3 {{ pub v: i32 }}

impl S3 {{ pub fn new() -> Self {{ S3 {{ v: 0 }} }} }}
