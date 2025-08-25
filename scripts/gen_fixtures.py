#!/usr/bin/env python3
"""
Generate many small source files across languages into the example-treesitter-repo
fixtures so tests can exercise large numbers of AST entities.

This script is intentionally simple and idempotent: it writes numbered files into
`crates/hyperzoekt/tests/fixtures/example-treesitter-repo/src/generated/<lang>/`.

Run: python3 scripts/gen_fixtures.py --count 1200
"""
import os
import argparse
from pathlib import Path

ROOT = Path("crates/hyperzoekt/tests/fixtures/example-treesitter-repo/src/generated")
LANG_TEMPLATES = {
    "rust": """// generated
pub fn func_{i}() {{
    let mut s = 0;
    for j in 0..10 {{
        if j % 2 == 0 {{ s += j; }} else {{ s -= j; }}
    }}
}}

pub struct S{i} {{ pub v: i32 }}

impl S{i} {{ pub fn new() -> Self {{ S{i} {{ v: 0 }} }} }}
""",
    "python": """# generated
def func_{i}():
    s = 0
    for j in range(10):
        if j % 2 == 0:
            s += j
        else:
            s -= j
    return s

class C{i}:
    def __init__(self):
        self.v = 0
""",
    "typescript": """// generated
export function func_{i}() {{
    let s = 0;
    for (let j = 0; j < 10; j++) {{
        if (j % 2 === 0) s += j; else s -= j;
    }}
    return s;
}}

export class C{i} {{ v: number; constructor() {{ this.v = 0; }} }}
""",
    "tsx": """// generated TSX
export function func_{i}() {{
    return null;
}}

export default function Comp{i}() {{ return null; }}
""",
    "js": """// generated
function func_{i}() {{
    for (var j=0;j<5;j++) if (j%2==0) console.log(j);
}}

var C{i} = function() {{ this.v = 0; }};
""",
    "cpp": """// generated
void func_{i}() {{
    for (int j=0;j<10;j++) {{ if (j%2==0) {{ }} }}
}}

struct S{i} {{ int v; }};
""",
    "java": """// generated
public class G{i} {{
    public static int func_{i}() {{
        int s=0; for(int j=0;j<10;j++) if (j%2==0) s+=j; else s-=j; return s;
    }}
}}
""",
    "go": """// generated
package gen{i}

func Func{i}() int {
    s := 0
    for j:=0; j<10; j++ {
        if j%2==0 { s += j } else { s -= j }
    }
    return s
}
""",
    "swift": """// generated
func func_{i}() -> Int {
    var s = 0
    for j in 0..<10 { if j % 2 == 0 { s += j } else { s -= j } }
    return s
}
""",
    "c_sharp": """// generated
public class G{i} {{
    public static int Func{i}() {{
        int s=0; for(int j=0;j<10;j++) if (j%2==0) s+=j; else s-=j; return s;
    }}
}}
""",
    "verilog": """// generated
module gen_v{i}();
    integer j;
    initial begin
        for (j=0;j<10;j=j+1) begin
            if (j%2==0) ;
        end
    end
endmodule
""",
}


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_files(lang, template, start_idx, end_idx):
    d = ROOT / lang
    ensure_dir(d)
    for i in range(start_idx, end_idx):
        fname = d / f"gen_{i}.{ext_for_lang(lang)}"
        with open(fname, "w") as f:
            # avoid str.format with braces in many languages; replace {i} token
            f.write(template.replace("{i}", str(i)))


def ext_for_lang(lang):
    return {
        "rust": "rs",
        "python": "py",
        "typescript": "ts",
        "tsx": "tsx",
        "js": "js",
        "cpp": "cpp",
        "java": "java",
        "go": "go",
        "swift": "swift",
        "c_sharp": "cs",
        "verilog": "v",
    }[lang]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000, help="approx total entities to generate")
    args = parser.parse_args()
    count = args.count

    # rough estimate: each generated file produces ~3 entities (1 function, 1 class/struct, maybe others)
    files_needed = max(10, count // 3)
    langs = list(LANG_TEMPLATES.keys())
    per_lang = max(1, files_needed // len(langs))

    idx = 0
    for lang in langs:
        template = LANG_TEMPLATES[lang]
        write_files(lang, template, idx, idx + per_lang)
        idx += per_lang

    print(f"Wrote ~{per_lang * len(langs)} files across {len(langs)} languages to {ROOT}")

if __name__ == '__main__':
    main()
