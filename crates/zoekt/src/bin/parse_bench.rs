use std::env;
use std::time::Instant;

// Small bench binary that repeatedly invokes the typesitter extractor to
// measure per-parse overhead. It uses the same extractor which now caches
// per-language Parser instances on a per-thread basis.
fn main() {
    let args: Vec<String> = env::args().collect();
    let iters: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let lang = args.get(2).map(|s| s.as_str()).unwrap_or("rs");
    let sample = match lang {
        "rs" => {
            r#"fn a() {}
struct S {}
impl S { fn m(&self) {} }
"#
        }
        "go" => {
            r#"package main
func Add(a int, b int) int { return a + b }
"#
        }
        "py" => {
            r#"def f(): pass
class C: pass
"#
        }
        _ => r#"fn a() {}"#,
    };

    // warmup
    for _ in 0..10 {
        let _ = zoekt_rs::typesitter::extract_symbols_typesitter(sample, lang);
    }

    let t0 = Instant::now();
    for _ in 0..iters {
        let _ = zoekt_rs::typesitter::extract_symbols_typesitter(sample, lang);
    }
    let elapsed = t0.elapsed();
    println!(
        "Ran {} parses for '{}' in {:?} -> {:?} per parse",
        iters,
        lang,
        elapsed,
        elapsed / (iters as u32)
    );
}
