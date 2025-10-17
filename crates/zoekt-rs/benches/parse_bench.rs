use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn parse_bench(c: &mut Criterion) {
    let sample = r#"fn a() {}
struct S {}
impl S { fn m(&self) {} }
"#;

    c.bench_function("typesitter_parse_rs", |b| {
        b.iter(|| {
            // call the public extractor; black_box to avoid accidental optimization
            let _ = zoekt_rs::typesitter::extract_symbols_typesitter(black_box(sample), "rs");
        })
    });
}

criterion_group!(benches, parse_bench);
criterion_main!(benches);
