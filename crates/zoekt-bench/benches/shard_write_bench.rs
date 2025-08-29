use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Instant;

fn shard_write_bench(c: &mut Criterion) {
    c.bench_function("shard_write_synthetic", |b| {
        b.iter_custom(|iters| {
            let t0 = Instant::now();
            for _ in 0..iters {
                // Small synthetic repo: a few small files (reduced count for bench)
                let dir = tempfile::tempdir().unwrap();
                let repo = dir.path();
                let file_count = 4000usize;
                let lines_per_file = 200usize;
                for i in 0..file_count {
                    let mut s = String::new();
                    for j in 0..lines_per_file {
                        s.push_str(&format!("// file {} line {}\n", i, j));
                        s.push_str(&format!("fn sym_{:04}_{}() {{}}\n", i, j));
                        s.push_str("let x = 42;\n");
                    }
                    std::fs::write(repo.join(format!("file_{:04}.rs", i)), s).unwrap();
                }

                // Build index and write a shard
                let idx = zoekt_rs::build_in_memory_index(repo).unwrap();
                let shard_path = repo.join("index.shard");
                let sw = zoekt_rs::ShardWriter::new(&shard_path);
                sw.write_from_index(&idx).unwrap();
            }
            t0.elapsed()
        })
    });
}

criterion_group!(benches, shard_write_bench);
criterion_main!(benches);
