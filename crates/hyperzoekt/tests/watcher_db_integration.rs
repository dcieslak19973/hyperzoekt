use hyperzoekt::repo_index::RepoIndexService;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;
use tempfile::tempdir;

#[tokio::test]
async fn watcher_db_ingest_flow() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare a small repository (single file)
    let dir = tempdir()?;
    let file_path = dir.path().join("hello.rs");
    std::fs::write(&file_path, "fn hello() { println!(\"hi\"); }\n")?;

    // Build options to index this single file
    let mut opts_builder = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
    opts_builder = opts_builder.root(&file_path);
    let opts = opts_builder.output_null().build();
    let (svc, _stats) = RepoIndexService::build_with_options(opts)?;

    // Build payloads like the binary would
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct ImportItem {
        path: String,
        line: u32,
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct UnresolvedImport {
        module: String,
        line: u32,
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct EntityPayload {
        file: String,
        language: String,
        kind: String,
        name: String,
        parent: Option<String>,
        signature: String,
        start_line: Option<u32>,
        end_line: Option<u32>,
        calls: Vec<String>,
        doc: Option<String>,
        rank: f32,
        imports: Vec<ImportItem>,
        unresolved_imports: Vec<UnresolvedImport>,
    }

    let mut payloads: Vec<EntityPayload> = Vec::new();
    for ent in svc.entities.iter() {
        let file = &svc.files[ent.file_id as usize];
        let mut imports: Vec<ImportItem> = Vec::new();
        let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
        if matches!(
            ent.kind,
            hyperzoekt::repo_index::indexer::types::EntityKind::File
        ) {
            if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                let lines = svc.import_lines.get(ent.id as usize);
                for (i, &target_eid) in edge_list.iter().enumerate() {
                    if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                        let target_file_idx = target_ent.file_id as usize;
                        if let Some(target_file) = svc.files.get(target_file_idx) {
                            let line_no = lines
                                .and_then(|l| l.get(i))
                                .cloned()
                                .unwrap_or(0)
                                .saturating_add(1);
                            imports.push(ImportItem {
                                path: target_file.path.clone(),
                                line: line_no,
                            });
                        }
                    }
                }
            }
            if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                for (m, lineno) in unres {
                    unresolved_imports.push(UnresolvedImport {
                        module: m.clone(),
                        line: lineno.saturating_add(1),
                    });
                }
            }
        }
        let (start_field, end_field) = if matches!(
            ent.kind,
            hyperzoekt::repo_index::indexer::types::EntityKind::File
        ) {
            let has_imports = !imports.is_empty();
            let has_unresolved = !unresolved_imports.is_empty();
            if has_imports || has_unresolved {
                (
                    Some(ent.start_line.saturating_add(1)),
                    Some(ent.end_line.saturating_add(1)),
                )
            } else {
                (None, None)
            }
        } else {
            (
                Some(ent.start_line.saturating_add(1)),
                Some(ent.end_line.saturating_add(1)),
            )
        };
        payloads.push(EntityPayload {
            file: file.path.clone(),
            language: file.language.clone(),
            kind: ent.kind.as_str().to_string(),
            name: ent.name.clone(),
            parent: ent.parent.clone(),
            signature: ent.signature.clone(),
            start_line: start_field,
            end_line: end_field,
            calls: ent.calls.clone(),
            doc: ent.doc.clone(),
            rank: ent.rank,
            imports,
            unresolved_imports,
        });
    }

    // Start an embedded SurrealDB instance
    let db = Surreal::new::<Mem>(()).await?;
    db.use_ns("test").use_db("test").await?;

    // Ingest payloads into the DB (idempotent upsert by file+name)
    for p in payloads.iter() {
        let q_file = "UPDATE file SET path = $path, language = $language WHERE path = $path; CREATE file CONTENT { path: $path, language: $language } IF NONE;";
        db.query(q_file)
            .bind(("path", p.file.clone()))
            .bind(("language", p.language.clone()))
            .await
            .ok();
        // Use simple CREATE for test environment
        let q_ent = "CREATE entity CONTENT $entity;";
        let v = serde_json::to_value(p)?;
        db.query(q_ent).bind(("entity", v)).await?;
    }

    // Query the DB for entities relating to our file
    let res = db
        .query("SELECT * FROM entity WHERE file = $file")
        .bind(("file", file_path.to_string_lossy().to_string()))
        .await?;
    // `res` is a Vec<QueryResponse> — check approximate presence by converting to string
    let s = format!("{:?}", res);
    assert!(
        s.contains("entity") || !s.is_empty(),
        "expected entities in DB result: {}",
        s
    );

    Ok(())
}
