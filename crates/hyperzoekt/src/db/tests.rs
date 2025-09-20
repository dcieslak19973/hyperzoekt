#[cfg(test)]
mod tests {
    use super::super::db::models::{Commit, RefKind, RefRecord};
    use super::super::db::{resolve_ref, InMemoryDb};
    use chrono::Utc;

    #[tokio::test]
    async fn test_resolve_ref_exact_and_candidates() {
        let mut db = InMemoryDb::new();
        // commit
        let c = Commit {
            id: "c1".into(),
            repo: "repo:foo".into(),
            parents: vec![],
            tree: None,
            author: None,
            committer: None,
            message: None,
            timestamp: None,
            indexed_at: None,
        };
        db.insert_commit(c);

        // branch
        let r = RefRecord {
            id: "repo:refs/heads/main".into(),
            repo: "repo:foo".into(),
            name: "refs/heads/main".into(),
            kind: RefKind::Branch,
            target: "c1".into(),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: None,
        };
        db.insert_ref(r);

        // resolve by short name
        let got = resolve_ref(&db, "repo:foo", "main")
            .await
            .expect("should resolve");
        assert_eq!(got, "c1");

        // resolve by full name
        let got2 = resolve_ref(&db, "repo:foo", "refs/heads/main")
            .await
            .expect("should resolve");
        assert_eq!(got2, "c1");
    }
}
