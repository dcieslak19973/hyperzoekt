use serde::Deserialize;

#[derive(Deserialize)]
struct LocalAppConfig {
    debug: Option<bool>,
    channel_capacity: Option<usize>,
}

#[test]
fn appconfig_load_from_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cfg_path = dir.path().join("test_config.toml");
    let toml = r#"
    debug = true
    out = ".data/test_out.jsonl"
    channel_capacity = 42
    "#;
    std::fs::write(&cfg_path, toml)?;
    let s = std::fs::read_to_string(&cfg_path)?;
    let cfg: LocalAppConfig = toml::from_str(&s)?;
    assert!(cfg.debug.unwrap_or(false));
    assert_eq!(cfg.channel_capacity.unwrap_or(0), 42);
    Ok(())
}
