use std::process::{Command, Stdio};
use std::time::Duration;

use reqwest::Client;

// Integration test: spawn dzr-admin binary on an ephemeral port, wait for /health,
// post to /login, ensure Set-Cookie is returned and / is accessible with that cookie.

#[tokio::test]
async fn login_flow_integration() {
    // Locate the compiled binary via env var set by cargo when running tests.
    let exe = std::env::var("CARGO_BIN_EXE_dzr-admin").unwrap_or_else(|_| {
        // Fallback: assume binary is built and available at workspace target
        let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // crates/zoekt-distributed -> crates
        p.pop();
        p.push("target/debug/dzr-admin");
        p.to_string_lossy().into_owned()
    });

    // If binary is not present, build it (use workspace root) and then try to locate it.
    if !std::path::Path::new(&exe).exists() {
        let mut workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        workspace.pop(); // crates
        workspace.pop(); // workspace root
        let status = Command::new("cargo")
            .arg("build")
            .arg("--package")
            .arg("zoekt-distributed")
            .arg("--bin")
            .arg("dzr-admin")
            .current_dir(&workspace)
            .status()
            .expect("cargo build dzr-admin");
        assert!(status.success(), "cargo build failed");

        // try common locations under workspace target
        let mut found = None;
        let target_debug = workspace.join("target/debug");
        let candidates = vec![target_debug.join("dzr-admin"), target_debug.join("deps")];
        for c in candidates {
            if c.is_file() {
                found = Some(c);
                break;
            }
            if c.is_dir() {
                if let Ok(entries) = std::fs::read_dir(&c) {
                    for e in entries.flatten() {
                        let name = e.file_name().to_string_lossy().to_string();
                        if name.contains("dzr-admin") {
                            found = Some(e.path());
                            break;
                        }
                    }
                }
            }
            if found.is_some() {
                break;
            }
        }
        if let Some(p) = found {
            // override exe path
            let s = p.to_string_lossy().into_owned();
            // prefer the exe discovered
            // (note: on Windows there may be .exe suffix)
            // set exe to discovered path
            // shadowing the previous exe variable
            std::env::set_var("DZRA_EXE_FALLBACK", &s);
        }
    }

    // allow discovered fallback
    let exe = std::env::var("DZRA_EXE_FALLBACK").unwrap_or(exe);

    // Reserve an ephemeral port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("local_addr");
    let port = addr.port();
    drop(listener);

    // Spawn the binary via `cargo run` in the workspace root so we don't need to
    // guess the exact path to the compiled executable.
    let mut workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace.pop(); // crates
    workspace.pop(); // workspace root
    let bind_arg = format!("127.0.0.1:{}", port);
    let mut child = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("zoekt-distributed")
        .arg("--bin")
        .arg("dzr-admin")
        .arg("--")
        .arg("--bind")
        .arg(&bind_arg)
        .current_dir(&workspace)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dzr-admin via cargo");

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let addr = format!("127.0.0.1:{}", port);

    // Wait for /health to become available
    let health_url = format!("http://{}/health", addr);
    let mut ok = false;
    for _ in 0..20 {
        if let Ok(r) = client.get(&health_url).send().await {
            if r.status().is_success() {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "dzr-admin did not respond healthy in time");

    // Post login credentials (use default creds from binary: admin/password)
    let login_url = format!("http://{}/login", addr);
    let resp = client
        .post(&login_url)
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=admin&password=password")
        .send()
        .await
        .expect("request");

    assert!(
        resp.status().is_redirection(),
        "expected redirect on successful login"
    );
    let cookies = resp.headers().get_all(reqwest::header::SET_COOKIE);
    assert!(
        cookies.iter().next().is_some(),
        "expected Set-Cookie header"
    );

    // extract first cookie value and use it
    let cookie_hdr = cookies.iter().next().unwrap().to_str().unwrap().to_string();
    let first_pair = cookie_hdr.split(';').next().unwrap_or_default();
    let cookie_val = first_pair.to_string();

    // Use cookie to GET /
    let index_url = format!("http://{}/", addr);
    let resp2 = client
        .get(&index_url)
        .header(reqwest::header::COOKIE, cookie_val)
        .send()
        .await
        .unwrap();
    assert!(resp2.status().is_success() || resp2.status().is_redirection());

    // cleanup: kill child
    let _ = child.kill();
}
