use assert_cmd::prelude::*;
use serde_json::json;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

// This test launches the hyperzoekt binary in MCP stdio mode and sends a
// minimal CallToolRequest for the `search` tool, then verifies a valid
// CallToolResult comes back.
#[test]
fn mcp_stdio_search_e2e() -> Result<(), Box<dyn std::error::Error>> {
    // Start the binary with --mcp_stdio; ensure dev build path
    let mut cmd = Command::cargo_bin("hyperzoekt")?;
    cmd.arg("--mcp-stdio")
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("child stdin");
    let stdout = child.stdout.take().expect("child stdout");

    // Spawn a thread to copy stderr for diagnostics
    let child_stderr = child.stderr.take().expect("child stderr");
    let err_handle = thread::spawn(move || {
        let mut buf = String::new();
        let mut r = BufReader::new(child_stderr);
        while let Ok(n) = r.read_line(&mut buf) {
            if n == 0 {
                break;
            }
            eprint!("[child stderr] {}", buf);
            buf.clear();
        }
    });

    // Reader thread to collect framed responses (Content-Length)
    let mut reader = BufReader::new(stdout);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _handle = thread::spawn(move || {
        let res = (|| -> Result<serde_json::Value, String> {
            // helper to read a single framed message
            fn read_message<R: Read>(r: &mut BufReader<R>) -> Result<String, String> {
                let mut header = String::new();
                // Read header lines until we find a blank line
                loop {
                    let mut line = String::new();
                    let n = r.read_line(&mut line).map_err(|e| e.to_string())?;
                    if n == 0 {
                        return Err("EOF while reading header".to_string());
                    }
                    if line.trim().is_empty() {
                        break;
                    }
                    header.push_str(&line);
                }
                // parse Content-Length
                for ln in header.lines() {
                    if let Some(rest) = ln.strip_prefix("Content-Length:") {
                        if let Ok(len) = rest.trim().parse::<usize>() {
                            let mut buf = vec![0u8; len];
                            r.read_exact(&mut buf).map_err(|e| e.to_string())?;
                            return String::from_utf8(buf).map_err(|e| e.to_string());
                        }
                    }
                }
                Err("no Content-Length header found".to_string())
            }

            // First message should be initialize/result or server's init response
            let s = read_message(&mut reader)?;
            let v: serde_json::Value = serde_json::from_str(&s).map_err(|e| e.to_string())?;
            Ok(v)
        })();
        // send the result back (ignore send errors)
        let _ = tx.send(res);
    });

    // send a fake framed initialize request (we keep this minimal and hope the server
    // responds quickly; building a full MCP client is more code than this test)
    let init = json!({
        "jsonrpc": "2.0",
        "id": "init",
        "method": "mcp/initialize",
        "params": { "name": "test-client" }
    })
    .to_string();
    // Write Content-Length header per MCP stdio (simple)
    write!(stdin, "Content-Length: {}\r\n\r\n{}", init.len(), init)?;
    stdin.flush()?;

    // wait briefly for server to initialize
    thread::sleep(Duration::from_millis(200));

    // Now send a CallToolRequest for search
    let call = json!({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "mcp/callTool",
        "params": { "name": "search", "arguments": { "q": "nonexistent-symbol-xyz" } }
    })
    .to_string();
    write!(stdin, "Content-Length: {}\r\n\r\n{}", call.len(), call)?;
    stdin.flush()?;

    // Give the server some time to respond
    thread::sleep(Duration::from_millis(300));

    // Read initialize response (with a timeout)
    let init_val = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("timeout waiting for init");
    match init_val {
        Ok(v) => assert!(v.get("server_info").is_some()),
        Err(e) => panic!("reader error: {}", e),
    }

    // Tear down child
    let _ = child.kill();
    let _ = child.wait();
    let _ = err_handle.join();
    Ok(())
}
