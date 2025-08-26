use crate::index;
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use log::{info, warn};
use notify::Watcher;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

pub fn run_watcher(
    root: PathBuf,
    debounce_ms: u64,
    tx: SyncSender<Vec<EntityPayload>>,
) -> Result<(), anyhow::Error> {
    let (watch_tx, watch_rx) = std::sync::mpsc::channel();
    let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
        move |res: notify::Result<notify::Event>| match res {
            Ok(ev) => {
                if watch_tx.send(ev).is_err() {
                    warn!("watch channel closed, dropping event");
                }
            }
            Err(e) => warn!("watch error: {}", e),
        },
        Config::default(),
    )?;
    watcher.watch(&root, RecursiveMode::Recursive)?;

    info!("Watching {} for changes...", root.display());

    let debounce_window = Duration::from_millis(debounce_ms);
    while let Ok(first_event) = watch_rx.recv() {
        let mut changed: HashSet<PathBuf> = HashSet::new();
        match first_event.kind {
            EventKind::Modify(_) | EventKind::Create(_) => {
                for path in first_event.paths.iter() {
                    if path.is_file() {
                        changed.insert(path.clone());
                    }
                }
            }
            _ => {}
        }

        let start = std::time::Instant::now();
        while start.elapsed() < debounce_window {
            match watch_rx.recv_timeout(debounce_window - start.elapsed()) {
                Ok(ev) => match ev.kind {
                    EventKind::Modify(_) | EventKind::Create(_) => {
                        for path in ev.paths.iter() {
                            if path.is_file() {
                                changed.insert(path.clone());
                            }
                        }
                    }
                    _ => {}
                },
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                Err(_) => break,
            }
        }

        if changed.is_empty() {
            continue;
        }

        for path in changed.into_iter() {
            if let Ok((file_payloads, _stats)) = index::index_single_file(&path) {
                if let Err(e) = tx.send(file_payloads) {
                    warn!("Failed to send file payloads to DB thread: {}", e);
                }
            }
        }
    }

    Ok(())
}
