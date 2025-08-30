use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::tempdir;
use zoekt_rs::test_helpers::process_pending_from_paths;

#[test]
fn process_pending_text_and_binary() {
    let td = tempdir().unwrap();
    let base = td.path().to_path_buf();

    // text file
    let txt = PathBuf::from("foo.txt");
    let mut f = File::create(td.path().join(&txt)).unwrap();
    f.write_all(b"hello world\nfn main() {}").unwrap();

    // binary file
    let bin = PathBuf::from("bin.dat");
    let mut fb = File::create(td.path().join(&bin)).unwrap();
    fb.write_all(&[0u8, 1, 2, 0, 3, 4]).unwrap();

    let processed =
        process_pending_from_paths(base.clone(), vec![txt.clone(), bin.clone()], false, Some(1));
    assert_eq!(processed.len(), 2);
    let ptxt = processed.iter().find(|(path, _)| *path == txt).unwrap();
    assert!(ptxt.1, "text file should have content");
    let pbin = processed.iter().find(|(path, _)| *path == bin).unwrap();
    assert!(!pbin.1, "binary file should not have content");
}
