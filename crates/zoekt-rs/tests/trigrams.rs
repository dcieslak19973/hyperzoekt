use zoekt_rs::trigrams;

#[test]
fn basic_trigrams() {
    let v: Vec<[u8; 3]> = trigrams("Hello_Zoekt1").collect();
    // Expect some trigrams like "hel", "ell", ...
    assert!(v.contains(&*b"hel"))
}
