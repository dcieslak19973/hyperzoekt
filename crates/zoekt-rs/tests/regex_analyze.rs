use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

#[test]
fn regex_literal_prefilter() {
    let pf = prefilter_from_regex("fooBar");
    match pf {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        _ => panic!("expected conj"),
    }
}
