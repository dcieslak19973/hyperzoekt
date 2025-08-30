use std::io::Cursor;
use zoekt_rs::test_helpers as th;

#[test]
fn test_write_var_u32_roundtrip() {
    let mut buf = vec![];
    th::test_write_var_u32(&mut buf, 0).unwrap();
    th::test_write_var_u32(&mut buf, 1).unwrap();
    th::test_write_var_u32(&mut buf, 300).unwrap();
    th::test_write_var_u32(&mut buf, std::u32::MAX).unwrap();
    // now read back
    let mut cursor = Cursor::new(buf);
    assert_eq!(th::test_read_var_u32(&mut cursor).unwrap(), 0);
    assert_eq!(th::test_read_var_u32(&mut cursor).unwrap(), 1);
    assert_eq!(th::test_read_var_u32(&mut cursor).unwrap(), 300);
    assert_eq!(th::test_read_var_u32(&mut cursor).unwrap(), std::u32::MAX);
}

#[test]
fn test_radix_sort_u128() {
    let mut v = vec![3u128, 1u128, 2u128, 0u128, std::u128::MAX];
    th::test_radix_sort_u128(&mut v);
    assert_eq!(v, vec![0u128, 1u128, 2u128, 3u128, std::u128::MAX]);
}
