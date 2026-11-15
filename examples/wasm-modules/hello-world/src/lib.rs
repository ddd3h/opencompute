//! Input: `String` name. Output: `String` greeting.
fn run(input: &[u8]) -> Vec<u8> {
    let name: String = postcard::from_bytes(input).expect("input must be a postcard String");
    postcard::to_stdvec(&format!("Hello, {name}!")).expect("encode")
}
opencompute_guest::export_task!(run);
