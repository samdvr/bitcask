mod core;

fn main() {
    core::serdes::KeyValue::new("1".as_bytes().to_vec(), "2".as_bytes().to_vec());
}
