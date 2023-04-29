mod core;

fn main() {
    core::serdes::KeyValue::new("1".as_bytes(), "2".as_bytes());
}
