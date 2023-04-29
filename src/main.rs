mod core;

fn main() {
    let a = core::serdes::KeyValue::new(b"1", b"2");
    println!("{:?}", a)
}
