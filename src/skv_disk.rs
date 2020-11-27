use std::collections::HashMap;

use libsimplekv::SimpleKV;

const USAGE: &str = "
Usage:
    skv_mem FILE get KEY
    skv_mem FILE delete KEY
    skv_mem FILE insert KEY VALUE
    skv_mem FILE update KEY VALUE
";

type ByteStr = [u8];
type ByteString = Vec<u8>;

fn store_index_on_disk(a: &mut SimpleKV, index_key: &ByteStr) {
    a.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&a.index).unwrap();
    a.index = std::collections::HashMap::new();
    a.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = std::path::Path::new(&fname);
    let mut store = SimpleKV::open(path).expect("unable to open file");

    store.load().expect("unable to load data");
    store_index_on_disk(&mut store, INDEX_KEY);

    match action {
        "get" => {
            // Two unwraps are required becase a.index is a HashMap that returns Option with Option values.
            let index_as_bytes = store.get(&INDEX_KEY).unwrap().unwrap();

            // Convert the on-disk representation to an in-memory representation.
            let index: HashMap<ByteString, u64> = bincode::deserialize(&index_as_bytes).unwrap();

            // Check if the key was actually been stored.
            match index.get(key) {
                None => eprintln!("{:?} not found", key),
                Some(value) => println!("{:?}", value),
            }
        }
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap()
        }
        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap()
        }
        _ => eprintln!("{}", &USAGE),
    }
}
