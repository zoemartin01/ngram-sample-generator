mod file_cache;

use clickrs::command;
use file_cache::FileCache;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::RowAccessor;
use std::fs;
use std::fs::create_dir;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug)]
struct NgramIndex {
    file: String,
    start_in_file: u64,
}

const DEFAULT_CHUNK_SIZE: u64 = 2500000;

#[command(name = "index")]
#[argument("parquet", short = "p", long, parse(from_os_str))]
#[argument("chunk_size", short = "s", long)]
#[argument("output", short = "o", long, parse(from_os_str))]
#[argument("cont", name = "continue", short = "c", long, parse(from_flag))]
#[argument("ngrams", short = "n", long, parse(from_os_str))]
fn main(
    parquet: PathBuf,
    chunk_size: Option<u64>,
    output: PathBuf,
    cont: bool,
    ngrams: Option<PathBuf>,
) {
    let chunk_size: usize = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE) as usize;

    if !cont {
        fs::remove_dir_all(&output).unwrap();
    }

    if !output.exists() {
        create_dir(&output).unwrap();
    }

    for n in 1..6 {
        if !output.join(n.to_string()).exists() {
            create_dir(output.join(n.to_string())).unwrap();
        }

        let count: u64;

        let file = File::open(parquet.clone().join(format!("{}.parquet", n)))
            .expect("Unable to open file");

        let reader = SerializedFileReader::new(file).unwrap();

        count = reader.metadata().file_metadata().num_rows() as u64;

        println!("{}-grams: {}", n, count);
        let mut buffer = FileCache::new();

        for offset in (0..count).step_by(chunk_size) {
            let out_path = output.join(n.to_string()).join(format!(
                "{}-{:05}-of-{:05}",
                n,
                offset / chunk_size as u64,
                count / (chunk_size as u64) + 1
            ));

            if cont && out_path.exists() {
                continue;
            }

            println!("Loading chunk {}", offset / chunk_size as u64);
            let file = File::open(parquet.clone().join(format!("{}.parquet", n)))
                .expect("Unable to open file");

            let reader = SerializedFileReader::new(file).unwrap();

            let results = reader
                .into_iter()
                .skip(offset as usize)
                .take(chunk_size)
                .map(|x| {
                    let columns = x.unwrap();
                    NgramIndex {
                        file: columns.get_string(0).unwrap().to_string(),
                        start_in_file: columns.get_long(1).unwrap() as u64,
                    }
                })
                .map(|x| {
                    let path = match ngrams {
                        Some(ref ngrams) => ngrams.join(&x.file).display().to_string(),
                        None => x.file,
                    };
                    buffer.get(&path, x.start_in_file)
                })
                .collect::<Vec<_>>()
                .join("");

            fs::write(out_path, results).unwrap();

            println!("Done with chunk {}", offset / chunk_size as u64);
        }
    }
}
