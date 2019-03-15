use std::env;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::collections::BTreeMap;
use std::thread;
use std::sync::mpsc::channel;
use std::time::Instant;

use memmap::MmapOptions;
use getopts::Options;

struct WordCount(BTreeMap<u64, u32>);

impl WordCount {
    fn new() -> Self {
        WordCount(BTreeMap::new())
    }

    fn increment(&mut self, token: u64) {
        *self.0.entry(token).or_insert(0) += 1;
    }

    fn merge_from(&mut self, other: &Self) {
      for (word, c) in other.0.iter() {
          *self.0.entry(*word).or_insert(0) += c;
      }
    }

    fn serialize_counts(&self, output: &mut Write) {
        let mut wtr = BufWriter::new(output);
        for (word, c) in self.0.iter() {
            let _ = write!(wtr, "{}\t{}\n", word, c);
        }
    }

    fn count_file(&mut self, file: &mut Read, buffer_size: usize) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let rdr = BufReader::with_capacity(buffer_size, file);
        let mut hasher = DefaultHasher::new();
        for b in rdr.bytes() {
            let _ = b.map(|b| {
                if b >= 97 && b <= 122 || b >= 65 && b <= 90 {
                    hasher.write_u8(b);
                } else {
                  self.increment(hasher.finish());
                  hasher = DefaultHasher::new();
                }
            });
        }
    }

}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> =  env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("o", "", "set output file name", "NAME");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("t", "threads", "set number of threads to use", "NUM_THREADS");
    opts.optopt("b", "buf", "size of the per-thread buffer in KB", "BUF_SIZE");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return Ok(())
    }

    let output_file = matches.opt_str("o").unwrap_or("/dev/stdout".to_string());
    let file_names = if !matches.free.is_empty() {
        matches.free.clone()
    } else {
        print_usage(&program, opts);
        return Err(std::io::Error::from_raw_os_error(-1));
    };

    let num_threads = match matches.opt_get("t") {
        Ok(Some(t)) => t,
        _ => num_cpus::get(),
    };

    let match_b: Result<Option<usize>, _> = matches.opt_get("b");
    let buffer_size = match match_b {
        Ok(Some(b)) => b,
        _ => 1,
    };

    println!("Counting words from {} input files", file_names.len());
    println!("Using {} threads", num_threads);
    println!("Buffer size {}kB", buffer_size);
    let start_time = Instant::now();

    let (sender, receiver) = channel();
    for file_name in file_names {
        let file = fs::File::open(&file_name)?;
        let mapped_file = Box::leak(Box::new(unsafe { MmapOptions::new().map(&file)? }));
        let chunk_size = f64::ceil((mapped_file.len() as f64) / (num_threads as f64)) as usize;

        for (i, mut chunks) in mapped_file.chunks(chunk_size).enumerate() {
            let s = sender.clone();
            println!("{:?}\tStarting thread {}", start_time.elapsed(), i);
            thread::spawn(move || {
                let mut counts = WordCount::new();
                counts.count_file(&mut chunks, buffer_size * 1024);
                let _ = s.send(counts);
                println!("{:?}\tThread {} done", start_time.elapsed(), i);
            });
        }
    }
    drop(sender);

    let mut totals = WordCount::new();
    for counts in receiver.iter() {
        totals.merge_from(&counts);
    }

    let mut output = fs::File::create(output_file)?;
    totals.serialize_counts(&mut output);
    println!("{:?}\tDone", start_time.elapsed());
    Ok(())
}

