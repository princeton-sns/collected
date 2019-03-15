use std::env;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::collections::BTreeMap;
use std::thread;
use std::sync::mpsc::channel;

use memmap::MmapOptions;

struct WordCount(BTreeMap<String, u32>);

impl WordCount {
    fn new() -> Self {
        WordCount(BTreeMap::new())
    }

    fn increment(&mut self, token: String) {
        *self.0.entry(token).or_insert(0) += 1;
    }

    fn merge_from(&mut self, other: &Self) {
      for (word, c) in other.0.iter() {
          *self.0.entry(word.to_string()).or_insert(0) += c;
      }
    }

    fn serialize_counts(&self, output: &mut Write) {
        let mut wtr = BufWriter::new(output);
        for (word, c) in self.0.iter() {
            let _ = write!(wtr, "{}\t{}\n", word, c);
        }
    }

    fn count_file(&mut self, file: &mut Read) {
        let rdr = BufReader::new(file);
        for line in rdr.lines() {
            match line {
                Ok(line_) => {
                    for token in line_.split(|c: char| !c.is_alphanumeric()) {
                        // Filter out multiple spaces delimiting to empty strings.
                        if token.len() > 0 {
                            self.increment(token.to_owned());
                        }
                    }
                }
                Err(e) => {
                    println!("Error reading file: {}", e);
                    panic!("Error!");
                }
            }
        }
    }

}

fn main() -> std::io::Result<()> {
    let num_cpus = num_cpus::get();

    let mut file_names = env::args();
    if file_names.next().is_none() {
        println!("wordcount OUTPUT_FILE INPUT_FILE [INPUT_FILEs...]");
        std::process::exit(1);
    }
    let output_file = match file_names.next() {
        Some(o) => o,
        None => {
            println!("wordcount OUTPUT_FILE [INPUT_FILES...]");
            std::process::exit(1);
        }
    };

    let (sender, receiver) = channel();
    for file_name in file_names {
        let file = fs::File::open(&file_name)?;
        let mapped_file = Box::leak(Box::new(unsafe { MmapOptions::new().map(&file)? }));
        let chunk_size = mapped_file.len() / num_cpus;

        for mut chunks in mapped_file.chunks(chunk_size) {
            let s = sender.clone();
            thread::spawn(move || {
                let mut counts = WordCount::new();
                counts.count_file(&mut chunks);
                let _ = s.send(counts);
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
    Ok(())
}

