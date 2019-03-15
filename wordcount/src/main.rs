use std::env;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::collections::BTreeMap;
use std::thread;
use std::sync::mpsc::channel;

fn count_words_in_file(file: fs::File, counts: &mut BTreeMap<String, u32>) {
    let rdr = BufReader::new(file);
    for line in rdr.lines() {
        match line {
            Ok(line_) => {
                for token in line_.split(|c: char| !c.is_alphanumeric()) {
                    // Filter out multiple spaces delimiting to empty strings.
                    if token.len() > 0 {
                        *counts.entry(token.to_owned()).or_insert(0) += 1;
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

fn serialize_counts(output: fs::File, totals: BTreeMap<String, u32>) {
    let mut wtr = BufWriter::new(output);
    for (word, c) in totals.iter() {
        let _ = write!(wtr, "{}\t{}\n", word, c);
    }
}

fn main() {
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
        let s = sender.clone();
        thread::spawn(move || {
            let file = fs::File::open(file_name).expect("Unable to open file");
            let mut counts = BTreeMap::new();
            count_words_in_file(file, &mut counts);
            let _ = s.send(counts);
        });
    }
    drop(sender);

    let mut totals = BTreeMap::new();
    for counts in receiver.iter() {
        for (word, c) in counts {
            *totals.entry(word).or_insert(0) += c;
        }
    }

    let output = fs::File::create(output_file).expect("couldn't open output file");
    serialize_counts(output, totals);
}
