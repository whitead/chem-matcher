use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::error::Error;
use std::path::Path;
use aho_corasick::{AhoCorasick};
use structopt::StructOpt;
use indicatif::{ProgressBar, ProgressStyle};


#[derive(StructOpt, Debug)]
#[structopt(name = "key-search")]
struct Opt {
    /// CSV file containing the hashmap key-value pairs
    #[structopt(short = "c", long = "csv")]
    csv_file: String,

    /// Text file(s) to search for keys
    #[structopt(short = "t", long = "text", parse(from_os_str))]
    text_files: Vec<std::path::PathBuf>,
}


fn estimate_lines (file_path: &str) -> Result<usize, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    Ok(line_count)
}

// Read CSV file and returns a HashMap with key-value pairs
fn parse_csv(file_path: &str) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let estimate = estimate_lines(file_path)?;
    let mut map = HashMap::with_capacity(estimate);

    let content = fs::read_to_string(file_path)?;

    let pb = ProgressBar::new(estimate as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar} {pos}/{len} ({eta})")?
            .progress_chars("█░"),
    );

    for line in content.lines() {
        let split: Vec<&str> = line.split('\t').collect();
        if split.len() == 2 {
            let value = split[0].trim().to_string();
            let key = split[1].trim().to_string();
            map.insert(key, value);
        }
        pb.inc(1);
    }

    pb.finish();
    Ok(map)
}

// Read the text file and return its content as a String
fn read_text_file(file_path: &str) -> Result<String, Box<dyn Error>> {
    let content = fs::read_to_string(file_path)?;
    Ok(content)
}

// Find the hashmap keys in the input text and return a Vec with the character indices and associated values.
fn search_keys_in_text(ac: &AhoCorasick, map: &HashMap<String, String>, text: &str) -> Vec<(String, String)> {
    let patterns: Vec<&str> = map.keys().map(|key| key.as_str()).collect();

    let mut search_results = Vec::new();

    for mat in ac.find_iter(text) {
            let key = patterns[mat.pattern()];
            if let Some(value) = map.get(key) {
                search_results.push((key.to_string(), value.to_string()));
            }
        }

        //search_results.sort_by_key(|(index, _)| *index);

        search_results
}

// Generate the report in a readable format
fn generate_report(search_results: Vec<(String, String)>, file_name: &str) -> String {
    let mut report = format!("Report for {}:\n", file_name);

    for (index, value) in search_results {
        report.push_str(&format!("[{}]: {}\n", index, value));
    }

    report
}

fn main() {
    let opt = Opt::from_args();

    let map = match parse_csv(&opt.csv_file) {
        Ok(map) => map,
        Err(err) => {
            eprintln!("Error parsing CSV file: {}", err);
            return;
        }
    };

    let patterns: Vec<&str> = map.keys().map(|key| key.as_str()).collect();
    let ac = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build(&patterns)
        .unwrap();

    for text_file in opt.text_files {
        let text = match read_text_file(text_file.to_str().unwrap()) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("Error reading text file: {}", err);
                continue;
            }
        };

        let search_results = search_keys_in_text(&ac, &map, &text);
        let report = generate_report(search_results, Path::new(&text_file).file_name().unwrap().to_str().unwrap());

        println!("{}", report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv() {
        let content = "test\texample\nhello\tworld";
        let (dir, filename) = (std::env::temp_dir(), "test.csv");
        let file_path = dir.join(filename);
        fs::write(&file_path, content).unwrap();

        let map = parse_csv(file_path.to_str().unwrap()).unwrap();

        let mut expected_map = HashMap::new();
        expected_map.insert("example".to_string(), "test".to_string());
        expected_map.insert("world".to_string(), "hello".to_string());

        assert_eq!(map, expected_map);
    }

    #[test]
    fn test_read_text_file() {
        let content = "This is a test";
        let (dir, filename) = (std::env::temp_dir(), "text.txt");
        let file_path = dir.join(filename);
        fs::write(&file_path, content).unwrap();

        let text = read_text_file(file_path.to_str().unwrap()).unwrap();

        assert_eq!(text, content);
    }

    #[test]
    fn test_search_keys_in_text() {
        let mut map = HashMap::new();
        map.insert("apple".to_string(), "fruit".to_string());
        map.insert("orange".to_string(), "fruit".to_string());
        map.insert("carrot".to_string(), "vegetable".to_string());

        let patterns: Vec<&str> = map.keys().map(|key| key.as_str()).collect();
        let ac = AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .build(&patterns)
            .unwrap();

        let text = "I have an apple and an orange, but I do not have a carrot.";
        let search_results = search_keys_in_text(&ac, &map, &text);

        let expected_results = vec![
            (10, "fruit".to_string()),
            (23, "fruit".to_string()),
            (51, "vegetable".to_string()),
        ];

        assert_eq!(search_results, expected_results);
    }
}