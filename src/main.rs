use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::error::Error;
use std::path::Path;
use structopt::StructOpt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest;
use std::collections::HashSet;
use rust_stemmers::{Algorithm, Stemmer};

const WORD_SPLITS: &[char] = &[' ', '\t', '\n', '\r', ',', '.', ';', ':', '!', '?', '(', ')', '[', ']', '{', '}', '<', '>', '"', '\''];
const MIN_WORD_LENGTH: usize = 5;
const BANNED: &str = "https://raw.githubusercontent.com/first20hours/google-10000-english/master/20k.txt";

type SearchResults<'a> = Vec<(&'a str, String, u32)>;

#[derive(StructOpt, Debug)]
#[structopt(name = "key-search")]
struct Opt {
    /// CSV file containing the hashmap key-value pairs
    #[structopt(short = "c", long = "csv")]
    csv_file: String,

    /// Text file(s) to search for keys
    #[structopt(short = "t", long = "text", parse(from_os_str))]
    text_files: Vec<std::path::PathBuf>,

    /// Context window size
    #[structopt(short = "w", long = "window", default_value = "250")]
    context_window: usize,
}

fn estimate_lines (file_path: &str) -> Result<usize, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    Ok(line_count)
}

struct StemmerWrapper {
    stemmer: Stemmer,
}

impl StemmerWrapper{
    pub fn new() -> StemmerWrapper {
        StemmerWrapper {
            stemmer: Stemmer::create(Algorithm::English),
        }
    }

    pub fn standardize(&self, word: &str) -> String {
        self.stemmer.stem(word.trim().to_lowercase().as_str()).to_string()
    }
}


fn to_ascii_titlecase(s: &str) -> String {
    let mut titlecased = s.to_owned();
    if let Some(r) = titlecased.get_mut(0..1) {
        r.make_ascii_uppercase();
    }
    titlecased
}

fn fetch_words_from_url(url: &str) -> Result<HashSet<String>, Box<dyn Error>> {
    let response = reqwest::blocking::get(url)?;
    let pb = ProgressBar::new(20000 as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("fetching common words [{elapsed_precise}] {bar} {pos}/{len} ({eta})")?
            .progress_chars("█░"),
    );
    let stemmer = StemmerWrapper::new();
    let words: HashSet<String> = response
        .text()?
        .split_whitespace()
        .filter(|word| !word.starts_with('#'))
        .map(|word| {
            pb.inc(1);
            stemmer.standardize(word)
        })
        .collect();
    pb.finish();
    Ok(words)
}

// Read CSV file and returns a HashMap with key-value pairs
fn parse_csv(file_path: &str, banned: &HashSet<String>) -> Result<HashMap<String, u32>, Box<dyn Error>> {
    let estimate = estimate_lines(file_path)?;
    let mut map = HashMap::with_capacity(estimate);
    let stemmer = StemmerWrapper::new();

    let content = fs::read_to_string(file_path)?;
    let mut skipped = 0;

    let pb = ProgressBar::new(estimate as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("building synonym map [{elapsed_precise}] {bar} {pos}/{len} ({eta})")?
            .progress_chars("█░"),
    );

    for line in content.lines() {
        let split: Vec<&str> = line.split('\t').collect();
        if split.len() == 2 {
            let value = split[0].trim().to_string();
            let key = split[1].trim().to_string();
            if key.len() >= MIN_WORD_LENGTH && !banned.contains(stemmer.standardize(&key).as_str()) {
                map.insert(to_ascii_titlecase(&key), value.parse::<u32>().unwrap());
            } else {
                skipped += 1;
            }
        }
        pb.inc(1);
    }
    pb.finish();

    println!("Skipped {} words", skipped);

    Ok(map)
}

// Read the text file and return its content as a String
fn read_text_file(file_path: &str) -> Result<String, Box<dyn Error>> {
    let content = fs::read_to_string(file_path)?;
    Ok(content)
}


// Find the hashmap keys in the input text and return a Vec with the character indices and associated values.
fn search_keys_in_text<'a>(map: &'a HashMap<String, u32>, text: &'a str, context_window: usize) -> SearchResults<'a> {
    let mut search_results = Vec::new();
    let mut count: usize = 0;
    text.split(WORD_SPLITS).map(|word| {
        count += word.len() + 1;
        let word = to_ascii_titlecase(word);
        if word.len() >= MIN_WORD_LENGTH && map.contains_key(&word) {
            let value = map.get(&word).unwrap();
            let index = count - word.len() - 1;
            let min = if index < context_window / 2 { 0 } else { index - context_window / 2 };
            let max = if index + context_window / 2 > text.len() { text.len() } else { index + context_window / 2 };
            search_results.push((&text[min..max], word.to_string(), *value));
        }
    }).count();

    search_results
}

// Generate the report in a readable format
fn generate_report(search_results: SearchResults, file_name: &str) -> String {
    let mut report = format!("Report for {}:\n", file_name);

    for (context, word, cid) in search_results {
        // show the context window around the word
        report.push_str(&format!("{} [{}] {}\n", word, cid, context));
    }

    report
}

fn main() {
    let opt = Opt::from_args();

    let banned = fetch_words_from_url(BANNED).unwrap();
    let map = match parse_csv(&opt.csv_file, &banned) {
        Ok(map) => map,
        Err(err) => {
            eprintln!("Error parsing CSV file: {}", err);
            return;
        }
    };

    for text_file in opt.text_files {
        let text = match read_text_file(text_file.to_str().unwrap()) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("Error reading text file: {}", err);
                continue;
            }
        };

        let search_results = search_keys_in_text(&map, &text, opt.context_window);
        let report = generate_report(search_results, Path::new(&text_file).file_name().unwrap().to_str().unwrap());

        println!("{}", report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standardize() {
        let stemmer = StemmerWrapper::new();
        let banned = fetch_words_from_url(BANNED).unwrap();
        assert!(banned.contains(stemmer.standardize("pathways").as_str()));
        assert!(!banned.contains(stemmer.standardize("Acetaminophen").as_str()));
    }

    #[test]
    fn test_parse_csv() {
        let content = "43\texample\n16\tworld";
        let mut banned = HashSet::new();
        banned.insert("exampl".to_string());
        let (dir, filename) = (std::env::temp_dir(), "test.csv");
        let file_path = dir.join(filename);
        fs::write(&file_path, content).unwrap();

        let map = parse_csv(file_path.to_str().unwrap(), &banned).unwrap();

        let mut expected_map = HashMap::new();
        //expected_map.insert("example".to_string(), "test".to_string());
        expected_map.insert("World".to_string(), 16);

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
        map.insert("Apple".to_string(), 1);
        map.insert("Orange".to_string(), 2);
        map.insert("Carrot".to_string(), 3);

        let text = "I have an apple and an orange, but I do not have a carrot.";
        let search_results = search_keys_in_text(&map, &text, 250);

        let expected_results = vec![
            (text, "Apple".to_string(), 1),
            (text, "Orange".to_string(), 2),
            (text, "Carrot".to_string(), 3),
        ];

        assert_eq!(search_results, expected_results);
    }
}