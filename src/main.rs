use std::sync::Arc;
use std::fs::{self, File, read_to_string};
use std::io::{BufRead, BufReader, BufWriter};
use std::error::Error;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest;
use std::collections::{HashSet, HashMap};
use rust_stemmers::{Algorithm, Stemmer};
use tokio;
use flume;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use std::io::prelude::*;
use regex;
use tempdir::TempDir;
use std::process;

const WORD_SPLITS: &[char] = &[' ', '\t', '\n', '\r', ',', '.', ';', ':', '!', '?', '(', ')', '[', ']', '{', '}', '<', '>', '"', '\''];
const MIN_WORD_LENGTH: usize = 5;
const BANNED: &str = "https://raw.githubusercontent.com/first20hours/google-10000-english/master/20k.txt";
const MASK: &str = "<|MOLECULE|>";

type SearchResults = Vec<(String, String, u32)>;

#[derive(StructOpt, Debug)]
#[structopt(name = "key-search")]
struct Opt {
    ///CSV file containing the JSON key-value pairs
    #[structopt(short = "c", long = "csv")]
    csv_file: String,

    /// Files (text or gzipped JSON) to search for keys
    #[structopt(short = "f", long = "files", parse(from_os_str))]
    files: Vec<std::path::PathBuf>,

    //Output file to write results
    #[structopt(short = "o", long = "output")]
    output_file: String,

    //context_window_prop_name
    #[structopt(short = "p", long = "property", default_value = "text")]
    property: String,

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

fn from_ascii_titlecase(s: &str) -> String {
    let mut titlecased = s.to_owned();
    if let Some(r) = titlecased.get_mut(0..1) {
        r.make_ascii_lowercase();
    }
    titlecased
}

async fn fetch_words_from_url(url: &str) -> Result<HashSet<String>, Box<dyn Error>> {
    let response = reqwest::get(url).await?;
    let pb = ProgressBar::new(20000 as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("fetching common words [{elapsed_precise}] {bar} {pos}/{len} ({eta})")?
            .progress_chars("█░"),
    );
    let stemmer = StemmerWrapper::new();
    let words: HashSet<String> = response
        .text()
        .await?
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


fn search_keys_in_text<'a>(map: &'a HashMap<String, u32>, text: &'a str) -> SearchResults {
    let mut search_results = Vec::new();
    let re = regex::Regex::new(r"\n\n").unwrap();
    re.split(text).map(|paragraph| {
        let mut count: usize = 0;
        let mut last_word = String::new();
        let mut last_count: usize = 0;
        let mut last_key = String::new();
        let mut seen = HashSet::new(); // we only want to observer a key once
        paragraph.split(WORD_SPLITS).map(|word| {
            count += word.len() + 1;
            let title_word = to_ascii_titlecase(word);
            let mut value: Option<&u32> = None;
            last_key.clear();
            last_key.push_str(&last_word);
            last_key.push(' ');
            last_key.push_str(word);
            println!("Considering: {} ({})", last_key, word);
            if word.len() >= MIN_WORD_LENGTH && map.contains_key(&last_key) && !seen.contains(&last_key) {
                value = map.get(&last_key);
            } else if last_word.len() >= MIN_WORD_LENGTH && map.contains_key(&last_word) && !seen.contains(&last_word) {
                value = map.get(&last_word);
                last_key.clear();
                last_key.push_str(&last_word);
            }
            
            if value.is_some() {
                // need to copy paragraph so I can mask out the word
                let mut paragraph = paragraph.to_string().replace(&last_key, MASK);
                paragraph = paragraph.replace(from_ascii_titlecase(&last_key).as_str(), MASK);
                seen.insert(last_key.to_string());
                println!("Found: {} ({})", last_key, word);
                println!("Replacing: {}", paragraph);
                search_results.push((paragraph, last_key.to_string(), *value.unwrap()));
            }
    
            last_word = title_word.to_string();
            last_count = count;
        }).count();

        // add the last word
        if last_word.len() >= MIN_WORD_LENGTH && map.contains_key(&last_word) && !seen.contains(&last_word) {
            let value = map.get(&last_word);
            if value.is_some() {
                // need to copy paragraph so I can mask out the word
                let mut paragraph = paragraph.to_string().replace(&last_word, MASK);
                paragraph = paragraph.replace(from_ascii_titlecase(&last_word).as_str(), MASK);
                seen.insert(last_word.to_string());
                search_results.push((paragraph.replace(&last_word, MASK), last_word.to_string(), *value.unwrap()));
            }
        }

    }).count();

    search_results
}


// Generate the report in a readable format
fn generate_report(search_results: SearchResults, writer: &mut BufWriter<File>, paper_id: &str) {
    for (context, word, cid) in search_results {
        // show the context window around the word
        let msg = format!("\"{}\",{},\"{}\",{}\n", word, cid, context.replace("\"", "\\\""), paper_id);
        writer.write_all(msg.as_bytes()).unwrap();
    }
}

async fn process_files(opt: Opt) -> Result<(), Box<dyn Error>> {
    let banned = Arc::new(fetch_words_from_url(BANNED).await.unwrap());
    let map = Arc::new(parse_csv(&opt.csv_file, &banned)?);
    let (tx, rx) = flume::unbounded();

    for (index, file_path) in opt.files.iter().enumerate() {
        let property = opt.property.clone();
        let fp = file_path.to_str().unwrap().to_string();
        let map: Arc<HashMap<String, u32>> = Arc::clone(&map);
        let tx = tx.clone();
        let output_file = opt.output_file.clone();
        tokio::spawn(async move {
            let ext = Path::new(&fp).extension().unwrap();
            let mut text: String;
            let ofp = format!("{}_{}", output_file, &index.to_string());
            let output_path = Path::new(&ofp);
            let mut writer = BufWriter::new(File::create(output_path).unwrap());
            match ext.to_str().unwrap() {
                "txt" => {
                    text = fs::read_to_string(&fp).unwrap();
                    let search_result = search_keys_in_text(&*map, &text);
                    generate_report(search_result, &mut writer, "");
                },
                "gz" => {
                    // TODO: WHY IS IT ALL LOADING INTO RAM??
                    let gz = BufReader::new(GzDecoder::new(File::open(&fp).unwrap()));
                    let mut count = 0;
                    for line in gz.lines() {
                        if count == 1000 {
                            break;
                        }
                        // skip empty lines
                        if line.as_ref().unwrap().is_empty() {
                            println!("echo \"{}\" >> {}", line.unwrap(), &ofp);
                            continue;
                        }
                        match serde_json::from_str::<serde_json::Value>(&line.unwrap()) {
                            Ok(json_data) => {
                                //print out json_data attributes
                                match json_data["content"][&property].as_str() {
                                    Some(t) => { text = t.to_string(); },
                                    None => { continue; }
                                }
                                let corpus_id  = match json_data["corpusid"].as_u64() {
                                    Some(t) => { t },
                                    None => {
                                        println!("{}", json_data.to_string());
                                        println!("Error: corpusid not found"); 
                                        process::exit(1);
                                        //continue; 
                                    }
                                };
                                let search_result = search_keys_in_text(&*map, &text);
                                generate_report(search_result, &mut writer, &corpus_id.to_string());
                                count += 1;
                            },
                            Err(e) => {
                                println!("Error: {}", e);
                                continue;
                            }
                        }
                    }
                },
                _ => { panic!("Unsupported file type") }
            }
            writer.flush().unwrap();
            tx.send(ofp).unwrap();
        });
    }

    drop(tx);

    // concat all files
    let mut writer = BufWriter::new(File::create(&opt.output_file).unwrap());
    for file_path in rx.iter() {
        let content = fs::read_to_string(&file_path).unwrap();
        writer.write_all(content.as_bytes()).unwrap();
        fs::remove_file(file_path).unwrap();
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    process_files(opt).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_standardize() {
        let stemmer = StemmerWrapper::new();
        let banned = fetch_words_from_url(BANNED).await.unwrap();
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
    fn test_search_keys_in_text() {
        let mut map = HashMap::new();
        map.insert("Apple".to_string(), 1);
        map.insert("Orange".to_string(), 2);
        map.insert("Carrot".to_string(), 3);

        let text = "I have an apple and an orange, but I do not have a carrot.";
        let search_results = search_keys_in_text(&map, &text);

        let expected_results = vec![
            ("I have an <|MOLECULE|> and an orange, but I do not have a carrot.".to_string(), "Apple".to_string(), 1),
            ("I have an apple and an <|MOLECULE|>, but I do not have a carrot.".to_string(), "Orange".to_string(), 2),
            ("I have an apple and an orange, but I do not have a <|MOLECULE|>.".to_string(), "Carrot".to_string(), 3),
        ];

        assert_eq!(search_results, expected_results);
    }

    #[test]
    fn test_search_keys_in_text_cases() {
        let mut map = HashMap::new();
        map.insert("Apple juice".to_string(), 1);
        map.insert("ORANGE".to_string(), 2);
        map.insert("Carrot".to_string(), 3);
        map.insert("juice".to_string(), 4);
        map.insert("Apple".to_string(), 5);

        let text = "I have an apple juice and an ORANGE, but I do not have a CARROT. Apple";
        let search_results = search_keys_in_text(&map, &text);

        let expected_results = vec![
            ("I have an <|MOLECULE|> and an ORANGE, but I do not have a CARROT. Apple".to_string(), "Apple juice".to_string(), 1),
            ("I have an apple juice and an <|MOLECULE|>, but I do not have a CARROT. Apple".to_string(), "ORANGE".to_string(), 2),
            ("I have an <|MOLECULE|> juice and an ORANGE, but I do not have a CARROT. <|MOLECULE|>".to_string(), "Apple".to_string(), 5),
        ];

        assert_eq!(search_results, expected_results);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_gz_json_file() {
        let csv_content = "43\tPhenol peroxidase\n16\texample";
        let textf_content =
            r#"{"corpusid": 533, "content": {"text": "this is a Phenol peroxidase of \"json\"", "title": "example title", "abstract": "example abstract"}}
            {"corpusid": 435, "content": {"text": "this is example 2 of json", "title": "example title", "abstract": "example abstract"}}"#;

        let tmp_dir = TempDir::new("rs_temp_dir").unwrap();
        let csv_filename = tmp_dir.path().join("test.csv");
        let text_filename = tmp_dir.path().join("text.json.gz");

        let text_filename_str = text_filename.to_str().unwrap();
        fs::write(&csv_filename, csv_content).unwrap();

        let file = File::create(text_filename_str).unwrap();
        let enc = GzEncoder::new(file, Compression::fast());
        {
            let mut writer = BufWriter::new(enc);
            write!(writer, "{}", textf_content).unwrap();
        }

        let opt = Opt {
            csv_file: csv_filename.to_str().unwrap().to_string(),
            files: vec![PathBuf::from(text_filename_str)],
            output_file: "output.txt".to_string(),
            property: "text".to_string(),
        };
        let result = process_files(opt).await;
        assert!(result.is_ok());
        assert!(read_to_string("output.txt").is_ok());
        assert_eq!(read_to_string("output.txt").unwrap(), "\"Phenol peroxidase\",43,\"this is a <|MOLECULE|> of \\\"json\\\"\",533\n");
        //clean-up
        fs::remove_file("output.txt").unwrap();
    }
}