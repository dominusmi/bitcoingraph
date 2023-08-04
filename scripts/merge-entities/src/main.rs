use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::collections::HashSet;
use std::time::{Instant, Duration};

struct UnionFind {
    pub parent: HashMap<usize, usize>,
}

impl UnionFind {
    fn new() -> UnionFind {
        UnionFind {
            parent: HashMap::new(),
        }
    }

    fn union(&mut self, x: usize, y: usize) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent.insert(root_x, root_y);
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
        }

        if *self.parent.get(&x).unwrap() != x {
            let parent = *self.parent.get(&x).unwrap();
            let found = self.find(parent);
            self.parent.insert(x, found);
        }

        *self.parent.get(&x).unwrap()
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        return Err("Please specify the input and output CSV files".into());
    }

    let input_path = Path::new(&args[1]);
    let output_path = Path::new(&args[2]);
    let mut entity_path = PathBuf::from(output_path.clone());

    // Extract the parent directory of the original path
    if let Some(parent) = output_path.parent() {
        entity_path = parent.join("entity.csv");
    } else {
        eprintln!("Original path has no parent directory.");
        return Err(Box::from("Original path has no parent directory.".to_string()));
    }

    let input_file = File::open(&input_path)?;
    let reader = BufReader::new(input_file);

    let mut idx_map: HashMap<String, usize> = HashMap::new();
    let mut uf = UnionFind::new();
    let mut lines: Vec<(usize, String)> = Vec::new();

    let start_time = Instant::now();
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();

        if parts.len() != 2 {
            return Err("Invalid line format".into());
        }

        let idx: usize = parts[0].parse()?;
        let str = parts[1].to_string();

        if let Some(first_idx) = idx_map.get(&str) {
            uf.union(*first_idx, idx);
        } else {
            idx_map.insert(str.clone(), idx);
            lines.push((idx, str));
        }
        if index>0 && index % 10_000_000 == 0{
            println!("Processed {:?} lines in {:?} seconds", &index, &start_time.elapsed().as_secs());
        }
    }

    let output_file = File::create(&output_path)?;
    let mut writer = BufWriter::new(output_file);

    println!("Done processing lines. Writing new file.");
    let mut start_write = Instant::now();
    for (idx, str) in lines {
        let new_idx = uf.find(idx);
        write!(writer, "{},{}\n", new_idx, str)?;

        if idx > 0 && idx % 10_000_000 == 0 {
            println!("Wrote {:?} lines in {:?} seconds", &idx, &start_write.elapsed().as_secs());
        }
    }
    writer.flush()?;

    // Create a HashSet to store the unique values
    let mut unique_entity_idx: HashSet<usize> = HashSet::new();

    // Iterate over the values in the HashMap
    println!("Finished merging entities. Getting unique entities. Total time since start: {:?}", &start_time.elapsed().as_secs());
    for &value in uf.parent.values() {
        // Insert the value into the HashSet, which automatically handles duplicates
        unique_entity_idx.insert(value);
    }

    println!("Found a total of {:?} entities", &unique_entity_idx.len());

    let path: &Path = entity_path.as_ref();
    let entity_file  = File::create(path)?;
    let mut entity_writer = BufWriter::new(entity_file);
    println!("Finished getting unique entities. Writing entities.csv. Total time since start: {:?}", &start_time.elapsed().as_secs());
    start_write = Instant::now();
    for (idx, parent) in unique_entity_idx.iter().enumerate() {
        write!(entity_writer, "{}\n", parent)?;
        if idx > 0 && idx % 10_000_000 == 0{
            println!("Wrote {:?} lines in {:?} seconds", &idx, &start_write.elapsed().as_secs());
        }
    }
    entity_writer.flush()?;  // Ensure all data is written

    Ok(())
}
