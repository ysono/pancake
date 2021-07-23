use anyhow::Result;
use std::io::Error as IOError;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamped_path(parent_path: &str) -> PathBuf {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    
    // Padding zeros on the left, to ensure that the filenames' alphanumerical order
    // is the same as their order when compared as numbers.
    // When using micros, the base-10 digit count will be 16 for the forseeable future.
    // Arbitrarily bump the digits to 18.
    // This may be useful on a system with misconfigured time.
    let filename = format!("{:0>18}.data", micros);

    let mut path = PathBuf::from(parent_path);
    path.push(filename);
    path
}

pub fn read_dir_sorted(parent_path: &str) -> Result<Vec<PathBuf>, IOError> {
    let dir_iter = std::fs::read_dir(parent_path)?;

    let paths_result: Result<Vec<_>, _> = dir_iter
        .map(|dir_entry_result| dir_entry_result.map(|dir_entry| dir_entry.path()))
        .collect();

    paths_result.map(|mut paths| {
        paths.sort();
        paths
    })
}
