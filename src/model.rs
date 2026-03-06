use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    pub relative_path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime_sec: i64,
    pub uid: u32,
    pub gid: u32,
    pub file_hash: String,
    pub total_chunks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub root: String,
    pub chunk_size: usize,
    pub files: Vec<FileManifest>,
    pub total_bytes: u64,
}
