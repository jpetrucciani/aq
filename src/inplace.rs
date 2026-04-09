use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::AqError;

pub fn write_atomically(path: &Path, contents: &str) -> Result<(), AqError> {
    let permissions = match fs::metadata(path) {
        Ok(metadata) => Some(metadata.permissions()),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(AqError::io(Some(path.to_path_buf()), error)),
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temp_path = unique_temp_path(parent, path)?;

    let write_result = (|| -> Result<(), AqError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|error| AqError::io(Some(temp_path.clone()), error))?;
        if let Some(permissions) = permissions {
            file.set_permissions(permissions)
                .map_err(|error| AqError::io(Some(temp_path.clone()), error))?;
        }
        file.write_all(contents.as_bytes())
            .map_err(|error| AqError::io(Some(temp_path.clone()), error))?;
        file.sync_all()
            .map_err(|error| AqError::io(Some(temp_path.clone()), error))?;
        drop(file);

        fs::rename(&temp_path, path)
            .map_err(|error| AqError::io(Some(path.to_path_buf()), error))?;
        sync_parent_directory(parent)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

fn unique_temp_path(parent: &Path, path: &Path) -> Result<PathBuf, AqError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| AqError::message(format!("cannot rewrite path {:?}", path)))?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AqError::message(format!("system clock error: {error}")))?
        .as_nanos();
    let pid = std::process::id();

    for attempt in 0..128_u32 {
        let candidate = parent.join(format!(".{file_name}.aq-tmp-{pid}-{nanos}-{attempt}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(AqError::message(format!(
        "failed to allocate temporary path for {:?}",
        path
    )))
}

fn sync_parent_directory(parent: &Path) -> Result<(), AqError> {
    #[cfg(unix)]
    {
        let directory = std::fs::File::open(parent)
            .map_err(|error| AqError::io(Some(parent.to_path_buf()), error))?;
        directory
            .sync_all()
            .map_err(|error| AqError::io(Some(parent.to_path_buf()), error))?;
    }

    #[cfg(not(unix))]
    {
        let _ = parent;
    }

    Ok(())
}
