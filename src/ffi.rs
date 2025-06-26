//! C-compatible Foreign Function Interface (FFI) for butterfly-dl
//!
//! This module provides C-compatible bindings that allow the butterfly-dl library
//! to be used from C, C++, Python (via ctypes), and other languages that support
//! calling C libraries.
//!
//! # Memory Management
//!
//! - All string parameters should be null-terminated C strings (char*)
//! - Returned strings are allocated by Rust and must be freed with `butterfly_free_string()`
//! - The library handles internal memory management for downloads
//!
//! # Error Handling
//!
//! All functions return a ButterflyResult code:
//! - 0: Success
//! - 1: Invalid parameter
//! - 2: Network error
//! - 3: I/O error  
//! - 4: Unknown error

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

/// Global async runtime for C FFI calls
static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create tokio runtime for FFI")
});

/// Result codes for C FFI
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum ButterflyResult {
    Success = 0,
    InvalidParameter = 1,
    NetworkError = 2,
    IoError = 3,
    UnknownError = 4,
}

/// Progress callback function type for C
pub type ProgressCallback = extern "C" fn(downloaded: u64, total: u64, user_data: *mut std::ffi::c_void);

/// Convert Rust Result to C result code
fn convert_error(result: crate::Result<()>) -> ButterflyResult {
    match result {
        Ok(()) => ButterflyResult::Success,
        Err(crate::Error::SourceNotFound(_)) | Err(crate::Error::InvalidInput(_)) => ButterflyResult::InvalidParameter,
        Err(crate::Error::NetworkError(_)) | Err(crate::Error::HttpError(_)) => ButterflyResult::NetworkError,
        Err(crate::Error::IoError(_)) => ButterflyResult::IoError,
        _ => ButterflyResult::UnknownError,
    }
}

/// Download a file (simple version)
///
/// # Parameters
/// - `source`: Source identifier (null-terminated string)
/// - `dest_path`: Destination file path (null-terminated string, or NULL for auto-generated)
///
/// # Returns
/// ButterflyResult code
#[no_mangle]
pub extern "C" fn butterfly_download(
    source: *const c_char,
    dest_path: *const c_char,
) -> ButterflyResult {
    // Validate input parameters
    if source.is_null() {
        return ButterflyResult::InvalidParameter;
    }

    // Convert C strings to Rust strings
    let source_str = match unsafe { CStr::from_ptr(source) }.to_str() {
        Ok(s) => s,
        Err(_) => return ButterflyResult::InvalidParameter,
    };

    let dest_str = if dest_path.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(dest_path) }.to_str() {
            Ok(s) => Some(s),
            Err(_) => return ButterflyResult::InvalidParameter,
        }
    };

    // Execute the download
    let result = RUNTIME.block_on(async {
        crate::get(source_str, dest_str).await
    });

    convert_error(result)
}

/// Download a file with progress callback
///
/// # Parameters
/// - `source`: Source identifier (null-terminated string)
/// - `dest_path`: Destination file path (null-terminated string, or NULL for auto-generated)
/// - `progress_callback`: Optional progress callback function
/// - `user_data`: User data pointer passed to progress callback
///
/// # Returns
/// ButterflyResult code
#[no_mangle]
pub extern "C" fn butterfly_download_with_progress(
    source: *const c_char,
    dest_path: *const c_char,
    progress_callback: Option<ProgressCallback>,
    user_data: *mut std::ffi::c_void,
) -> ButterflyResult {
    // Validate input parameters
    if source.is_null() {
        return ButterflyResult::InvalidParameter;
    }

    // Convert C strings to Rust strings
    let source_str = match unsafe { CStr::from_ptr(source) }.to_str() {
        Ok(s) => s,
        Err(_) => return ButterflyResult::InvalidParameter,
    };

    let dest_str = if dest_path.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(dest_path) }.to_str() {
            Ok(s) => Some(s),
            Err(_) => return ButterflyResult::InvalidParameter,
        }
    };

    // Execute the download with optional progress callback
    let result = if let Some(callback) = progress_callback {
        // Convert the raw pointer to an integer to make it Send + Sync
        // This is safe because the callback is extern "C" and the user_data
        // lifetime is guaranteed by the C caller
        let user_data_addr = user_data as usize;
        
        RUNTIME.block_on(async move {
            crate::get_with_progress(
                source_str,
                dest_str,
                move |downloaded, total| {
                    let user_data_ptr = user_data_addr as *mut std::ffi::c_void;
                    callback(downloaded, total, user_data_ptr);
                }
            ).await
        })
    } else {
        RUNTIME.block_on(async {
            crate::get(source_str, dest_str).await
        })
    };

    convert_error(result)
}

/// Get the auto-generated filename for a source
///
/// # Parameters
/// - `source`: Source identifier (null-terminated string)
///
/// # Returns
/// Allocated string that must be freed with `butterfly_free_string()`, or NULL on error
#[no_mangle]
pub extern "C" fn butterfly_get_filename(source: *const c_char) -> *mut c_char {
    if source.is_null() {
        return ptr::null_mut();
    }

    let source_str = match unsafe { CStr::from_ptr(source) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let filename = crate::core::resolve_output_filename(source_str);
    
    match CString::new(filename) {
        Ok(c_string) => c_string.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a string allocated by the library
///
/// # Parameters
/// - `ptr`: String pointer returned by library functions
#[no_mangle]
pub extern "C" fn butterfly_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Get library version string
///
/// # Returns
/// Static string with version information (does not need to be freed)
#[no_mangle]
pub extern "C" fn butterfly_version() -> *const c_char {
    use std::sync::OnceLock;
    static VERSION_STRING: OnceLock<std::ffi::CString> = OnceLock::new();
    
    VERSION_STRING.get_or_init(|| {
        std::ffi::CString::new(format!("butterfly-dl {}", env!("BUTTERFLY_VERSION")))
            .expect("Version string contains null byte")
    }).as_ptr()
}

/// Initialize the library (optional, called automatically)
///
/// This function is called automatically when needed, but can be called
/// explicitly to initialize the async runtime early.
///
/// # Returns
/// ButterflyResult::Success on success
#[no_mangle]
pub extern "C" fn butterfly_init() -> ButterflyResult {
    // Just access the runtime to ensure it's initialized
    Lazy::force(&RUNTIME);
    ButterflyResult::Success
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_butterfly_version() {
        let version = butterfly_version();
        let version_str = unsafe { CStr::from_ptr(version) }.to_str().unwrap();
        assert!(version_str.contains("butterfly-dl"));
        assert!(version_str.contains(env!("BUTTERFLY_VERSION")));
    }

    #[test]
    fn test_butterfly_get_filename() {
        let source = CString::new("europe/belgium").unwrap();
        let filename_ptr = butterfly_get_filename(source.as_ptr());
        assert!(!filename_ptr.is_null());
        
        let filename = unsafe { CStr::from_ptr(filename_ptr) }.to_str().unwrap();
        assert_eq!(filename, "belgium-latest.osm.pbf");
        
        butterfly_free_string(filename_ptr);
    }

    #[test]
    fn test_butterfly_init() {
        let result = butterfly_init();
        assert_eq!(result as u32, ButterflyResult::Success as u32);
    }

    #[test]
    fn test_invalid_parameters() {
        let result = butterfly_download(std::ptr::null(), std::ptr::null());
        assert_eq!(result as u32, ButterflyResult::InvalidParameter as u32);
    }
}