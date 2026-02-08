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
//!
//! For detailed error messages, call `butterfly_last_error_message()` after any
//! non-success result. The returned string must be freed with `butterfly_free_string()`.
//!
//! # Threading Model
//!
//! The library uses a single global Tokio runtime shared across all FFI calls.
//! Each call to `butterfly_download` or `butterfly_download_with_progress` blocks
//! the calling thread via `Runtime::block_on()` until the operation completes.
//! This is safe for concurrent calls from multiple C threads — each thread blocks
//! independently while the runtime's thread pool handles async I/O. The only
//! restriction is that these functions must NOT be called from within an existing
//! Tokio runtime context (which C callers do not do).

use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use tokio::runtime::Runtime;

// Thread-local storage for the last error message.
// After any FFI call returns a non-success code, the detailed error string
// is available via `butterfly_last_error_message()`.
thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Store an error message in thread-local storage for later retrieval.
fn set_last_error(msg: String) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(msg);
    });
}

/// Clear the last error message.
fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Global async runtime for C FFI calls.
///
/// Initialized lazily on first use. If runtime creation fails (extremely rare —
/// would require OS-level resource exhaustion), all FFI functions will return
/// `ButterflyResult::UnknownError` rather than panicking across the FFI boundary.
static RUNTIME: Lazy<Option<Runtime>> = Lazy::new(|| Runtime::new().ok());

/// Get a reference to the runtime, or set an error and return None.
fn get_runtime() -> Option<&'static Runtime> {
    match RUNTIME.as_ref() {
        Some(rt) => Some(rt),
        None => {
            set_last_error("Failed to initialize async runtime".to_string());
            None
        }
    }
}

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
pub type ProgressCallback =
    extern "C" fn(downloaded: u64, total: u64, user_data: *mut std::ffi::c_void);

/// Convert Rust Result to C result code, storing the detailed error message
/// in thread-local storage for retrieval via `butterfly_last_error_message()`.
fn convert_error(result: crate::Result<()>) -> ButterflyResult {
    match result {
        Ok(()) => {
            clear_last_error();
            ButterflyResult::Success
        }
        Err(ref e) => {
            // Store the full error Display output for detailed retrieval
            set_last_error(e.to_string());
            match e {
                crate::Error::SourceNotFound(_) | crate::Error::InvalidInput(_) => {
                    ButterflyResult::InvalidParameter
                }
                crate::Error::NetworkError(_) | crate::Error::HttpError(_) => {
                    ButterflyResult::NetworkError
                }
                crate::Error::IoError(_) => ButterflyResult::IoError,
                _ => ButterflyResult::UnknownError,
            }
        }
    }
}

/// Get the last error message from the most recent FFI call.
///
/// Returns a detailed error string that must be freed with `butterfly_free_string()`,
/// or NULL if no error has occurred on this thread.
///
/// # Example (C)
/// ```c
/// ButterflyResult res = butterfly_download("europe/belgium", NULL);
/// if (res != BUTTERFLY_SUCCESS) {
///     char* msg = butterfly_last_error_message();
///     if (msg) {
///         fprintf(stderr, "Error: %s\n", msg);
///         butterfly_free_string(msg);
///     }
/// }
/// ```
#[no_mangle]
pub extern "C" fn butterfly_last_error_message() -> *mut c_char {
    LAST_ERROR.with(|cell| {
        let borrow = cell.borrow();
        match borrow.as_ref() {
            Some(msg) => match CString::new(msg.as_str()) {
                Ok(c_string) => c_string.into_raw(),
                Err(_) => ptr::null_mut(),
            },
            None => ptr::null_mut(),
        }
    })
}

/// Download a file (simple version)
///
/// # Parameters
/// - `source`: Source identifier (null-terminated string)
/// - `dest_path`: Destination file path (null-terminated string, or NULL for auto-generated)
///
/// # Returns
/// ButterflyResult code. On non-success, call `butterfly_last_error_message()` for details.
///
/// # Safety
///
/// - `source` must be a valid, null-terminated C string or NULL (returns InvalidParameter).
/// - `dest_path` must be a valid, null-terminated C string or NULL (auto-generated name).
#[no_mangle]
pub unsafe extern "C" fn butterfly_download(
    source: *const c_char,
    dest_path: *const c_char,
) -> ButterflyResult {
    // catch_unwind prevents any panic from unwinding across the FFI boundary (UB).
    let result = std::panic::catch_unwind(|| {
        if source.is_null() {
            set_last_error("source parameter is NULL".to_string());
            return ButterflyResult::InvalidParameter;
        }

        let source_str = match unsafe { CStr::from_ptr(source) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("source parameter is not valid UTF-8".to_string());
                return ButterflyResult::InvalidParameter;
            }
        };

        let dest_str = if dest_path.is_null() {
            None
        } else {
            match unsafe { CStr::from_ptr(dest_path) }.to_str() {
                Ok(s) => Some(s),
                Err(_) => {
                    set_last_error("dest_path parameter is not valid UTF-8".to_string());
                    return ButterflyResult::InvalidParameter;
                }
            }
        };

        let rt = match get_runtime() {
            Some(rt) => rt,
            None => return ButterflyResult::UnknownError,
        };

        let result = rt.block_on(async { crate::get(source_str, dest_str).await });
        convert_error(result)
    });

    result.unwrap_or_else(|_| {
        set_last_error("internal panic caught in butterfly_download".to_string());
        ButterflyResult::UnknownError
    })
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
/// ButterflyResult code. On non-success, call `butterfly_last_error_message()` for details.
///
/// # Safety
///
/// - `source` must be a valid, null-terminated C string or NULL (returns InvalidParameter).
/// - `dest_path` must be a valid, null-terminated C string or NULL (auto-generated name).
/// - `user_data` must remain valid for the duration of the call. This is guaranteed because
///   `block_on()` blocks the calling thread until the download completes — the C caller's
///   stack frame (and thus `user_data`) remains alive for the entire operation.
/// - `progress_callback`, if provided, must be safe to call from any thread.
#[no_mangle]
pub unsafe extern "C" fn butterfly_download_with_progress(
    source: *const c_char,
    dest_path: *const c_char,
    progress_callback: Option<ProgressCallback>,
    user_data: *mut std::ffi::c_void,
) -> ButterflyResult {
    // catch_unwind prevents any panic from unwinding across the FFI boundary (UB).
    let result = std::panic::catch_unwind(|| {
        if source.is_null() {
            set_last_error("source parameter is NULL".to_string());
            return ButterflyResult::InvalidParameter;
        }

        let source_str = match unsafe { CStr::from_ptr(source) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("source parameter is not valid UTF-8".to_string());
                return ButterflyResult::InvalidParameter;
            }
        };

        let dest_str = if dest_path.is_null() {
            None
        } else {
            match unsafe { CStr::from_ptr(dest_path) }.to_str() {
                Ok(s) => Some(s),
                Err(_) => {
                    set_last_error("dest_path parameter is not valid UTF-8".to_string());
                    return ButterflyResult::InvalidParameter;
                }
            }
        };

        let rt = match get_runtime() {
            Some(rt) => rt,
            None => return ButterflyResult::UnknownError,
        };

        let result = if let Some(callback) = progress_callback {
            // SAFETY: `user_data` is converted to usize to satisfy Send + Sync bounds
            // on the async block. This is safe because `block_on()` blocks the calling
            // C thread until the download completes, guaranteeing that `user_data`
            // remains valid for the entire lifetime of this closure. The C caller
            // cannot free `user_data` until this function returns.
            let user_data_addr = user_data as usize;

            rt.block_on(async move {
                crate::get_with_progress(source_str, dest_str, move |downloaded, total| {
                    let user_data_ptr = user_data_addr as *mut std::ffi::c_void;
                    callback(downloaded, total, user_data_ptr);
                })
                .await
            })
        } else {
            rt.block_on(async { crate::get(source_str, dest_str).await })
        };

        convert_error(result)
    });

    result.unwrap_or_else(|_| {
        set_last_error("internal panic caught in butterfly_download_with_progress".to_string());
        ButterflyResult::UnknownError
    })
}

/// Get the auto-generated filename for a source
///
/// # Parameters
/// - `source`: Source identifier (null-terminated string)
///
/// # Returns
/// Allocated string that must be freed with `butterfly_free_string()`, or NULL on error
///
/// # Safety
///
/// - `source` must be a valid, null-terminated C string or NULL (returns NULL).
#[no_mangle]
pub unsafe extern "C" fn butterfly_get_filename(source: *const c_char) -> *mut c_char {
    let result = std::panic::catch_unwind(|| {
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
    });

    result.unwrap_or(ptr::null_mut())
}

/// Free a string allocated by the library
///
/// # Parameters
/// - `ptr`: String pointer returned by library functions
///
/// # Safety
///
/// - `ptr` must be a pointer previously returned by a butterfly_* function,
///   or NULL (which is safely ignored).
#[no_mangle]
pub unsafe extern "C" fn butterfly_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Get library version string
///
/// # Returns
/// Static string with version information (does not need to be freed).
/// Returns NULL only if the version string contains a null byte (should never happen).
#[no_mangle]
pub extern "C" fn butterfly_version() -> *const c_char {
    // Use a static byte string with embedded null terminator to avoid any
    // possibility of panic from CString::new(). The concat! + \0 pattern
    // is infallible at compile time.
    static VERSION_BYTES: &[u8] =
        concat!("butterfly-dl ", env!("BUTTERFLY_VERSION"), "\0").as_bytes();
    VERSION_BYTES.as_ptr() as *const c_char
}

/// Initialize the library (optional, called automatically)
///
/// This function is called automatically when needed, but can be called
/// explicitly to initialize the async runtime early.
///
/// # Returns
/// ButterflyResult::Success on success, ButterflyResult::UnknownError if
/// the runtime could not be created.
#[no_mangle]
pub extern "C" fn butterfly_init() -> ButterflyResult {
    let result = std::panic::catch_unwind(|| match get_runtime() {
        Some(_) => ButterflyResult::Success,
        None => ButterflyResult::UnknownError,
    });

    result.unwrap_or(ButterflyResult::UnknownError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_butterfly_version() {
        let version = butterfly_version();
        assert!(!version.is_null());
        let version_str = unsafe { CStr::from_ptr(version) }.to_str().unwrap();
        assert!(version_str.contains("butterfly-dl"));
        assert!(version_str.contains(env!("BUTTERFLY_VERSION")));
    }

    #[test]
    fn test_butterfly_get_filename() {
        let source = CString::new("europe/belgium").unwrap();
        let filename_ptr = unsafe { butterfly_get_filename(source.as_ptr()) };
        assert!(!filename_ptr.is_null());

        let filename = unsafe { CStr::from_ptr(filename_ptr) }.to_str().unwrap();
        assert_eq!(filename, "belgium-latest.osm.pbf");

        unsafe { butterfly_free_string(filename_ptr) };
    }

    #[test]
    fn test_butterfly_init() {
        let result = butterfly_init();
        assert_eq!(result as u32, ButterflyResult::Success as u32);
    }

    #[test]
    fn test_invalid_parameters() {
        let result = unsafe { butterfly_download(std::ptr::null(), std::ptr::null()) };
        assert_eq!(result as u32, ButterflyResult::InvalidParameter as u32);
    }

    #[test]
    fn test_last_error_message() {
        // Trigger an error
        let result = unsafe { butterfly_download(std::ptr::null(), std::ptr::null()) };
        assert_eq!(result as u32, ButterflyResult::InvalidParameter as u32);

        // Retrieve the error message
        let msg_ptr = butterfly_last_error_message();
        assert!(!msg_ptr.is_null());

        let msg = unsafe { CStr::from_ptr(msg_ptr) }.to_str().unwrap();
        assert!(msg.contains("NULL"), "Expected NULL mention, got: {msg}");

        unsafe { butterfly_free_string(msg_ptr) };
    }

    #[test]
    fn test_last_error_message_none() {
        // Clear by calling init (success clears error)
        butterfly_init();

        // After success, last error should be None
        clear_last_error();
        let msg_ptr = butterfly_last_error_message();
        assert!(msg_ptr.is_null());
    }

    #[test]
    fn test_version_is_static() {
        // Verify version can be called multiple times safely
        let v1 = butterfly_version();
        let v2 = butterfly_version();
        assert_eq!(v1, v2); // Same pointer — truly static
    }
}
