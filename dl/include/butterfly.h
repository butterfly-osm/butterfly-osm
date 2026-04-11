/**
 * @file butterfly.h
 * @brief C header for butterfly-dl library
 * 
 * High-performance OpenStreetMap data downloader with HTTP support.
 *
 * # Features
 * - Smart source routing: HTTP for planet files and regional extracts
 * - Memory efficient: <1GB RAM usage regardless of file size
 * - Progress tracking with callbacks
 * 
 * # Usage Example
 * 
 * ```c
 * #include "butterfly.h"
 * 
 * void progress_callback(uint64_t downloaded, uint64_t total, void* user_data) {
 *     double percent = (double)downloaded / total * 100.0;
 *     printf("Progress: %.1f%%\n", percent);
 * }
 * 
 * int main() {
 *     ButterflyResult result;
 *     
 *     // Simple download
 *     result = butterfly_download("europe/belgium", NULL);
 *     if (result != BUTTERFLY_SUCCESS) {
 *         fprintf(stderr, "Download failed: %d\n", result);
 *         return 1;
 *     }
 *     
 *     // Download with progress
 *     result = butterfly_download_with_progress(
 *         "planet", 
 *         "planet.pbf", 
 *         progress_callback, 
 *         NULL
 *     );
 *     
 *     return result == BUTTERFLY_SUCCESS ? 0 : 1;
 * }
 * ```
 * 
 * # Memory Management
 * 
 * - All string parameters should be null-terminated C strings
 * - Strings returned by library functions must be freed with butterfly_free_string()
 * - The library handles internal memory management
 * 
 * # Thread Safety
 * 
 * - All functions are thread-safe
 * - Multiple downloads can run concurrently
 * - Progress callbacks are called from the download thread
 */

#ifndef BUTTERFLY_H
#define BUTTERFLY_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Result codes returned by library functions
 */
typedef enum {
    BUTTERFLY_SUCCESS = 0,         /**< Operation completed successfully */
    BUTTERFLY_INVALID_PARAMETER = 1, /**< Invalid parameter provided */
    BUTTERFLY_NETWORK_ERROR = 2,   /**< Network-related error */
    BUTTERFLY_IO_ERROR = 3,        /**< File I/O error */
    BUTTERFLY_UNKNOWN_ERROR = 4    /**< Unknown or unexpected error */
} ButterflyResult;

/**
 * @brief Progress callback function type
 * 
 * Called periodically during download to report progress.
 * 
 * @param downloaded Number of bytes downloaded so far
 * @param total Total number of bytes to download
 * @param user_data User-provided data pointer
 */
typedef void (*ButterflyProgressCallback)(uint64_t downloaded, uint64_t total, void* user_data);

/**
 * @brief Download a file from a source
 * 
 * Downloads OpenStreetMap data from the specified source to a file.
 * If dest_path is NULL, the filename is auto-generated based on the source.
 * 
 * @param source Source identifier (e.g., "planet", "europe", "europe/belgium")
 * @param dest_path Destination file path, or NULL for auto-generated filename
 * @return ButterflyResult indicating success or failure
 * 
 * @note This function blocks until the download completes
 */
ButterflyResult butterfly_download(const char* source, const char* dest_path);

/**
 * @brief Download a file with progress tracking
 * 
 * Downloads OpenStreetMap data with optional progress reporting.
 * 
 * @param source Source identifier (e.g., "planet", "europe", "europe/belgium") 
 * @param dest_path Destination file path, or NULL for auto-generated filename
 * @param progress_callback Optional callback for progress updates, or NULL
 * @param user_data User data passed to progress callback
 * @return ButterflyResult indicating success or failure
 * 
 * @note This function blocks until the download completes
 * @note Progress callback is called from the download thread
 */
ButterflyResult butterfly_download_with_progress(
    const char* source,
    const char* dest_path, 
    ButterflyProgressCallback progress_callback,
    void* user_data
);

/**
 * @brief Get the auto-generated filename for a source
 * 
 * Returns the filename that would be used for auto-generated filenames.
 * The returned string must be freed with butterfly_free_string().
 * 
 * @param source Source identifier
 * @return Allocated filename string, or NULL on error
 * 
 * @note Caller must free the returned string with butterfly_free_string()
 */
char* butterfly_get_filename(const char* source);

/**
 * @brief Free a string allocated by the library
 * 
 * Frees memory allocated by library functions that return strings.
 * 
 * @param ptr String pointer to free
 * 
 * @note Only call this on strings returned by library functions
 * @note Safe to call with NULL pointer
 */
void butterfly_free_string(char* ptr);

/**
 * @brief Get library version string
 * 
 * Returns a static string with version information.
 * 
 * @return Version string (does not need to be freed)
 */
const char* butterfly_version(void);

/**
 * @brief Initialize the library
 * 
 * Initializes the internal async runtime. This is called automatically
 * when needed, but can be called explicitly for early initialization.
 * 
 * @return BUTTERFLY_SUCCESS on success
 * 
 * @note This function is optional - the library initializes automatically
 * @note Safe to call multiple times
 */
ButterflyResult butterfly_init(void);

#ifdef __cplusplus
}
#endif

#endif /* BUTTERFLY_H */