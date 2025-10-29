///! Tag lookup utility for profiles
///!
///! Provides convenient access to OSM tags from dictionary-encoded arrays.

use std::collections::HashMap;

/// Helper for looking up tags in dictionary-encoded format
pub struct TagLookup<'a> {
    keys: &'a [u32],
    vals: &'a [u32],
    // For now, we'll need the actual dictionary strings to be passed in
    // or we'll work with IDs only and resolve strings in the pipeline
    key_dict: Option<&'a HashMap<u32, String>>,
    val_dict: Option<&'a HashMap<u32, String>>,
}

impl<'a> TagLookup<'a> {
    pub fn new(keys: &'a [u32], vals: &'a [u32]) -> Self {
        Self {
            keys,
            vals,
            key_dict: None,
            val_dict: None,
        }
    }

    pub fn from_input(
        keys: &'a [u32],
        vals: &'a [u32],
        key_dict: Option<&'a HashMap<u32, String>>,
        val_dict: Option<&'a HashMap<u32, String>>,
    ) -> Self {
        Self {
            keys,
            vals,
            key_dict,
            val_dict,
        }
    }

    pub fn with_dicts(
        keys: &'a [u32],
        vals: &'a [u32],
        key_dict: &'a HashMap<u32, String>,
        val_dict: &'a HashMap<u32, String>,
    ) -> Self {
        Self {
            keys,
            vals,
            key_dict: Some(key_dict),
            val_dict: Some(val_dict),
        }
    }

    /// Get a tag value by key name (requires dictionaries to be set)
    pub fn get_str(&self, key: &str) -> Option<&str> {
        let key_dict = self.key_dict?;
        let val_dict = self.val_dict?;

        // Find the key ID
        let key_id = key_dict.iter()
            .find(|(_, v)| v.as_str() == key)
            .map(|(k, _)| *k)?;

        // Find the value ID
        for (i, k) in self.keys.iter().enumerate() {
            if *k == key_id {
                let val_id = self.vals[i];
                return val_dict.get(&val_id).map(|s| s.as_str());
            }
        }

        None
    }

    /// Check if a key exists
    pub fn has(&self, key: &str) -> bool {
        self.get_str(key).is_some()
    }

    /// Get a tag value by key ID (when working with IDs directly)
    pub fn get_by_id(&self, key_id: u32) -> Option<u32> {
        self.keys.iter()
            .position(|k| *k == key_id)
            .map(|i| self.vals[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_lookup_with_dicts() {
        let keys = vec![1, 2];
        let vals = vec![10, 20];

        let mut key_dict = HashMap::new();
        key_dict.insert(1, "highway".to_string());
        key_dict.insert(2, "name".to_string());

        let mut val_dict = HashMap::new();
        val_dict.insert(10, "motorway".to_string());
        val_dict.insert(20, "Main Street".to_string());

        let tags = TagLookup::with_dicts(&keys, &vals, &key_dict, &val_dict);

        assert_eq!(tags.get_str("highway"), Some("motorway"));
        assert_eq!(tags.get_str("name"), Some("Main Street"));
        assert_eq!(tags.get_str("surface"), None);
    }

    #[test]
    fn test_has() {
        let keys = vec![1];
        let vals = vec![10];

        let mut key_dict = HashMap::new();
        key_dict.insert(1, "highway".to_string());

        let mut val_dict = HashMap::new();
        val_dict.insert(10, "motorway".to_string());

        let tags = TagLookup::with_dicts(&keys, &vals, &key_dict, &val_dict);

        assert!(tags.has("highway"));
        assert!(!tags.has("name"));
    }
}
