//! String normalization shared by the parser and the shard builder.
//!
//! Keeping this in one module is load-bearing: the shard's inverted
//! index is keyed by `normalize(...)`. If the parser used a different
//! normalization, lookups would silently miss.

#[must_use]
pub fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = true;
    for c in s.chars() {
        let mapped = fold_char(c);
        for fc in mapped.chars() {
            let lower = fc.to_ascii_lowercase();
            if lower.is_ascii_alphanumeric() || lower == '-' || lower == '\'' {
                out.push(lower);
                last_was_space = false;
            } else if lower.is_whitespace() && !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

fn fold_char(c: char) -> String {
    let s: &str = match c {
        'à' | 'á' | 'â' | 'ä' | 'ã' | 'å' => "a",
        'À' | 'Á' | 'Â' | 'Ä' | 'Ã' | 'Å' => "a",
        'ç' | 'Ç' => "c",
        'è' | 'é' | 'ê' | 'ë' => "e",
        'È' | 'É' | 'Ê' | 'Ë' => "e",
        'ì' | 'í' | 'î' | 'ï' => "i",
        'Ì' | 'Í' | 'Î' | 'Ï' => "i",
        'ñ' | 'Ñ' => "n",
        'ò' | 'ó' | 'ô' | 'ö' | 'õ' => "o",
        'Ò' | 'Ó' | 'Ô' | 'Ö' | 'Õ' => "o",
        'ù' | 'ú' | 'û' | 'ü' => "u",
        'Ù' | 'Ú' | 'Û' | 'Ü' => "u",
        'ý' | 'ÿ' | 'Ý' | 'Ÿ' => "y",
        'ß' => "ss",
        'œ' | 'Œ' => "oe",
        'æ' | 'Æ' => "ae",
        _ => return c.to_string(),
    };
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercase_and_strip() {
        assert_eq!(normalize("Rue Wayez 122"), "rue wayez 122");
    }

    #[test]
    fn diacritics_folded() {
        assert_eq!(normalize("Chaussée de Wavre"), "chaussee de wavre");
        assert_eq!(normalize("Sint-Niklaas"), "sint-niklaas");
    }

    #[test]
    fn collapse_whitespace() {
        assert_eq!(normalize("  Rue   Royale  "), "rue royale");
    }

    #[test]
    fn drop_punctuation() {
        assert_eq!(normalize("Rue, Royale!"), "rue royale");
    }
}
