//! Phase 2 augmentation strategies.
//!
//! Each strategy renders a `(record fields)` tuple into a single
//! query string and returns its `AugmentationKind` tag. The label
//! the parser is supposed to recover is the same for every output —
//! that's the retrieval-success-invariant from #96 §Shard-Agnostic
//! Augmentation.
//!
//! Country-aware: postcode position (after-street for BE, before-street
//! for some others) is parameterised by `country`, so a future BE
//! shard's strict `street, hn, pc, locality` rendering doesn't bleed
//! into a French rendering when this code is reused with FR.

use butterfly_geocode::CountryId;
use rand::Rng;
use rand_chacha::ChaCha20Rng;

use crate::sample::AugmentationKind;

/// Source record fields. We don't reuse `AddressRecord` here because
/// we want a borrowed view (most fields are `Arc<str>` in the shard
/// reader).
#[derive(Debug, Clone)]
pub struct Fields<'a> {
    pub street: &'a str,
    pub housenumber: &'a str,
    pub postcode: &'a str,
    pub locality: &'a str,
}

/// Render the canonical query for `country`. For BE the convention is
/// `street housenumber postcode locality` (postcode-after-street). FR/DE/
/// AT/CH/LU follow the same shape; NL adds a 4-digit + 2-letter postcode
/// rendered with a space.
#[must_use]
pub fn render_canonical(f: &Fields<'_>, country: CountryId) -> String {
    // For Phase 2 (BE only at MVP), postcode follows the housenumber.
    // The country parameter is forward-looking — when other shards
    // land, we extend this match.
    let _ = country;
    let mut parts: Vec<String> = Vec::with_capacity(4);
    if !f.street.is_empty() {
        parts.push(f.street.to_string());
    }
    if !f.housenumber.is_empty() {
        parts.push(f.housenumber.to_string());
    }
    if !f.postcode.is_empty() {
        parts.push(f.postcode.to_string());
    }
    if !f.locality.is_empty() {
        parts.push(f.locality.to_string());
    }
    parts.join(" ")
}

/// Render `postcode locality street housenumber`.
#[must_use]
pub fn render_postcode_first(f: &Fields<'_>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(4);
    if !f.postcode.is_empty() {
        parts.push(f.postcode.to_string());
    }
    if !f.locality.is_empty() {
        parts.push(f.locality.to_string());
    }
    if !f.street.is_empty() {
        parts.push(f.street.to_string());
    }
    if !f.housenumber.is_empty() {
        parts.push(f.housenumber.to_string());
    }
    parts.join(" ")
}

#[must_use]
pub fn render_drop_postcode(f: &Fields<'_>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(3);
    if !f.street.is_empty() {
        parts.push(f.street.to_string());
    }
    if !f.housenumber.is_empty() {
        parts.push(f.housenumber.to_string());
    }
    if !f.locality.is_empty() {
        parts.push(f.locality.to_string());
    }
    parts.join(" ")
}

#[must_use]
pub fn render_drop_locality(f: &Fields<'_>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(3);
    if !f.street.is_empty() {
        parts.push(f.street.to_string());
    }
    if !f.housenumber.is_empty() {
        parts.push(f.housenumber.to_string());
    }
    if !f.postcode.is_empty() {
        parts.push(f.postcode.to_string());
    }
    parts.join(" ")
}

/// French/Dutch street-type contractions.
const ABBR_CONTRACT: &[(&str, &str)] = &[
    ("Rue", "R."),
    ("rue", "r."),
    ("Boulevard", "Bd"),
    ("boulevard", "bd"),
    ("Avenue", "Av."),
    ("avenue", "av."),
    ("Place", "Pl."),
    ("place", "pl."),
    ("Chaussée", "Ch."),
    ("chaussée", "ch."),
    ("Saint", "St"),
    ("saint", "st"),
    ("Straat", "Str."),
    ("straat", "str."),
    ("Laan", "Ln."),
    ("laan", "ln."),
];

const ABBR_EXPAND: &[(&str, &str)] = &[
    ("R. ", "Rue "),
    ("r. ", "rue "),
    ("Bd. ", "Boulevard "),
    ("bd. ", "boulevard "),
    ("Av. ", "Avenue "),
    ("av. ", "avenue "),
    ("Pl. ", "Place "),
    ("pl. ", "place "),
    ("Ch. ", "Chaussée "),
    ("ch. ", "chaussée "),
    ("St ", "Saint "),
    ("st ", "saint "),
    ("Str. ", "Straat "),
    ("str. ", "straat "),
    ("Ln. ", "Laan "),
    ("ln. ", "laan "),
];

#[must_use]
pub fn render_abbr_contract(f: &Fields<'_>, country: CountryId) -> String {
    let mut street_owned: String = f.street.to_string();
    for (long, short) in ABBR_CONTRACT {
        if street_owned.starts_with(long) {
            street_owned = format!("{}{}", short, &street_owned[long.len()..]);
            break;
        }
    }
    let f2 = Fields {
        street: &street_owned,
        housenumber: f.housenumber,
        postcode: f.postcode,
        locality: f.locality,
    };
    render_canonical(&f2, country)
}

#[must_use]
pub fn render_abbr_expand(f: &Fields<'_>, country: CountryId) -> String {
    let mut street_owned: String = f.street.to_string();
    for (short, long) in ABBR_EXPAND {
        if street_owned.starts_with(short) {
            street_owned = format!("{}{}", long, &street_owned[short.len()..]);
            break;
        }
    }
    let f2 = Fields {
        street: &street_owned,
        housenumber: f.housenumber,
        postcode: f.postcode,
        locality: f.locality,
    };
    render_canonical(&f2, country)
}

#[must_use]
pub fn render_upper(f: &Fields<'_>, country: CountryId) -> String {
    render_canonical(f, country).to_uppercase()
}

#[must_use]
pub fn render_lower(f: &Fields<'_>, country: CountryId) -> String {
    render_canonical(f, country).to_lowercase()
}

/// Whitespace + comma noise. Replaces some inter-token spaces with
/// double-space, ` , `, or ` - `.
#[must_use]
pub fn render_ws_noise(f: &Fields<'_>, rng: &mut ChaCha20Rng) -> String {
    let parts = [f.street, f.housenumber, f.postcode, f.locality];
    let mut out = String::new();
    let mut first = true;
    for p in parts {
        if p.is_empty() {
            continue;
        }
        if !first {
            let sep = match rng.random_range(0..6) {
                0 => "  ",
                1 => " - ",
                2 => ", ",
                3 => " , ",
                4 => "  ,  ",
                _ => " ",
            };
            out.push_str(sep);
        }
        out.push_str(p);
        first = false;
    }
    out
}

/// Typo injection: substitute one ASCII letter in the street.
#[must_use]
pub fn render_typo(f: &Fields<'_>, rng: &mut ChaCha20Rng, country: CountryId) -> String {
    let mut bytes: Vec<u8> = f.street.as_bytes().to_vec();
    if bytes.len() < 2 {
        return render_canonical(f, country);
    }
    // Find an ASCII alphabetic byte and substitute it.
    let mut tries = 8;
    let mut applied = false;
    while tries > 0 && !applied {
        let pos = rng.random_range(0..bytes.len());
        if bytes[pos].is_ascii_alphabetic() {
            // pick a different lowercase letter
            let new = b'a' + (rng.random_range(0..26) as u8);
            if new != bytes[pos].to_ascii_lowercase() {
                bytes[pos] = if bytes[pos].is_ascii_uppercase() {
                    new.to_ascii_uppercase()
                } else {
                    new
                };
                applied = true;
            }
        }
        tries -= 1;
    }
    let new_street = String::from_utf8(bytes).unwrap_or_else(|_| f.street.to_string());
    let f2 = Fields {
        street: &new_street,
        housenumber: f.housenumber,
        postcode: f.postcode,
        locality: f.locality,
    };
    render_canonical(&f2, country)
}

/// Apply augmentation `kind` to the canonical fields. Returns the
/// rendered query string. For deterministic output the caller threads
/// the `rng` through; non-stochastic kinds ignore it.
#[must_use]
pub fn apply(
    kind: AugmentationKind,
    f: &Fields<'_>,
    country: CountryId,
    rng: &mut ChaCha20Rng,
) -> String {
    match kind {
        AugmentationKind::Canonical => render_canonical(f, country),
        AugmentationKind::PostcodeFirst => render_postcode_first(f),
        AugmentationKind::DropPostcode => render_drop_postcode(f),
        AugmentationKind::DropLocality => render_drop_locality(f),
        AugmentationKind::AbbrContract => render_abbr_contract(f, country),
        AugmentationKind::AbbrExpand => render_abbr_expand(f, country),
        AugmentationKind::UpperCase => render_upper(f, country),
        AugmentationKind::LowerCase => render_lower(f, country),
        AugmentationKind::WhitespaceNoise => render_ws_noise(f, rng),
        AugmentationKind::Typo => render_typo(f, rng, country),
    }
}

/// Default 8-augmentation roster (canonical is emitted separately).
/// Per #98 Phase 2 prompt: N=8 augmentations per gold record (codex's
/// review of #164 noted N=10 was too aggressive for the model class
/// at issue; we're not training a transformer — feature-space GBDT —
/// so 8 is fine and balances corpus size against per-record diversity).
pub const DEFAULT_AUGMENTATIONS: [AugmentationKind; 8] = [
    AugmentationKind::PostcodeFirst,
    AugmentationKind::DropPostcode,
    AugmentationKind::DropLocality,
    AugmentationKind::AbbrContract,
    AugmentationKind::AbbrExpand,
    AugmentationKind::WhitespaceNoise,
    AugmentationKind::LowerCase,
    AugmentationKind::Typo,
];

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn fields() -> Fields<'static> {
        Fields {
            street: "Rue Wayez",
            housenumber: "122",
            postcode: "1070",
            locality: "Anderlecht",
        }
    }

    #[test]
    fn canonical_renders_be_order() {
        let q = render_canonical(&fields(), CountryId::BE);
        assert_eq!(q, "Rue Wayez 122 1070 Anderlecht");
    }

    #[test]
    fn postcode_first_swaps_order() {
        let q = render_postcode_first(&fields());
        assert_eq!(q, "1070 Anderlecht Rue Wayez 122");
    }

    #[test]
    fn drop_postcode_omits_pc() {
        let q = render_drop_postcode(&fields());
        assert!(!q.contains("1070"));
        assert!(q.contains("Rue Wayez"));
        assert!(q.contains("Anderlecht"));
    }

    #[test]
    fn abbr_contract_changes_street_prefix() {
        let q = render_abbr_contract(&fields(), CountryId::BE);
        assert!(q.starts_with("R."), "got: {q}");
    }

    #[test]
    fn case_transforms_preserve_content() {
        let upper = render_upper(&fields(), CountryId::BE);
        let lower = render_lower(&fields(), CountryId::BE);
        assert_eq!(upper, upper.to_uppercase());
        assert_eq!(lower, lower.to_lowercase());
    }

    #[test]
    fn typo_changes_street_only() {
        let mut rng = ChaCha20Rng::seed_from_u64(7);
        let q = render_typo(&fields(), &mut rng, CountryId::BE);
        assert!(q.contains("122"));
        assert!(q.contains("1070"));
        assert!(q.contains("Anderlecht"));
    }

    #[test]
    fn ws_noise_keeps_all_tokens() {
        let mut rng = ChaCha20Rng::seed_from_u64(7);
        let q = render_ws_noise(&fields(), &mut rng);
        for tok in ["Rue", "Wayez", "122", "1070", "Anderlecht"] {
            assert!(q.contains(tok), "missing {tok} in {q}");
        }
    }

    #[test]
    fn default_augmentations_is_eight() {
        assert_eq!(DEFAULT_AUGMENTATIONS.len(), 8);
    }
}
