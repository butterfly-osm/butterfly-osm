//! Built-in routing profiles for different travel modes
//!
//! Each profile implements tag semantics for access, speed, and preferences.

pub mod bike;
pub mod car;
pub mod foot;
pub mod tag_lookup;

pub use bike::BikeProfile;
pub use car::CarProfile;
pub use foot::FootProfile;
