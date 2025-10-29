///! Built-in routing profiles for different travel modes
///!
///! Each profile implements tag semantics for access, speed, and preferences.

pub mod tag_lookup;
pub mod car;
pub mod bike;
pub mod foot;

pub use car::CarProfile;
pub use bike::BikeProfile;
pub use foot::FootProfile;
