mod logger;
mod penguin;
mod types;

pub mod prelude {
    pub use super::{
        penguin::{Penguin, PenguinBuilder},
        types::PenguinError,
    };
}
