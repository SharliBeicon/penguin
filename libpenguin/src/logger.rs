use std::{io, path::Path};

/// Tracing logger that keeps a background guard alive.
/// Tracing can keep sending logs messages as long this guard is alive.
pub struct Logger {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

impl Logger {
    /// Initialize tracing to a file and return a guard
    /// Log level respect the `RUST_LOG` env filter.
    pub fn try_init_from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let (non_blocking, _guard) = tracing_appender::non_blocking(file);
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

        tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter(env_filter)
            .with_ansi(false)
            .with_target(false)
            .try_init()
            .map_err(io::Error::other)?;

        Ok(Logger { _guard })
    }
}
