use standard_error::traits::*;
mod causes;
mod code;
mod description;

use std::fmt::Display;

#[derive(Debug)]
pub enum PersistentRabbitError {
    NoAddress,
    FailedToEstablishConnection,
    FailedToEstablishChannel,
}

impl Display for PersistentRabbitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistentRabbitError::NoAddress => {
                write!(
                    f,
                    "{}",
                    "No Address was specified when trying to establish a connection"
                )
            }
            PersistentRabbitError::FailedToEstablishConnection => {
                write!(
                    f,
                    "{}",
                    "Failed to establish connection. Possibly the address is incorrect"
                )
            }
            PersistentRabbitError::FailedToEstablishChannel => {
                write!(
                    f,
                    "{}",
                    "Failed to establish channel. Connection is possibly at a bad state"
                )
            }
        }
    }
}
