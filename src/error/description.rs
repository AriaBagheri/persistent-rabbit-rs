use super::*;

impl StandardErrorDescriptionTrait for PersistentRabbitError {
    fn description(&self) -> Option<&'static str> {
        match self {
            PersistentRabbitError::NoAddress => Some("No Address was specified in configuration."),
            PersistentRabbitError::FailedToEstablishConnection => {
                Some("Failed to establish connection.")
            }
            PersistentRabbitError::FailedToEstablishChannel => Some("Failed to establish channel."),
        }
    }
}
