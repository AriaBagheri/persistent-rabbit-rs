use super::*;

impl StandardErrorCodeTrait for PersistentRabbitError {
    fn code(&self) -> usize {
        match self {
            PersistentRabbitError::NoAddress => 6000,
            PersistentRabbitError::FailedToEstablishConnection => 6001,
            PersistentRabbitError::FailedToEstablishChannel => 6002,
        }
    }
}
