use super::*;

impl StandardErrorCausesTrait for PersistentRabbitError {
    fn causes(&self) -> Option<&'static str> {
        match self {
            PersistentRabbitError::NoAddress => {Some("This error is serious. This usually indicates bad configuration files.")}
            PersistentRabbitError::FailedToEstablishConnection => {Some("For a connection to fail to establish, there could be either bad connection on the server end, or a bad address configured.")}
            PersistentRabbitError::FailedToEstablishChannel => {None}
        }
    }
}
