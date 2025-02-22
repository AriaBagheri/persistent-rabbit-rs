use std::str::FromStr;

pub struct RabbitUri(String);

impl FromStr for RabbitUri {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RabbitUri(s.to_string()))
    }
}
