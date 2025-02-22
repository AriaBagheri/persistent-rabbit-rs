use colored::Colorize;
use lapin::{Channel, Connection, ConnectionProperties};
use standard_error::traits::StandardErrorDescriptionTrait;
use crate::error::PersistentRabbitError;

pub struct RabbitConfiguration {
    pub address: String,
    pub connection: Connection,
    pub channel: Channel,
}

impl RabbitConfiguration {
    /// Sets the RabbitMQ server address, initiates the connection and channel,
    /// and sets up channel listeners for any registered consumers.
    pub async fn new(address: String) -> Result<Self, PersistentRabbitError> {
        let connection = Self::initiate_connection(&address).await?;
        let channel = Self::create_channel_with(&connection).await?;

        Ok(RabbitConfiguration {
            address,
            connection,
            channel,
        })
    }
    async fn initiate_connection(addr: &str) -> Result<Connection, PersistentRabbitError> {
        println!(
            "{}",
            "RABBIT - INITIATE_CONNECTION - Establishing connection...".blue()
        );
        Connection::connect(addr, ConnectionProperties::default())
            .await
            .map(|c| {
                println!(
                    "{}",
                    "RABBIT - INITIATE_CONNECTION - Connection established successfully!".green()
                );
                c
            })
            .map_err(|e| {
                println!(
                    "{}",
                    format!(
                        "RABBIT - INITIATE_CONNECTION - {} - {}",
                        PersistentRabbitError::FailedToEstablishConnection
                            .description()
                            .unwrap_or_default(),
                        e.to_string()
                    )
                        .red()
                );
                PersistentRabbitError::FailedToEstablishConnection
            })
    }

    async fn create_channel_with(
        connection: &Connection,
    ) -> Result<Channel, PersistentRabbitError> {
        println!("{}", "RABBIT - CREATE_CHANNEL - Creating channel...".blue());
        connection
            .create_channel()
            .await
            .map(|c| {
                println!(
                    "{}",
                    "RABBIT - CREATE_CHANNEL - Channel created successfully!".green()
                );
                c
            })
            .map_err(|e| {
                println!(
                    "{}",
                    format!(
                        "RABBIT - CREATE_CHANNEL - {} - {}",
                        PersistentRabbitError::FailedToEstablishChannel
                            .description()
                            .unwrap_or_default(),
                        e.to_string()
                    )
                        .red()
                );
                PersistentRabbitError::FailedToEstablishChannel
            })
    }

    pub fn is_connected(&self) -> bool {
        self.connection.status().connected() && self.channel.status().connected()
    }
}
