use lettre::message::{header::ContentType, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

/// SMTP mailer used to send verification emails. Configured via env vars:
/// SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_FROM.
#[derive(Clone)]
pub struct Mailer {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
    reply_to: String,
}

impl Mailer {
    pub fn from_env() -> Option<Self> {
        let host = std::env::var("SMTP_HOST").ok()?;
        let port = std::env::var("SMTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(587);
        let from = std::env::var("SMTP_FROM").ok()?;
        // Replies go to a noreply mailbox so users can't reply to the bot email.
        let reply_to = std::env::var("SMTP_REPLY_TO").unwrap_or_else(|_| {
            let domain = from.rsplit('@').next().unwrap_or("example.com");
            format!("noreply@{domain}")
        });

        let mut builder = match AsyncSmtpTransport::<Tokio1Executor>::relay(&host) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("SMTP relay setup failed ({e}); using plain builder");
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
            }
        };
        builder = builder.port(port);
        if let (Ok(username), Ok(password)) =
            (std::env::var("SMTP_USERNAME"), std::env::var("SMTP_PASSWORD"))
        {
            builder = builder.credentials(Credentials::new(username, password));
        }

        Some(Mailer {
            transport: builder.build(),
            from,
            reply_to,
        })
    }

    pub async fn send_verification_code(&self, to: &str, code: &str) -> Result<(), String> {
        let body = format!(
            "Your FediTexter verification code is: {code}\n\nEnter this code in the app to verify your email address.\n\nThis is an automated message; please do not reply."
        );
        let email = Message::builder()
            .from(self.from.parse().map_err(|e: lettre::address::AddressError| e.to_string())?)
            .to(to.parse().map_err(|e: lettre::address::AddressError| e.to_string())?)
            .reply_to(self.reply_to.parse().map_err(|e: lettre::address::AddressError| e.to_string())?)
            .subject("Your FediTexter verification code")
            .header(ContentType::TEXT_PLAIN)
            .body(body)
            .map_err(|e| e.to_string())?;
        self.transport.send(email).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
