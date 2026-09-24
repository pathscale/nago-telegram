//! A Telegram Bot API client over [`nago_http`]. No tokio, no OpenSSL, no C.
//!
//! Requests go through a [`nago_http::Client`], which runs its own reactor on a
//! thread of its own, so a [`Bot`]'s futures complete under whatever executor
//! awaits them: a server's reactor, a nagoya pool, or `nagoya::block_on`.
//! Concurrent calls proceed concurrently, so a `getUpdates` long poll does not
//! hold up a `sendMessage`.
//!
//! The common methods are typed; anything else goes through [`Bot::call`].
//!
//! ```no_run
//! use std::time::Duration;
//! use nago_telegram::{Bot, SendMessage};
//!
//! # fn main() -> Result<(), nago_telegram::Error> {
//! let bot = Bot::new("123456:ABC-DEF")?;
//! nagoya::block_on(async {
//!     let mut offset = None;
//!     loop {
//!         for update in bot.get_updates(offset, Duration::from_secs(25), &["message"]).await? {
//!             offset = Some(update.update_id + 1);
//!             let Some(message) = update.message else { continue };
//!             if message.command().is_some_and(|c| c.name == "/start") {
//!                 bot.send_message(&SendMessage::new(message.chat.id, "hello")).await?;
//!             }
//!         }
//!     }
//! })
//! # }
//! ```
//!
//! The bot token is part of every request path. No error from this crate
//! carries it: nago-http never puts a path in an error, and neither does
//! anything here.

use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const HOST: &str = "api.telegram.org";

/// The budget for any call but a long poll, and the round-trip slack on top of
/// one. A request past this is a dead connection rather than a slow one.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The HTTP client's own ceiling. It only has to clear the longest call; each
/// call keeps its own tighter deadline.
const CLIENT_CEILING: Duration = Duration::from_secs(120);

/// One HTTP client for every bot in the process.
fn http() -> Result<&'static nago_http::Client, Error> {
    static CLIENT: OnceLock<nago_http::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let client = nago_http::Client::new()
        .map_err(Error::Http)?
        .with_timeout(CLIENT_CEILING);
    // Losing a race drops this one; the winner serves everyone.
    Ok(CLIENT.get_or_init(|| client))
}

/// Why a call failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The request did not complete: connect, TLS or I/O.
    Http(nago_http::Error),
    /// No answer within the call's deadline.
    Timeout { method: String, after: Duration },
    /// Telegram answered and refused.
    Api {
        method: String,
        status: u16,
        /// Telegram's own code, usually the HTTP status.
        error_code: Option<i64>,
        description: String,
        /// Seconds to wait before retrying, when rate limited.
        retry_after: Option<u64>,
    },
    /// The answer was not the JSON the method returns.
    Decode {
        method: String,
        status: u16,
        message: String,
    },
    /// The parameters could not be serialized.
    Encode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(e) => write!(f, "Telegram request failed: {e}"),
            Self::Timeout { method, after } => {
                write!(f, "Telegram {method} timed out after {after:?}")
            }
            Self::Api {
                method,
                status,
                description,
                ..
            } => write!(
                f,
                "Telegram {method} failed with HTTP {status}: {description}"
            ),
            Self::Decode {
                method,
                status,
                message,
            } => write!(
                f,
                "Telegram {method} answered HTTP {status} with an unreadable body: {message}"
            ),
            Self::Encode(message) => write!(f, "encoding Telegram parameters: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// A bot, by its token. Cheap to clone.
#[derive(Clone)]
pub struct Bot {
    token: String,
    http: &'static nago_http::Client,
}

impl fmt::Debug for Bot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the token.
        f.debug_struct("Bot").finish_non_exhaustive()
    }
}

impl Bot {
    /// A bot for `token`. Nothing touches the network until a call.
    ///
    /// # Errors
    /// If the HTTP client's reactor cannot start.
    pub fn new(token: impl Into<String>) -> Result<Self, Error> {
        Ok(Self {
            token: token.into(),
            http: http()?,
        })
    }

    /// Call any Bot API method with JSON `params`, within `limit`.
    ///
    /// # Errors
    /// If the request fails, times out, Telegram refuses it, or the answer is
    /// not a `T`.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        limit: Duration,
    ) -> Result<T, Error> {
        let body = serde_json::to_vec(params).map_err(|e| Error::Encode(e.to_string()))?;
        let url = format!("https://{HOST}/bot{}/{method}", self.token);
        let request = self.http.post(&url, &[], "application/json", &body);
        let response = match nagoya::timeout(limit, request).await {
            Ok(Ok(response)) => response,
            Ok(Err(nago_http::Error::Timeout(_))) | Err(_) => {
                return Err(Error::Timeout {
                    method: method.to_owned(),
                    after: limit,
                });
            }
            Ok(Err(e)) => return Err(Error::Http(e)),
        };

        let reply: Reply<T> =
            serde_json::from_slice(&response.body).map_err(|e| Error::Decode {
                method: method.to_owned(),
                status: response.status,
                message: e.to_string(),
            })?;
        match reply {
            Reply {
                ok: true,
                result: Some(result),
                ..
            } => Ok(result),
            Reply {
                description,
                error_code,
                parameters,
                ..
            } => Err(Error::Api {
                method: method.to_owned(),
                status: response.status,
                error_code,
                description: description.unwrap_or_default(),
                retry_after: parameters.and_then(|p| p.retry_after),
            }),
        }
    }

    /// `getMe`: the bot's own user, which also proves the token works.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn get_me(&self) -> Result<User, Error> {
        self.call("getMe", &Empty {}, REQUEST_TIMEOUT).await
    }

    /// `getUpdates`: updates from `offset` on, waiting up to `timeout` for one
    /// to arrive. `allowed` names the update kinds wanted (`"message"`,
    /// `"callback_query"`, ...); empty keeps whatever was asked for last.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout: Duration,
        allowed: &[&str],
    ) -> Result<Vec<Update>, Error> {
        #[derive(Serialize)]
        struct GetUpdates<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            offset: Option<i64>,
            timeout: u64,
            #[serde(skip_serializing_if = "<[_]>::is_empty")]
            allowed_updates: &'a [&'a str],
        }
        self.call(
            "getUpdates",
            &GetUpdates {
                offset,
                timeout: timeout.as_secs(),
                allowed_updates: allowed,
            },
            timeout.saturating_add(REQUEST_TIMEOUT),
        )
        .await
    }

    /// `sendMessage`.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn send_message(&self, message: &SendMessage) -> Result<Message, Error> {
        self.call("sendMessage", message, REQUEST_TIMEOUT).await
    }

    /// `editMessageReplyMarkup`: replace a message's inline keyboard, or remove
    /// it with `None`.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn edit_message_reply_markup(
        &self,
        chat_id: i64,
        message_id: i64,
        markup: Option<&InlineKeyboardMarkup>,
    ) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Edit<'a> {
            chat_id: i64,
            message_id: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            reply_markup: Option<&'a InlineKeyboardMarkup>,
        }
        // Answers with the message, or `true` for an inline message.
        let _: serde_json::Value = self
            .call(
                "editMessageReplyMarkup",
                &Edit {
                    chat_id,
                    message_id,
                    reply_markup: markup,
                },
                REQUEST_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    /// `answerCallbackQuery`: acknowledge an inline-button press, optionally
    /// with a notification (`show_alert` makes it a dialog).
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Answer<'a> {
            callback_query_id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            text: Option<&'a str>,
            show_alert: bool,
        }
        let _: bool = self
            .call(
                "answerCallbackQuery",
                &Answer {
                    callback_query_id,
                    text,
                    show_alert,
                },
                REQUEST_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    /// `setWebhook`. With a `secret_token`, Telegram sends it back in the
    /// `X-Telegram-Bot-Api-Secret-Token` header of every delivery, which is how
    /// a webhook tells Telegram from anyone else.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn set_webhook(&self, url: &str, secret_token: Option<&str>) -> Result<(), Error> {
        #[derive(Serialize)]
        struct SetWebhook<'a> {
            url: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            secret_token: Option<&'a str>,
        }
        let _: bool = self
            .call(
                "setWebhook",
                &SetWebhook { url, secret_token },
                REQUEST_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    /// `deleteWebhook`, which `getUpdates` needs: Telegram refuses a long poll
    /// while a webhook is set.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn delete_webhook(&self) -> Result<(), Error> {
        let _: bool = self
            .call("deleteWebhook", &Empty {}, REQUEST_TIMEOUT)
            .await?;
        Ok(())
    }

    /// `getWebhookInfo`.
    ///
    /// # Errors
    /// As [`Bot::call`].
    pub async fn get_webhook_info(&self) -> Result<WebhookInfo, Error> {
        self.call("getWebhookInfo", &Empty {}, REQUEST_TIMEOUT)
            .await
    }
}

#[derive(Serialize)]
struct Empty {}

#[derive(Deserialize)]
struct Reply<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

/// How Telegram should read a message's text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ParseMode {
    /// Escape interpolated text with [`escape_html`].
    #[serde(rename = "HTML")]
    Html,
    MarkdownV2,
}

/// The parameters of `sendMessage`.
#[derive(Clone, Debug, Serialize)]
pub struct SendMessage {
    pub chat_id: i64,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_mode: Option<ParseMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_parameters: Option<ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

impl SendMessage {
    /// Plain text to `chat_id`.
    pub fn new(chat_id: i64, text: impl Into<String>) -> Self {
        Self {
            chat_id,
            text: text.into(),
            parse_mode: None,
            reply_parameters: None,
            reply_markup: None,
        }
    }

    /// Read the text as HTML.
    #[must_use]
    pub fn html(mut self) -> Self {
        self.parse_mode = Some(ParseMode::Html);
        self
    }

    /// Read the text as MarkdownV2.
    #[must_use]
    pub fn markdown_v2(mut self) -> Self {
        self.parse_mode = Some(ParseMode::MarkdownV2);
        self
    }

    /// Send as a reply to `message_id` in the same chat.
    #[must_use]
    pub fn reply_to(mut self, message_id: i64) -> Self {
        self.reply_parameters = Some(ReplyParameters { message_id });
        self
    }

    /// Attach an inline keyboard.
    #[must_use]
    pub fn keyboard(mut self, markup: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(markup);
        self
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplyParameters {
    pub message_id: i64,
}

/// Buttons under a message, row by row.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InlineKeyboardButton {
    pub text: String,
    /// Sent back in the [`CallbackQuery`] when pressed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_data: Option<String>,
    /// Opened when pressed, instead of a callback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl InlineKeyboardButton {
    /// A button that sends `data` back as a callback query.
    pub fn callback(text: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            callback_data: Some(data.into()),
            url: None,
        }
    }

    /// A button that opens `url`.
    pub fn url(text: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            callback_data: None,
            url: Some(url.into()),
        }
    }
}

/// One update, from `getUpdates` or a webhook body. Kinds this crate does not
/// model are left out, and the update still parses.
#[derive(Clone, Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub edited_message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    /// Unix seconds.
    pub date: i64,
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub reply_to_message: Option<Box<Message>>,
}

/// A bot command at the start of a message: `/name@bot arg arg`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command<'a> {
    /// With its `/`, without any `@botname` suffix.
    pub name: &'a str,
    /// The whitespace-separated words after it.
    pub args: Vec<&'a str>,
}

impl Message {
    /// The text, or a media message's caption.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref().or(self.caption.as_deref())
    }

    /// The command this message's text starts with, if any.
    #[must_use]
    pub fn command(&self) -> Option<Command<'_>> {
        let mut words = self.text.as_deref()?.split_whitespace();
        let first = words.next()?;
        if !first.starts_with('/') {
            return None;
        }
        Some(Command {
            name: first.split('@').next().unwrap_or(first),
            args: words.collect(),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    /// `private`, `group`, `supergroup` or `channel`.
    #[serde(rename = "type")]
    pub kind: String,
    pub title: Option<String>,
    pub username: Option<String>,
}

impl Chat {
    /// A one-to-one chat with a user.
    #[must_use]
    pub fn is_private(&self) -> bool {
        self.kind == "private"
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct User {
    pub id: i64,
    pub is_bot: bool,
    pub first_name: String,
    pub username: Option<String>,
}

/// An inline-button press.
#[derive(Clone, Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub from: User,
    /// The message the button was on, if the bot can still see it.
    pub message: Option<Box<Message>>,
    pub data: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WebhookInfo {
    /// Empty when no webhook is set.
    pub url: String,
    pub pending_update_count: i64,
    pub last_error_message: Option<String>,
}

/// Escape text for [`ParseMode::Html`].
#[must_use]
pub fn escape_html(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            other => escaped.push(other),
        }
    }
    escaped
}
