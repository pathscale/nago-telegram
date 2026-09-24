# nago-chat

Chat platform APIs over [nago-http](https://github.com/pathscale/nago-http): no
tokio, no OpenSSL, no C.

One module per platform, each behind a cargo feature of the same name, so a
service compiles only what it talks to. Each module is its platform's API as it
is. The platforms differ too much (long poll or webhook, chat ids, formatting,
buttons) for a shared trait to be more than a leak.

| Feature | Module | Covers |
|---|---|---|
| `telegram` (default) | `nago_chat::telegram` | Telegram Bot API: `getMe`, `getUpdates`, `sendMessage`, `editMessageReplyMarkup`, `answerCallbackQuery`, webhooks, and `Bot::call` for any other method |

```rust
use std::time::Duration;
use nago_chat::telegram::{Bot, SendMessage};

let bot = Bot::new(token)?;
nagoya::block_on(async {
    let mut offset = None;
    loop {
        for update in bot.get_updates(offset, Duration::from_secs(25), &["message"]).await? {
            offset = Some(update.update_id + 1);
            let Some(message) = update.message else { continue };
            if message.command().is_some_and(|c| c.name == "/start") {
                bot.send_message(&SendMessage::new(message.chat.id, "hello")).await?;
            }
        }
    }
})
```

Requests run on nago-http's own reactor thread, so a `Bot`'s futures complete
under any executor, and concurrent calls proceed concurrently: a long poll does
not hold up a send.

The bot token is part of every request path. No error or `Debug` output from
this crate carries it.

## Releasing

Bump `version` in `Cargo.toml` on `master`; the publish workflow releases
whatever version crates.io does not have yet.

## License

MIT OR Apache-2.0.
