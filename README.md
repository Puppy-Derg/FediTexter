# FediTexter

Clustered texting platform that works like others on the market. Similar to how a federated system works. People should be able to make their own instances of the server, even a version that allows for their own clusters. Currently thinking of using Rust + wgpu for the backend and GUI, something lightweight.

## Architecture

- Each server should be able to talk to each other to coordinate logins, getting text and what links to use.
- At the beginning, stickers should be stored on the client side and accessed when required. Will probably transition to stickers being hosted server side with a cache client side.
- Files should be sent peer-to-peer to save on hosting costs.
- With p2p connections, IP addresses should not be given out for each user. Each should be hidden in a code that is unique to each connection.

## Decision Making

Decisions should be made with a majority of developers approval, with all devs knowledgeable about what is going on.

## Currently Wanting to Implement

- **2FA** — needs to be a requirement; stuff like Google Authenticator should be standard.
- **Phone number verification** — probably requiring a phone number for accounts to avoid spam on the service.
- **Device UUIDs** — create a UUID for each device that makes an account and link those UUIDs to phone numbers (this should not affect privacy too much, and this data will be deleted soon after account creation). Maybe build a trust score of it and hold account info for less time if it is considered trustworthy.
- **Sticker/emoji packs**
- **GIFs** — can be hosted externally. If possible, allow for a similar thing to Telegram where videos can be converted to GIFs if the user wants. Make these easy to make with an option to not put them in for GIF selection.
- **Text**
- **Link embedding** — needs to work for bsky, Twitter, YouTube, Instagram (if possible) and Mastodon. Allow links to be auto converted to fx links if needed.
- **Images** — allow for getting from gallery, but also attempt to use the new camera APIs that allow for nice looking photos.
- **Video** — should be sent p2p.
- **Voice chats** — can also be p2p.
- **Sending files**
- **Group chats**
- **Large group chats** — a more Discord server style UI.
- **Blocking users** — required by Google Play.
- **Under 16 ban** — also required, but will probably be an under 18 ban to be safe (project is in the Aus region so needs to follow local laws).
- **(maybe) Video chats**

## Never Implement

- AI features, for example text gen, image gen.
- Avoid using AI to write code (finding bugs is OK; bug reports do need to be written by a person and tested properly by that person).
- Coder UI (unless that UI is not final/shipped, does not include beta testers).
- Never allow people to ramble like the telegram dev does (just dont put it infront of all users faces without consent)

## Contact

Will be making a system to allow for people to easily contact me; for now, just use the GitHub systems.
