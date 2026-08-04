# FediTexter
Clustered texting platform that works like others on the market
Similar to how a federated system works
People should be able to make there own instances of the server even a version that allow for their own clusters
Currently thinking of using rust + wgpu for the backend and gui something lightweight

Each server should be able to talk to each other to coordinate logins, getting text and what links to use
At the beginning stickers should be stored on the client side and access when required probs will transition to stickers being hosted server side with a cache client side
Files should be sent peer2peer to save on hosting costs

With p2p connections should make it so ip addresses are not given out for each user should be a hidden in a code that is unique to each connection 

Decisions should be made with a majority of developers approval and with all devs knowledgable with what is going on

Currently wanting to implement:
2fa need to be a requirement, stuff like google authenticator should be standard
Probs requiring a phone number for accounts to avoid spam on the service 
create uuid for each device that makes an account and link those uuids to phone numbers (this should not affect privacy too much and this data will be deleted soon after account creation (maybe build a trust score of it and hold account info for less time if its considered trustworthy)
Allowing for sticker/emoji packs 
Gifs (these can be hosted externally) if possible allow for a similar thing to telegram where videos can be converted to gifs if user wants (make these easy to make and an option to not put them into for gif selection)
Text
Link embedding (this need to work for bsky, twitter, youtube and instagram (if possible) will allow for links to be auto converted to fx links if needed) do allow for mastodon as well 
Images (allow for use of getting from gallery but also attempt to use the new camera apis that allow for nice looking photos)
Video (this should be sent p2p)
Voice chats (this can also be p2p)
Sending files
Group chats
Large group chats (this would be a more discord server style ui)
Blocking users is required by google play
Under 16 ban is also required but probs will be an under 18 ban to be safe (this project is in Aus region so need to follow local laws)
(maybe) video chats


!! Never implement:
AI features for example text gen, image gen
Avoid using AI to write code (finding bugs is ok, bug reports do need to be written by a person and tested properly by that person)
Coder UI (unless that ui is not final/shipped, does not include beta testers)

Will be making a system to allow for people to easily contact me for now just use the github systems 
