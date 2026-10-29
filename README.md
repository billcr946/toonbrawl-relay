# Toon Brawl online relay

Lets viewers anywhere play Toon Brawl against a streamer running Toonlight Brawl. It only passes button presses and fight frames between the streamer's app and players' browsers; the player page comes from the streamer's app while it's connected. No accounts, nothing stored.

Run it on any Node.js 18+ host with WebSockets (e.g. Render Web Service): start command `npm start`. Check `/health`.

In Toonlight Brawl: Brawl tab → Let viewers play → Players from anywhere → paste this service's address.
