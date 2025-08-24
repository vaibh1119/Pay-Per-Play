🚀 Project Overview

Name: Pay Per Play (AptStream)

Tagline: Stream music, movies, or web series — pay only for what you watch/listen to, per second, using Aptos micro-transactions.

Problem: Current streaming platforms charge subscriptions even if you consume little. Creators are underpaid; users overpay.

Solution: We leverage Aptos Move smart contracts + fast, low-fee blockchain to allow pay-per-second streaming.

🏗️ Architecture

Frontend: React + Vite + Aptos Wallet Adapter (Petra/Martian).

Backend (on-chain): Aptos Move module AptStream::streaming.

Wallets: Petra, Martian (testnet).

Flow:

User connects wallet → chooses content.

open_stream_with_amount escrows deposit.

As playback progresses, settle/close pays creator & refunds leftover.

UI polls stream_info to show real on-chain usage + balance.

(Insert a diagram.png here if you can — judges love a simple flowchart.)

⚙️ Setup & Installation
Prerequisites

Node.js (>=18)

Aptos CLI (>=3.0.0)

Testnet wallet (Petra/Martian)

Clone
git clone https://github.com/<your-username>/aptstream.git
cd aptstream

Backend (Move)
cd move
# compile
aptos move compile --profile aptstream
# publish (replace with your address)
aptos move publish --profile aptstream --named-addresses AptStream=0xYOURADDR --assume-yes
# one-time init
aptos move run --profile aptstream --function-id 0xYOURADDR::streaming::init_events

Frontend
cd frontend
cp .env.example .env
npm install
npm run dev

🔑 Environment Variables (.env)
VITE_NETWORK=testnet
VITE_MODULE_ADDR=0xYOURADDR
VITE_CREATOR_ADDR_1=0xCREATOR1ADDR
VITE_CREATOR_ADDR_2=0xCREATOR2ADDR
VITE_CREATOR_ADDR_3=0xCREATOR3ADDR

🎬 Demo Flow (Judge Checklist)

Open live site (add your Vercel/Netlify URL).

Connect wallet (Petra on testnet).

Click “Aptos Anthem” → transaction pops up. Confirm.

Media starts; deposit leaves wallet.

After 20–30s, click “Stop”.

On-chain:

aptos account list --account 0xUSER
aptos account list --account 0xCREATOR
aptos move view --function-id 0xYOURADDR::streaming::stream_info --args address:0xUSER


→ User’s balance decreased, creator’s balance increased. ✅ Payment occurred.

📸 Screenshots

Insert screenshots of Available Content screen & Now Playing.

(Optional) short GIF or Loom link of demo.

📜 Smart Contract API

register_asset(creator, content_id: vector<u8>, base_rate: u64)

open_stream_with_amount(user, creator, content_id, rate, deposit_amount)

pause(user), resume(user)

settle(user: address)

close(user)

#[view] stream_info(addr: address)
