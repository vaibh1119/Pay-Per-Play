module AptStream::streaming {
    use std::signer;
    use std::vector;
    use std::timestamp;
    use std::event;
    use aptos_framework::coin;
    use aptos_framework::aptos_coin::{AptosCoin};

    // ===== Error codes =====
    const E_NOT_CREATOR: u64 = 1;
    const E_NOT_STREAM_OWNER: u64 = 2;
    const E_STREAM_NOT_FOUND: u64 = 3;
    const E_INSUFFICIENT_DEPOSIT: u64 = 4;
    const E_RATE_TOO_LOW: u64 = 5;
    const E_ASSET_NOT_FOUND: u64 = 6;

    // ===== Data =====
    /// NOTE: This simple version allows only ONE asset per creator (content_id must match).
    struct Asset has key {
        creator: address,
        content_id: vector<u8>,
        base_rate: u64, // octas per second
    }

    /// NOTE: This simple version allows only ONE active stream per user.
    struct Stream has key {
        user: address,
        creator: address,
        content_id: vector<u8>,
        rate: u64,
        start_ts: u64,
        last_debit_ts: u64,
        paid: u64,
        deposit: u64,
        paused: bool,
    }

    // ===== Events =====
    struct Events has key {
        asset_registered: event::EventHandle<AssetRegistered>,
        stream_opened: event::EventHandle<StreamOpened>,
        stream_debited: event::EventHandle<StreamDebited>,
        stream_closed: event::EventHandle<StreamClosed>,
    }

    struct AssetRegistered has drop, store { creator: address, content_id: vector<u8>, base_rate: u64 }
    struct StreamOpened   has drop, store { user: address, creator: address, content_id: vector<u8>, rate: u64, deposit: u64 }
    struct StreamDebited  has drop, store { user: address, creator: address, content_id: vector<u8>, amount: u64, upto_ts: u64 }
    struct StreamClosed   has drop, store { user: address, creator: address, content_id: vector<u8>, paid: u64, refund: u64 }

    /// One global event resource stored at @AptStream
    struct EventStore has key { events: Events }

    // ===== Init =====
    public entry fun init_events(account: &signer) {
        move_to(account, EventStore {
            events: Events {
                asset_registered: event::new_event_handle<AssetRegistered>(account),
                stream_opened:   event::new_event_handle<StreamOpened>(account),
                stream_debited:  event::new_event_handle<StreamDebited>(account),
                stream_closed:   event::new_event_handle<StreamClosed>(account),
            }
        })
    }

    fun events(): &mut Events acquires EventStore {
        &mut borrow_global_mut<EventStore>(@AptStream).events
    }

    // ===== Creator: register content + default rate =====
    public entry fun register_asset(creator: &signer, content_id: vector<u8>, base_rate: u64) {
        let addr = signer::address_of(creator);
        // Simple model: store a single Asset under the creator account.
        move_to(creator, Asset { creator: addr, content_id, base_rate });
        event::emit_event<AssetRegistered>(&mut events().asset_registered, AssetRegistered {
            creator: addr, content_id, base_rate
        });
    }

    // ===== User: open a stream (Coin version; called internally) =====
    public entry fun open_stream(
        user: &signer,
        creator: address,
        content_id: vector<u8>,
        rate: u64,
        mut deposit: coin::Coin<AptosCoin>
    ) acquires Asset, EventStore {
        // Validate asset & fair rate
        assert!(exists<Asset>(creator), E_ASSET_NOT_FOUND);
        let a = borrow_global<Asset>(creator);
        assert!(a.content_id == content_id, E_ASSET_NOT_FOUND);
        assert!(rate >= a.base_rate, E_RATE_TOO_LOW);

        let now = timestamp::now_seconds();
        let user_addr = signer::address_of(user);
        let deposit_amt = coin::value<AptosCoin>(&deposit);
        assert!(deposit_amt > 0, E_INSUFFICIENT_DEPOSIT);

        // Simple model: only one active Stream resource per user.
        move_to(user, Stream {
            user: user_addr,
            creator,
            content_id,
            rate,
            start_ts: now,
            last_debit_ts: now,
            paid: 0,
            deposit: deposit_amt,
            paused: false,
        });

        // Move deposited coins into module custody (treasury at @AptStream must have CoinStore<AptosCoin>)
        coin::deposit(@AptStream, deposit);

        event::emit_event<StreamOpened>(&mut events().stream_opened, StreamOpened {
            user: user_addr, creator, content_id, rate, deposit: deposit_amt
        });
    }

    /// Convenience wrapper so frontends/CLI can pass a u64 deposit amount.
    public entry fun open_stream_with_amount(
        user: &signer,
        creator: address,
        content_id: vector<u8>,
        rate: u64,
        deposit_amount: u64
    ) acquires Asset, EventStore {
        let coin_obj = coin::withdraw<AptosCoin>(user, deposit_amount);
        open_stream(user, creator, content_id, rate, coin_obj);
    }

    /// Let the user add more funds to an active stream.
    public entry fun top_up_with_amount(user: &signer, amount: u64) acquires Stream {
        let addr = signer::address_of(user);
        assert!(exists<Stream>(addr), E_STREAM_NOT_FOUND);
        let mut s = borrow_global_mut<Stream>(addr);
        let add = coin::withdraw<AptosCoin>(user, amount);
        let added = coin::value<AptosCoin>(&add);
        coin::deposit(@AptStream, add);
        s.deposit = s.deposit + added;
    }

    // ===== Internal: compute charges due since last_debit =====
    fun debit_due(stream: &mut Stream, now: u64): u64 {
        if (stream.paused) { return 0 };
        let elapsed = now - stream.last_debit_ts;
        let due = (elapsed as u128) * (stream.rate as u128);
        let due_u64 = due as u64;
        let pay = if (due_u64 > stream.deposit) stream.deposit else due_u64;
        stream.last_debit_ts = now;
        stream.paid = stream.paid + pay;
        stream.deposit = stream.deposit - pay;
        pay
    }

    /// Anyone can settle (useful for cron/worker). Auto-closes if deposit reaches zero.
    public entry fun settle(user: address) acquires Stream, EventStore {
        assert!(exists<Stream>(user), E_STREAM_NOT_FOUND);
        let now = timestamp::now_seconds();
        let stream = borrow_global_mut<Stream>(user);
        let amount = debit_due(stream, now);
        if (amount > 0) {
            // pay creator from module custody
            let mut pool = coin::withdraw<AptosCoin>(@AptStream, amount);
            coin::deposit<AptosCoin>(stream.creator, pool);
            event::emit_event<StreamDebited>(&mut events().stream_debited, StreamDebited {
                user: stream.user, creator: stream.creator, content_id: stream.content_id, amount, upto_ts: now
            });
        };
        if (stream.deposit == 0) {
            let s = move_from<Stream>(user);
            event::emit_event<StreamClosed>(&mut events().stream_closed, StreamClosed {
                user: s.user, creator: s.creator, content_id: s.content_id, paid: s.paid, refund: 0
            });
        }
    }

    public entry fun pause(user: &signer) acquires Stream {
        let addr = signer::address_of(user);
        assert!(exists<Stream>(addr), E_STREAM_NOT_FOUND);
        let s = borrow_global_mut<Stream>(addr);
        s.paused = true;
    }

    public entry fun resume(user: &signer) acquires Stream {
        let addr = signer::address_of(user);
        assert!(exists<Stream>(addr), E_STREAM_NOT_FOUND);
        let s = borrow_global_mut<Stream>(addr);
        s.paused = false;
        s.last_debit_ts = timestamp::now_seconds();
    }

    /// Close: settle then refund leftover to user
    public entry fun close(user: &signer) acquires Stream, EventStore {
        let addr = signer::address_of(user);
        assert!(exists<Stream>(addr), E_STREAM_NOT_FOUND);
        // Final settle
        settle(addr);
        // If settle auto-closed due to zero deposit, nothing to refund.
        if (!exists<Stream>(addr)) return;
        // Otherwise remove and refund remaining deposit
        let s = move_from<Stream>(addr);
        let refund = s.deposit;
        if (refund > 0) {
            let mut change = coin::withdraw<AptosCoin>(@AptStream, refund);
            coin::deposit<AptosCoin>(addr, change);
        }
        event::emit_event<StreamClosed>(&mut events().stream_closed, StreamClosed {
            user: s.user, creator: s.creator, content_id: s.content_id, paid: s.paid, refund
        });
    }

    // ===== Read helper for dashboards (copyable output) =====
    #[view]
    public fun stream_info(addr: address): (bool, address, address, u64, u64, u64, u64, bool) acquires Stream {
        if (!exists<Stream>(addr)) return (false, @0x0, @0x0, 0, 0, 0, 0, false);
        let s = borrow_global<Stream>(addr);
        (true, s.user, s.creator, s.rate, s.start_ts, s.last_debit_ts, s.deposit, s.paused)
    }
}