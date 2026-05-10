# Reservation System (planned)

The protective layer between cached search results and the Shopify order flow.

## Why this exists

Resporgs.com / TFN.com / VNFS / VN.com all serve searches off cached Somos data
that's only updated monthly. Between cache build (1st of month) and a customer
clicking "Order" (any time), other Resp Orgs grab numbers we still show as
available. Customers reach Shopify and only later learn their number was taken
days ago. Bad UX, refunds, support burden.

The reservation API sits between the click and Shopify: real-time Somos check,
reserve under our RespOrg if available, only then forward the customer to the
cart with a number we now hold.

## Architecture

```
[Customer click on TFN.com / VNFS / VN.com / Resporgs.com]
                    │
                    ▼
           [Site backend — NYC3]
            POST /reserve {tfn}
                    │  ~20 ms NYC↔CHI
                    ▼
       [Chicago box — 149.28.119.6]
        Reservation API
         1. Check our reservation DB (do we already hold it?)
         2. If no, hit Somos ROCNumberRequest (~5 ms RTT)
         3. Persist + return {success, expires_at}
                    │
                    ▼
       [Somos TFN Registry API]
        api-tfnregistry.somos.com (Imperva edge, Chicago)
```

Why the Chicago box for the reservation API: 3.16 ms RTT to Somos vs 20.5 ms
from NYC3. With HTTPS keepalive, that's the difference between racing
competitors at sub-30 ms vs 80+ ms. Resp orgs running automated catchers do
the same thing.

## Somos reference info (from Bill, 2026-05-08)

| Item | Value |
|---|---|
| API base (prod) | `https://api-tfnregistry.somos.com` |
| API base (sandbox) | `https://sandbox-api-tfnregistry.somos.com` |
| Auth pattern | `POST /v2/ip/sec/session/open` returns Authorization token |
| Reservation endpoint | `ROCNumberRequest` (TFN reservation submission) |
| Reservation duration | **6 weeks**, unless explicitly "spared" by us |
| Reservation quota | **2,000 simultaneous holds** |
| Currently holding | "a couple hundred most of the time" — plenty of headroom |
| Where Bill has creds today | Postman environment (sandbox); NOT yet on any server |
| Documentation files (local) | `Downloads/Login Workflow.postman_collection.json`, `Downloads/TFNR_APIs_Customer_V3_17.json` |

## Order flow Bill described

> "The order link currently just goes to shopify. The customer enters the
> ring-to number and adds it to the cart. For the regular $49 activations, the
> most common product, I think we want to reserve the number they're clicking on
> if possible, and then make sure it's not held by any other resporg before we
> send it to the shopping cart."

Two-step at click time:
1. **Verify** the TFN is still available at Somos (not held by another RespOrg)
2. **Reserve** it under our RespOrg (so it's locked for the customer)
3. **Then** redirect to Shopify cart

Most common product: $49 activation. Pre-warming high-value numbers is a
secondary feature that can come later — "we don't really hold too many numbers
but we will work on this more later."

## Failure UX (per Bill)

> "If a number isn't available could we have it offer to send an email for us
> to check it and look for alternatives for them."

So when the click-time reservation fails (number gone):
- Don't hard-error — collect the customer's interest as a lead
- "Sorry, that number was just taken — give us your email and we'll find similar
  options for you"
- The collected leads feed into Bill's existing email-handling workflow (the
  `email_requests` table in `1cup_business.db` already does this for unsolicited
  email requests; reuse the same table or pattern)

## Constraints (don't break Bill's existing operations)

> "We exported a database of customer orders in a project but I'm not sure
> where it is and it's not updating from the shopping cart yet. That's probably
> another big project. We'll get into that when we automate the post order
> process and documentation but I'm reluctant to go too far in that direction
> because my 84 year old mother does it and I don't want to replace her right
> now. It's important for her to be able to help. :)"

**Therefore: build only the click-time reservation step. Do NOT auto-process
post-Shopify orders.** Mom's manual process continues unchanged. The
reservation API just guarantees the number is held when the order arrives at
her queue.

## Build plan

### Phase 1 — Click-time reservation API (next session)

```
scripts/somos_api/
├── client.py                 - thin HTTP client: session/open, ROCNumberRequest, etc.
│                               Reads creds from /var/www/resporgs/somos.env.
│                               HTTP/2 keepalive pool to api-tfnregistry.somos.com.
├── reservations_db.py        - SQLite for our reservation state:
│                                 (tfn, reserved_at, expires_at, status, source,
│                                  customer_id, request_id, somos_resp_json)
├── reserve.py                - core: idempotent reserve(tfn) -> {success, expires_at}
└── tests/
    └── test_client.py
webapp_blueprint/
└── reserve_api.py            - Flask blueprint mounted at /api/reserve
                                Called by consumer sites (TFN.com etc.)
```

Endpoints:
```
POST /api/reserve
  body: {"tfn": "8005551234", "customer_email": "...", "source": "tfn.com"}
  -> 200  {"reservation_id": "...", "tfn": "8005551234",
            "expires_at": "2026-06-19T...", "already_held": false}
  -> 409  {"error": "already_taken", "lead_capture_url": "/lead?tfn=..."}
  -> 500  {"error": "somos_api_error", "detail": "..."}

GET /api/reservations/<id>
POST /api/reservations/<id>/release    (free a reservation we no longer want)
```

Integration on consumer sites (TFN.com, VNFS, etc.):
- Replace direct `<a href="https://shopify.../cart/add?...">` link
- With form submission to `/api/reserve` then redirect to cart on success

### Phase 2 — Pre-warming (later)

Background daemon scans the most valuable currently-AVAIL numbers (using
`master_vanity.db` blended scoring), reserves the top N where N << 2000.
Click-time path treats already-held as instant success.

### Phase 3 — Catcher daemon (separate project, builds on the API client)

The race-window auto-reserver for newly-dropped numbers. Different code path
(systemd timer fires at 23:55 ET to pre-warm TLS, race executor, etc.).
Documented separately in `catcher.md` when we get there.

## Where the credentials go

Already prepped at `/var/www/resporgs/somos.env` on Chicago box:

```
SOMOS_API_BASE=https://api-tfnregistry.somos.com
SOMOS_USERNAME=<FILL_IN>
SOMOS_PASSWORD=<FILL_IN>
SOMOS_RESPORG_ID=<FILL_IN>
```

Bill needs to populate these from his Postman environment before the
reservation API can ship. Mode 600, owned by www-data, in `.gitignore`.

The systemd service for the reservation API will load this env file via
`EnvironmentFile=/var/www/resporgs/somos.env`.

## What's NOT in scope

- Post-Shopify automation (mom's manual process stays)
- Replacement of the 1Cup `1cup_business.db` orders table (separate, larger project)
- Any change to current Shopify checkout — we just protect what arrives there

## Status — 2026-05-08

✅ Chicago box provisioned (149.28.119.6, 3.16 ms to Somos)
✅ Somos API endpoint identified, anycast confirmed, latency benchmarked
✅ Credentials env file ready on Chicago box (placeholders)
✅ Architecture decision: single reservation API on Chicago, all sites call it
🔲 Bill populates `/var/www/resporgs/somos.env` from Postman
🔲 Phase 1 — click-time reservation API + DB (next session)
🔲 Phase 2 — pre-warming
🔲 Phase 3 — catcher (separate doc)
