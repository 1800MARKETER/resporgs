# Somos TFN Registry API — Reference

Reference notes for building the reservation system. Source: `TFNR_APIs_Customer_V3_17.json` (Bill's Downloads, May 4 2026), the Postman collection (`Login Workflow.postman_collection.json`), plus what we've measured live.

## Endpoints

| Environment | Host | Base path |
|---|---|---|
| **Live** | `api-tfnregistry.somos.com` | `/v3/ip` |
| Sandbox | `sandbox-api-tfnregistry.somos.com` | `/v3/ip` |

All endpoints are HTTPS. The host is fronted by **Imperva (Incapsula) anycast**. From our Chicago box (`149.28.119.6`) we measured **3.16 ms RTT** to the edge — Bill's account specifically lands at the Chicago Imperva PoP regardless of where the actual Somos origin sits.

A v2 API also exists (`/v2/ip/...`), used by the older Postman collection. **Use v3** for new work.

## Auth flow

Token-based. Two-step:

1. `POST /sec/session/open` with `{usrName, password}` returns:
   - `oauthToken` (Bearer, **60 min** lifetime)
   - `refreshToken` (24 hr lifetime — use to mint new oauth tokens without re-logging in)
   - `clientKey` + `clientSecret` (for OAuth2.0 password/refresh grants if you prefer that path)
   - `expiresIn` (seconds until oauthToken expires)
2. Subsequent calls send `Authorization: Bearer <oauthToken>`
3. When 401 / token expired, hit `POST /token` with the refreshToken to get a new oauthToken (no re-login)
4. `PUT /sec/session/close` to release session politely

Don't open a new session per call — Somos tracks active sessions and there are limits. Cache the token for ~55 min, refresh on demand.

For our reservation API, this means a small token-manager class:
- Lazy-load token on first call
- Track expiry; refresh in background ~5 min before expiration
- On 401, force a refresh and retry once

## Endpoints we actually need (Phase 1 + 2)

### Reserving numbers

| Endpoint | What it does |
|---|---|
| `POST /num/tfn/reserve` | Reserve specific TFN(s) you already know you want |
| `POST /num/tfn/srchres/specific` | Search-AND-reserve specific number(s) — atomic |
| `POST /num/tfn/srchres/random` | Search-AND-reserve N random numbers from a pool |
| `POST /num/tfn/srchres/wildcard` | Search-AND-reserve matching a wildcard pattern |
| `GET /num/res/limits` | Check our current reservation usage vs quota (2000) |

**Click-time path** uses `POST /num/tfn/reserve` (we already know which TFN — the one the customer clicked). One round trip to Somos. With a warm TLS connection, that's ~5 ms request + ~5 ms response = ~10 ms total before we redirect the customer to Shopify.

### `POST /num/tfn/reserve` request body (`reserveNUSRTfnRequest`)

Required:
- `conName` (string) — Customer or contact person name initiating the reservation
- `conTel` (string) — Customer / contact phone
- `numList` (array of TFNs) — **the numbers to reserve**

Optional:
- `shrtNotes` (string) — Free-text notes attached to reservation
- `asyncId` (string) — For async response handling

Response codes:
- 200 — Success (reservation created)
- 202 — Timeout (kicked into async queue, poll later)
- 400 / 401 / 408 / 429 / 500 / 503 — assorted failures

Plan: pass the customer's email as `conName`, our office number as `conTel`, and the single TFN as a 1-element `numList`. Use 200 = success path. Treat 202 as failure for click-time (we don't have time to poll); customer goes to lead-capture flow.

### Pre-flight check (before/instead of reserve)

| Endpoint | What it does |
|---|---|
| `GET /num/tfn/query/tfn/{num}` | Current state of a specific TFN (status, owning RespOrg) |
| `GET /num/tfn/query` | Query with filters |
| `GET /org/resporg/num/{TFN}` | Lookup the RespOrg currently holding a TFN |

Useful pattern: query first, only call reserve if status looks reservable (faster failure path on numbers we don't actually own / aren't available). But for click-time, just go straight to `/num/tfn/reserve` — if it fails, fall back to lead capture. One round trip is faster than two.

### Releasing reservations

The 6-week reservation auto-expires. To release earlier, look at the customer-record APIs (`/cus/rec/disconnect`) or `/cus/rec/transfer` for actively transferring to another RespOrg. The pure "drop the reservation" path may need clarification with Somos — the spec has a `cus/rec` lifecycle but doesn't have an obvious "unreserve" verb. Worth testing in sandbox.

## Endpoints we'll likely need later (Catcher Phase)

| Endpoint | Phase |
|---|---|
| `POST /num/oca/random` | Pre-warming — bulk random search of available numbers |
| `POST /num/oca/wildcard` | Pre-warming — find numbers matching a vanity pattern |
| `GET /num/and` | Number Administration — possibly for monitoring drops |
| `GET /num/trq` | Trade Query — recent activity |
| `PUT /cus/rec/lock` | Lock a customer record (after order completes) |
| `POST /cus/rec` | Create a customer record (final activation step) |

## Reservation behavior — what to know

| | |
|---|---|
| Max simultaneous reservations | **2,000** |
| We typically hold | "a couple hundred" — comfortable margin |
| Default reservation duration | **6 weeks** |
| Renewal | A reservation auto-renews into a customer record when activated; otherwise it expires |
| "Sparing" | Bill explicitly releases unwanted reservations; this is the only way to free quota before the 6-week timeout |

## Where credentials live

`/var/www/resporgs/somos.env` on the Chicago box (mode 600, www-data, gitignored). Currently placeholders:

```
SOMOS_API_BASE=https://api-tfnregistry.somos.com
SOMOS_USERNAME=<FILL_IN>
SOMOS_PASSWORD=<FILL_IN>
SOMOS_RESPORG_ID=<FILL_IN>
```

Bill populates these from his Postman environment when ready to ship Phase 1. The reservation API (Flask blueprint) will load them via `EnvironmentFile=/var/www/resporgs/somos.env` in its systemd unit.

For sandbox testing, switch `SOMOS_API_BASE` to `https://sandbox-api-tfnregistry.somos.com` — same auth credentials should work (Somos issues a sandbox-equivalent of the prod login).

## Other endpoints worth knowing about

The full v3 spec has **272 endpoints** across these top-level groups:

- **`/sec`** — Authentication/session, password management
- **`/num`** — Number administration: search, reserve, query, transfer
- **`/cus`** — Customer Records (the routing data — what makes a number "Working" beyond just reserved)
- **`/org`** — Resp Org administration: who owns what, RespOrg metadata
- **`/car`** — Carrier admin: CIC codes, IXC info, etc.
- **`/scp`** — SCP (Service Control Point) administration: routing infrastructure
- **`/sys`** — Bulk operations + system automation
- **`/rpt`** — Reporting (e.g., `/rpt/tfn/queryHistory` for full TFN ownership history)
- **`/adm`** — Network administration (NPA/NXX exclusion lists, etc.)
- **`/clientCredentials`**, **`/token`**, **`/revoke`** — OAuth2.0 endpoints

For the reservation work we'll touch maybe 5-7 endpoints. Most of the 272 are for full Resp Org operations that mom handles manually downstream.

## Implementation sketch (next session)

```python
# scripts/somos_api/client.py

class SomosClient:
    def __init__(self):
        self.base = os.environ['SOMOS_API_BASE']
        self.user = os.environ['SOMOS_USERNAME']
        self.pwd  = os.environ['SOMOS_PASSWORD']
        self._token = None
        self._expires_at = 0
        # Persistent HTTP/1.1 keepalive connection; HTTP/2 if Somos supports it.
        self._sess = requests.Session()
        self._sess.mount('https://', HTTPAdapter(
            pool_connections=2, pool_maxsize=4,
        ))

    def _token_valid(self) -> bool:
        return self._token and time.time() < self._expires_at - 60

    def _login(self):
        r = self._sess.post(f"{self.base}/v3/ip/sec/session/open",
            json={"usrName": self.user, "password": self.pwd},
            timeout=10)
        r.raise_for_status()
        d = r.json()
        self._token = d['oauthToken']
        self._expires_at = time.time() + d['expiresIn']

    def _auth_headers(self):
        if not self._token_valid():
            self._login()
        return {"Authorization": f"Bearer {self._token}"}

    def reserve(self, tfn: str, contact_name: str, contact_tel: str,
                notes: str = "") -> dict:
        r = self._sess.post(
            f"{self.base}/v3/ip/num/tfn/reserve",
            headers=self._auth_headers(),
            json={
                "conName": contact_name,
                "conTel": contact_tel,
                "numList": [tfn],
                "shrtNotes": notes,
            },
            timeout=10,
        )
        # Retry once on 401 (token expired mid-request)
        if r.status_code == 401:
            self._login()
            r = self._sess.post(...)  # retry with new token
        r.raise_for_status()
        return r.json()

    def query(self, tfn: str) -> dict:
        r = self._sess.get(
            f"{self.base}/v3/ip/num/tfn/query/tfn/{tfn}",
            headers=self._auth_headers(),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
```

Plus a `reservations_db.py` to track our local view of what we hold (so we don't re-reserve numbers we already have, and so we can show customers their pending reservations even without hitting Somos every time).

## Questions still open (worth confirming in sandbox)

1. **Release-without-disconnect**: how do we tell Somos "drop this reservation, I changed my mind"? `/cus/rec/disconnect` is for active records, not reservations. Test in sandbox.
2. **Idempotency**: if we send `reserve` twice for the same TFN within 6 weeks (because of network retries), does the second call succeed silently, return success, or fail with "already reserved by you"? Test in sandbox; we want it to be safe to retry.
3. **`asyncId` mechanics**: when does Somos go async (202)? At certain volumes? Always for some endpoints? We need to handle both sync and async paths.
4. **Wildcard vs `srchres/wildcard` for vanity searches**: the catcher daemon will want to auto-grab newly-dropped numbers matching patterns. Need to test which endpoint behaves best for that use case.

Tackle these by spending an hour in Postman with Bill's sandbox creds before writing production code.

## References

- **OpenAPI spec**: `C:\Users\Bill\Downloads\TFNR_APIs_Customer_V3_17.json` (1.5 MB, Swagger 2.0, 272 endpoints, 700+ definitions)
- **Postman collection**: `C:\Users\Bill\Downloads\Login Workflow.postman_collection.json` (v2 endpoints — older but still functional for basic session+profile testing)
- **Live host**: `api-tfnregistry.somos.com` (Chicago Imperva edge from our box)
- **Sandbox host**: `sandbox-api-tfnregistry.somos.com`
- **Developer portal**: `https://developer.somos.com` (login required, shows the same docs)
