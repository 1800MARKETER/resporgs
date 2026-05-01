# Resporgs.com — SEO + AI-SEO Analysis & Action Plan
_Drafted 2026-04-28 by Claude. Implementation status tracked inline._

## TL;DR

The site has **313 unique long-tail-keyword pages** at `/r/<rpfx>`, each targeting an identifiable telecom entity ("[Resporg Name]", "[Resporg Name] toll-free", "who owns [number]"). Most of those names have **near-zero existing search competition** — the highest-leverage SEO opportunity in the project. But the site is missing four foundational SEO assets that turn each profile page from "exists" to "discoverable, extractable, and citable":

1. **No robots.txt** — AI crawlers (GPTBot, PerplexityBot, ClaudeBot, Google-Extended) have no explicit invitation
2. **No sitemap.xml** — search engines crawl the site by accident, not by design
3. **No structured data (schema.org)** — research shows 30-40% AI citation lift from schema; we have none
4. **No meta descriptions / Open Graph tags** — every shared link looks generic

Adding these four, plus the page-level enhancements below, will shift the asymmetric advantage from "we have unique data" to "we have unique data AND it's optimally indexable."

## The opportunity, quantified

| Tier | Description | # Resporgs | Combined inventory | Why it matters |
|---|---|---:|---:|---|
| **Mid-tier sweet spot** | 50K–1M numbers, low/medium AI-overview confidence (= low public-info competition) | 46 | **11.2M numbers** | Bill's target — high inventory, easy to rank, real lead conversion potential |
| **Long-tail obscure** | < 50K numbers, often single-employee shops with no website | ~150 | ~6M numbers | Even easier to rank but less search volume |
| **Major brands** | > 1M numbers (AT&T, Verizon, Bandwidth, Comcast, Twilio) | 12 | ~30M numbers | Heavy SERP competition; we won't win generic queries but can win "[Brand] toll-free" longtail |
| **Misdial / Vanity-Biz** | Skipped — not customer-facing | 77 | ~30M numbers | Still rank-worthy as evidence pages, but not lead targets |

**Top 30 mid-tier targets** (highest inventory × lowest public competition):

| RespOrg | Code | Inventory | Public-info confidence |
|---|---|---:|---|
| ATL Communications | AUN01 | 919,838 | medium |
| Commercial LT Baroda | JJJ01 | 902,344 | (no data) |
| Inteliquent | LQX01 | 903,396 | medium |
| Inteliquent | JFT01 | 805,322 | medium |
| Google Voice | GOG01 | 678,684 | medium |
| Inteliquent | QYR01 | 526,011 | medium |
| RingCentral | RBI01 | 496,964 | (no data) |
| Five9 | BPS01 | 452,406 | medium |
| Incontact | BCU01 | 446,365 | medium |
| Telnyx | QIA01 | 325,939 | low |
| Beta Texting | HTC01 | 232,100 | low |
| Wide Voice | WAV01 | 265,315 | low |
| ANI Networks | GDG01 | 128,926 | medium |
| Peerless Network | PNN01 | 127,576 | medium |
| TouchTone | QTS01 | 122,912 | low |
| Allstream Business | UXC01 | 118,181 | low |
| Veracity | VTC01 | 116,496 | low |
| Magna5 | NVT01 | 93,869 | low |
| Serenova | HHG01 | 93,691 | low |

These 30 alone hold **8.5M numbers**. Each page could rank in the top 3 for "[Company Name] toll-free numbers" with foundational SEO work.

---

## Priority 1 — Foundational infrastructure (ship today)

### 1.1 robots.txt

Currently 404. Add `/robots.txt` with explicit AI crawler invitations + sitemap reference:

```
# Resporgs.com — robots policy
User-agent: *
Allow: /

# AI search crawlers — explicitly welcomed
User-agent: GPTBot
Allow: /
User-agent: ChatGPT-User
Allow: /
User-agent: PerplexityBot
Allow: /
User-agent: ClaudeBot
Allow: /
User-agent: anthropic-ai
Allow: /
User-agent: Google-Extended
Allow: /
User-agent: Bingbot
Allow: /

Sitemap: https://resporgs.com/sitemap.xml
```

### 1.2 sitemap.xml

Generate dynamically at `/sitemap.xml` covering:
- All 313 `/r/<rpfx>` pages
- Category pages (`/category/<slug>`) and group pages (`/group/<slug>`)
- Static pages (/, /directory, /news, /pool, /faq, /transferring)
- Each entry with `lastmod` reflecting Sanity's `_updatedAt`

### 1.3 Meta tags + Open Graph in `base.html`

Add to `<head>`:
- `<meta name="description" content="{{ page_description or default }}">`
- `<meta property="og:title" content="...">`
- `<meta property="og:description" content="...">`
- `<meta property="og:type" content="website">`
- `<meta property="og:image" content="...">` (logo or screenshot)
- `<meta name="twitter:card" content="summary_large_image">`

Each page passes its specific description through the template context. Profile pages can use the AI-generated overview as the meta description (perfect 2-3 sentence summary).

### 1.4 Schema.org Organization + WebPage markup on resporg profiles

Highest-leverage single change. Add JSON-LD block to `profile.html`:

```json
{
  "@context": "https://schema.org",
  "@type": "Organization",
  "name": "ATL Communications",
  "alternateName": "AUN01",
  "url": "https://atlcommunications.com",
  "description": "...AI overview...",
  "address": { "@type": "PostalAddress", "addressLocality": "...", "addressRegion": "..." },
  "telephone": "...",
  "logo": "https://...",
  "sameAs": ["https://atlcommunications.com"],
  "subjectOf": {
    "@type": "Article",
    "headline": "ATL Communications RespOrg Profile (AUN01) - Toll-Free Number Inventory",
    "datePublished": "...",
    "dateModified": "..."
  }
}
```

Plus a `WebPage` block with `breadcrumb` and `mainEntity` references for richer SERP snippets.

---

## Priority 2 — Page-level content optimizations

### 2.1 H1 / H2 / H3 hierarchy that matches query patterns

Current H1 is `{{ title }} {{ rpfx }}`. Search-friendly version:

- H1: `{{ title }} ({{ rpfx }}) — Toll-Free RespOrg Profile`
- H2: `What does {{ title }} do?` (for "what is" queries)
- H2: `{{ title }} toll-free number inventory` (for "X toll-free" queries)
- H2: `Recent news about {{ title }}` (already in place)
- H2: `Get a toll-free number from {{ title }}` (conversion-focused)
- H2: `Frequently asked questions about {{ title }}` (FAQ → FAQPage schema)

The headings should literally match how people search. "What does X do" gets cited by AI for definitional queries.

### 2.2 FAQPage schema with auto-generated questions

For every resporg, programmatically generate 5–8 FAQs from existing data:

- *"What is {{ title }}?"* → AI overview as answer
- *"How many toll-free numbers does {{ title }} have?"* → from `total_inv`
- *"Where is {{ title }} headquartered?"* → from address
- *"How do I request a toll-free number from {{ title }}?"* → CTA + form link
- *"What carriers does {{ title }} use?"* → from group/category
- *"Has {{ title }}'s toll-free inventory grown or shrunk?"* → from delta data we already compute
- *"Can I port my toll-free number to {{ title }}?"* → standard answer
- *"Does {{ title }} support text-enabled toll-free?"* → standard answer

Each Q+A becomes a `FAQPage` schema item — these get extracted directly into Google AI Overviews.

### 2.3 Statistics-rich content blocks (40% AI citation lift)

Per the Princeton GEO study, statistics with sources boost AI citation by 37-40%. Every resporg page already has stats — just expose them in extractable form:

```html
<section class="key-stats" itemscope itemtype="https://schema.org/Dataset">
  <p>{{ title }} manages <strong>{{ total_inv | format }} active toll-free numbers</strong>
     as of {{ month }}, ranking <strong>#{{ inv_rank }}</strong> among
     {{ total_resporgs }} US toll-free RespOrgs by inventory size.
     Inventory has changed by {{ delta_pct }}% over the past 4 years
     ({{ delta }} numbers).</p>
</section>
```

This is the highest-citation-yield single change for AI SEO. The page is already computing all these numbers; this just packages them for extraction.

### 2.4 Self-contained answer paragraphs (40-60 words each)

Every section's first paragraph should answer the implicit question completely so AI can quote it without surrounding context. Example for the inventory section:

> "ATL Communications (AUN01) holds 919,838 active toll-free numbers as of April 2026, ranking among the top 50 US RespOrgs by inventory size. The company has [grown/shrunk] inventory by X% over the past four years, with [Y] new acquisitions and [Z] disconnects in that window."

That's 50 words, completely self-contained, statistics-rich, citation-ready.

### 2.5 Conversion CTA repositioning

Current CTA (`Request a specific #`) appears once per page. For conversion optimization:

- Place CTA above the fold (in header card next to logo)
- Repeat after the inventory stats block ("Looking for a specific number from {{ title }}? Request it here →")
- Repeat after the news section
- Add inline CTAs in the "How do I request a number from X?" FAQ answer
- Mention the CTA in the meta description so people clicking through from SERP know what to expect

---

## Priority 3 — AI SEO specifics

### 3.1 Citable framing — "Resporgs.com tracks..."

AI systems cite sources by name + URL. Add an authoritative-tone introduction to every page:

> "Resporgs.com tracks the toll-free inventory of every US RespOrg using monthly Somos registry data. This profile of {{ title }} ({{ rpfx }}) reflects the {{ month }} scan."

This single sentence is a candidate for direct AI quotation. It establishes:
- Authority ("tracks every RespOrg")
- Methodology ("monthly Somos registry data")
- Freshness ("the {{ month }} scan")
- Specificity ("{{ title }} ({{ rpfx }})")

### 3.2 Definition-first openings

Instead of leading with marketing ("ATL Communications profile"), lead with definition:

> "ATL Communications (RespOrg code AUN01) is a US toll-free service provider managing 919,838 numbers as of April 2026. RespOrg AUN01 was [established/active since] in [year/window]; the entity is based in [location]."

Definition openings get cited 3x more by AI for "What is" queries.

### 3.3 The "vs" and "best" page strategy

Long-term: programmatically generate comparison pages between pairs of mid-tier RespOrgs in the same category. "[X] vs [Y] for toll-free numbers" — AI search loves these (33% of all citations are comparisons per the platform-ranking-factors data). We have the inventory data, growth data, geography data. Programmatic comparison pages at scale.

This is a Phase 2 project (not a quick win) but worth queuing.

### 3.4 Internal linking to build entity authority

Every resporg page should link to:
- Its category page (already done via tag chips)
- Its group page (already done)
- 3-5 "related RespOrgs" (same category, similar size) — currently NOT linked
- The /news page (should be linked from each profile's news section header)
- The category's parent industry concept (if defined)

Internal links pass entity context to AI. A page with 10 in-bound internal links from semantically related pages becomes the "canonical" page for that entity.

---

## Priority 4 — Off-site presence

Per the Princeton research: brands are 6.5x more likely to be cited via third-party sources than their own site. The mid-tier resporgs in our target set typically have:

- ❌ No Wikipedia page
- ❌ No Crunchbase profile
- ❌ Few or no industry-publication mentions
- ❌ No reviews on G2/Capterra (most aren't B2B SaaS)

We can't fix that for them, but we CAN:

- Write the Wikipedia-quality summary OURSELVES (which we now have via Sonar Pro) and host it as the canonical reference
- Submit citations to Wikipedia where Resporgs.com is the primary source for a fact (e.g., RespOrg code, founding year, inventory size)
- Reach out to small-business / telecom trade publications offering our data as source material

---

## Priority 5 — Monitoring

Set up a monthly check:

```
For each of the top 30 mid-tier RespOrgs:
  query Google for: "{{ title }} toll-free"
  query Google for: "{{ title }} RespOrg"
  query ChatGPT for: "Who is {{ title }}?"
  query Perplexity for: "{{ title }} toll-free service"
  Record: ranking position, AI Overview presence, are we cited?
```

This is a manual tracker initially (spreadsheet). Once the foundational work ships, automate via the same Sonar Pro pipeline we're using for resporg news.

---

## Implementation status (quick wins shipping today)

- [x] Analysis complete (this document)
- [ ] robots.txt
- [ ] sitemap.xml route
- [ ] Meta description + Open Graph in base.html
- [ ] Organization + FAQPage schema in profile.html
- [ ] Statistics-rich content block
- [ ] CTA repositioning (above fold + repeat)
- [ ] Definition-first opening paragraph
- [ ] "Resporgs.com tracks..." citable framing

Phase 2 (Bill's call):
- [ ] Programmatic [X] vs [Y] comparison pages for mid-tier resporgs
- [ ] Related-resporgs internal linking
- [ ] Wikipedia / off-site presence push
- [ ] Monthly AI-visibility monitoring

---

## Estimated impact

If the priority 1+2 work ships:

| Metric | Current | Estimated after | Why |
|---|---|---|---|
| Resporg profile pages indexed by Google | ~unknown | 313 of 313 | sitemap + robots.txt fix |
| Pages with AI Overview citation potential | ~0 | ~150+ | schema + structured content |
| AI citation rate on "Who is [RespOrg]?" queries | ~0% | 20-40% | definition-first + statistics |
| Lead-form conversion rate from organic SERP | unknown | 1.5-2.5x | CTA repositioning + matched titles |
| Long-tail keyword rankings (top 30 mid-tier) | unknown | top 3 for "[Name] toll-free" | minimal competition + structured pages |

The 11.2M-number aggregate inventory across the 46 mid-tier resporgs is what makes this an asymmetric play. Each individual page might only generate 5-50 organic visits/month, but at 46 pages that's 230-2,300 monthly organic visits — and many of those visitors are searching number-related queries where the lead conversion is high (people search for a specific number; they want one).
