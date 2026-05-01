# FCC Now Requires All Voice Providers to Block Calls From Invalid Numbers

_As of December 15, 2025, every voice service provider in the call path — not just gateway carriers — must block calls from numbers on a 'do-not-originate' list. Penalties run up to $23,000 per day, and downstream carriers can refuse traffic from non-compliant providers. Here's what the rule says._

---

**Source query (for editor):** fcc-dno-mandate-dec-2025
**Generated:** 2026-04-30T20:01:30.859091Z
**Model:** perplexity/sonar-pro
**⚠ AI-generated draft. Verify all citations and dates against primary sources (FCC dockets, somos.com, etc.) before publishing.**

---

# FCC's December 15, 2025 Do-Not-Originate Call-Blocking Mandate

The **December 15, 2025 DNO mandate requires all voice service providers—originating, intermediate, gateway, and terminating carriers—to block calls purporting to originate from numbers on a reasonable do-not-originate list**, expanding previous requirements that applied only to gateway providers.[1][2] This rule is codified in 47 CFR § 64.1200(p) and was adopted by the FCC in its Eighth Report and Order regarding CG Docket No. 17-59 on February 27, 2025, with an earlier foundational order in WC Docket No. 17-97 adopted March 16, 2023.[1][4]

## Core Provider Obligations

Under 47 CFR § 64.1200(p), effective December 15, 2025, each voice service provider must:[1]

1. **Use at least one reasonable DNO source** to identify numbers that should not originate calls
2. **Block calls from DNO-flagged numbers** throughout the call path
3. **Establish an error reporting mechanism** with a publicly available point of contact for consumers to report erroneously blocked calls
4. **Document compliance** with the rule

## Required Number Categories for Blocking

The FCC mandates that DNO lists include at minimum four categories of numbers:[1]

1. **Invalid numbers** — those that do not conform to valid North American Numbering Plan formats
2. **Unallocated numbers** — numbers not yet assigned by the North American Numbering Plan Administrator
3. **Unused numbers** — numbers allocated but not currently in service
4. **Inbound-only numbers** — numbers explicitly allocated for receiving calls only, such as certain government or service numbers

The FCC cautioned that it may "deem unreasonable a list so limited in scope that it leaves out obvious numbers that could be included with little effort."[3] Providers must also **constantly update DNO lists**, particularly for unused numbers that may transition into active service.[3]

## Relationship to STIR/SHAKEN

The DNO blocking mandate works in conjunction with **STIR/SHAKEN authentication and industry traceback initiatives** as part of the FCC's broader strategy to combat spoofed and fraudulent traffic.[1] While STIR/SHAKEN focuses on authenticating the legitimacy of caller identity claims, DNO blocking provides an additional layer of defense by preventing calls from originating on numbers that are inherently invalid or unallocated—numbers that cannot legitimately be used for outbound calling regardless of authentication status.

## Impact on Toll-Free Numbers

The search results do not provide specific details about toll-free number treatment under the DNO mandate. However, one source mentions that Netnumber's Dynamic DNO service "integrates with the netnumber Services Registry (nnSR), North America's authoritative registry for text-enabling 10-digit and toll-free numbers," suggesting that toll-free numbers are included within the scope of DNO compliance systems.[1] The rule extends DNO blocking requirements to SMS and text messaging under 47 CFR § 64.1200(p), which would encompass toll-free messaging services.[1]

## Enforcement and Compliance Scope

**Non-compliance carries severe penalties.** The FCC can impose forfeitures of up to $23,000 per violation under 47 U.S.C. § 503(b), with each day of continuing violation constituting a separate offense.[1] More significantly, the FCC has warned that failure to comply "may result in the blocking of all traffic from a non-compliant provider," meaning compliant downstream providers can refuse to carry traffic from non-compliant carriers, effectively cutting off service.[1]

The mandate applies to **all providers in the call path**, not just large carriers.[4][5] The FCC acknowledged that smaller voice service providers may utilize less extensive DNO lists than larger carriers, particularly when other providers in the call chain provide blocking functionality.[3]

## Key Distinction from Previous Rules

The December 15, 2025 rule represents a major expansion from the June 14, 2022 gateway-only requirements.[1] The new mandate extends blocking obligations to **all intermediate and terminating providers** (not just gateways), applies to **domestic calls** (not just international-to-domestic), and creates **multiple checkpoints throughout the network** rather than filtering only at international gateways.[1]
