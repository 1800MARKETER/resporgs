# Cross-server number-data architecture

The number-data layer of the network has a clear canonical-home pattern: the
**Chicago Resporg box (`149.28.119.6`)** is the source of truth for every
piece of toll-free + local number reference data. NYC3 (`104.131.76.98`)
hosts the consumer sites, each reading a local mirror that's kept in sync.

## The canonical home (Chicago)

```
/var/www/resporgs/
├── cache/<MM>.parquet                    Resporgs.com raw monthly snapshots (44 files,
│                                         2022-06 → 2026-05). Built upstream by
│                                         scripts/cache_months.py from Somos download.
├── cache/adjusted/<MM>.parquet           somos_adjust output: raw parquet + 4 control-file
│                                         passes (RestrictedACExc, RO2RO, RO2Stat, Individual).
│                                         Built by scripts.somos_adjust.build_adjusted (~35 min).
├── cache/intel/tollfree_intel-<MM>.db    SQLite for VanityNumbers.com lookup. Built by
│                                         scripts.somos_adjust.build_tollfree_intel (~3 min).
├── cache/legacy/<MM>-TFOINPUT.txt        Bud-byte-compat outputs from
│   <MM>-MikeOINPUT.txt                   scripts.somos_adjust.output_legacy (~8 min).
└── scripts/somos_adjust/                 The pipeline + per-step modules.

/var/www/local-prospector/data/
├── master_vanity.db                      Vanity scoring DB (~377 MB, 2M rows).
│                                         Built on Bill's local machine via
│                                         local-prospector/scripts/build_master_excel.py
│                                         (and the apply_excel_verdicts/recategorize family).
│                                         Pushed to Chicago via sync_master_data.py.
├── Master_Million_Vanity.xlsx            Source workbooks for master_vanity.db.
└── Master_Million_Vanity_7digit.xlsx     Same — 7-digit subset.

/var/www/PhonePlatinumWire/db/
└── phoneplatinumwire.db                  LERG-style local NPA/NXX data (~483 MB):
                                          nxx_blocks, ocn, wire_centers, lata, county,
                                          cbsa, msa, mta, bta, cofeatures, websites.
                                          Mirrored from NYC3 on 2026-05-09 — build pipeline
                                          for this DB is currently external (no script
                                          on Bill's machines or droplets yet). When the
                                          planned LERG/NANPA subscription goes live, the
                                          build pipeline should run here and sync to NYC3.
```

## NYC3 mirrors (consumers)

```
/var/www/local-prospector/data/master_vanity.db          ← synced from Chicago
/var/www/PhonePlatinumWire/db/phoneplatinumwire.db       ← synced from Chicago
/var/www/1cup/data/tollfree_intel.db                     ← BUILT on Chicago, deployed to NYC3
/var/www/vanitynumbersforsales/data/inventory_<MM>.db    ← BUILT on Chicago, deployed to NYC3
/var/www/1cup/data/tollfree.db                           ← lives on NYC3 (1Cup vendor inventory,
                                                           updated separately when Ray sends XLSX)
```

## Sync pattern matrix

| Data file | Built where | Mirrored on | How synced |
|---|---|---|---|
| `cache/<MM>.parquet` (raw) | Chicago (cache_months.py) | Chicago only | n/a |
| `cache/adjusted/<MM>.parquet` | Chicago (build_adjusted) | Chicago only | n/a |
| `cache/intel/...db` | Chicago (build_tollfree_intel) | Chicago + NYC3 | `--deploy` flag in build script: gzip-stream + atomic rename + service restart |
| `cache/legacy/<MM>-*.txt` | Chicago (output_legacy) | Uploaded to TFN.com / Mike's site (manual) | Bill uploads after each monthly run |
| `inventory_<MM>.db` (VNFS) | Chicago (output_vnfs) | Chicago + NYC3 | `--deploy` flag in build script |
| `master_vanity.db` | **Bill's local** | Chicago + NYC3 | `local-prospector/scripts/sync_master_data.py` |
| `Master_Million_Vanity*.xlsx` | Bill's local | Chicago + NYC3 | same script as above |
| `phoneplatinumwire.db` | External (no script today) | Chicago + NYC3 | manual rsync; future LERG pipeline will land it on Chicago first |
| `1cup/data/tollfree.db` | NYC3 | NYC3 only | manual updates when Ray sends a new XLSX (apply_ray_diff.py pattern) |

## Why this layout

1. **Race-critical work runs on Chicago** (3.16 ms RTT to Somos vs 20.5 ms from NYC3).
   That's why the future reservation API + catcher daemon live there.
2. **Heavy compute also runs on Chicago** (4 dedicated AMD EPYC vCPUs, 16 GB RAM).
   The 35-min adjusted-parquet build doesn't disturb consumer sites running on NYC3.
3. **Consumer sites stay on NYC3** for now (TFN.com, VNFS, VanityCellular, NLN, FaithNumbers,
   PhonePlatinumWire, 1Cup, ACup, vanitynumbers.com). Their network demands are modest;
   no reason to migrate them until something actually requires it. They read local
   SQLite mirrors so per-request latency is zero.
4. **Cross-server SCP/rsync uses `/root/.ssh/migration_key`** on Chicago — set up during
   the May 8 RESPORGS migration. Removing that key from NYC3's `authorized_keys` will
   break every `--deploy` flag on the build scripts.

## Generic sync command (when build pipeline doesn't auto-deploy)

For files where the build script doesn't have its own `--deploy` (currently
`master_vanity.db` and `phoneplatinumwire.db` after a manual rebuild), this
one-liner from Chicago pushes to NYC3 with no service restart:

```bash
# From Chicago. Adjust the path for whatever file got rebuilt.
ssh root@149.28.119.6 'rsync -az --inplace \
    -e "ssh -i /root/.ssh/migration_key -o StrictHostKeyChecking=no" \
    /var/www/<path>/<file> \
    root@104.131.76.98:/var/www/<path>/<file>'
```

For `master_vanity.db` specifically, the proper pattern is to run
`sync_master_data.py` from Bill's local — not from Chicago — because Bill's
local is the actual source of truth (the build pipeline lives there).

## When this layout will change

- **LERG/NANPA subscription goes live** → phoneplatinumwire.db build pipeline
  moves to Chicago. NYC3 stops being part of any build path.
- **PhonePlatinumWire app moves to Chicago** (eventual) → the
  `tollfree_intel.db` cross-server sync becomes unnecessary; it lives where
  it's queried. Same applies to `phoneplatinumwire.db`. NYC3 retains only
  the consumer sites that don't need the data layer (or moves them too).
- **TollFreeNumbers.com v2** (in planning) → consumes adjusted parquet directly
  on Chicago instead of TFOINPUT.txt upload flow. One less manual upload step
  per month.

Don't optimize for these futures until they happen — current sync overhead
is small (<5 min/month total across all files).
