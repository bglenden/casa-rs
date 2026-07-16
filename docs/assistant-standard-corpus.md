# Standard Radio-Astronomy Assistant Corpus

Truth class: normative runtime and maintenance contract  
Last reality check: 2026-07-15  
Verification: `python3 scripts/check-assistant-corpus-inventory.py`

## Installed baseline

`casars-mac` ships one versioned, read-only baseline resource pack. It is an app
resource, not a project document and not a copy of the maintainer's
RadioAstronomyOracle checkout. Each project has its own disposable SQLite FTS
index, but the baseline source files are installed only once with the app.

Version `2026.07.1` contains:

- all 27 NRAO Synthesis Imaging Workshop 2026 decks (1,404 slides),
- *Interferometry and Synthesis in Radio Astronomy*, Third Edition, by A.
  Richard Thompson, James M. Moran, and George W. Swenson Jr. (910 pages),
  distributed under CC BY-NC 4.0, and
- the small CASA-RS radio-interferometry primer.

The source inventory accounts individually for all 55 Oracle sources and 4,892
pages/slides. The 26 decks from 2024 remain recorded but are excluded as a
superseded epoch. *Synthesis Imaging in Radio Astronomy II* remains excluded
because its publisher offers paid electronic access and no distributable source
was established. Individual 2024 sources can be admitted in a later pack when
they provide unique coverage.

The NRAO material is publicly hosted by NRAO. The CASA-RS project owner, who was
formerly responsible for NRAO data management and software, explicitly
authorized bundling it on 2026-07-15; the durable authorization and selection
are recorded in issue #420.

## Representation and provenance

The app bundles compact, normalized page/slide text rather than duplicating the
source PDFs. `corpus-pack.json` schema v2 binds every included source to:

- pack identity and version,
- original title and page/slide citation kind,
- authoritative origin URL,
- SHA-256 of the source working copy and bundled text,
- license identifier and URL, and
- the recorded redistribution basis.

The installed `NOTICE.md` and manifest provide source-level attribution and
state that the bundled representation is normalized page text, with images
omitted and any visually verified OCR corrections disclosed as modifications.

The runtime rejects a pack entry when its bundled-content digest differs. It
produces one baseline document per source page or slide, preserving the source
label and page/slide number in every retrieval citation. Baseline version
replacement removes stale baseline documents only; project documents and
assistant conversations are independent and survive the upgrade.

The committed inventory is
`resources/assistant-corpus/radio-astronomy-source-inventory-v1.json`. The
committed origin audit records that all 28 selected authoritative origins were
reachable on 2026-07-15. End-user installation and indexing require neither
RadioAstronomyOracle nor network access.

## Retrieval precedence

The compact v1 pack avoids cross-epoch conflicts by installing only the 2026
workshop. Current-practice questions therefore draw from the current workshop;
the book remains the foundation for theory and derivations. The full inventory
retains explicit precedence metadata (`2026`, then `2024`, then books for
current practice; books first for theory) for future selective expansion.

## Visual notation check

The normalized text for *Basic radio interferometry - Geometry*, slide 15, was
compared directly with the rendered source slide. The image shows the `(u,v,w)`
coordinate matrix using `H_0`, `delta_0`, and `(B_x, B_y, B_z)`, followed by
`tau_g = (lambda/c) w = w/nu` and
`nu_F = dw/dt = -(dH/dt) u cos(delta_0) = -omega_E u cos(delta_0)`.

The upstream normalized OCR had rendered the zero subscripts as degree symbols,
`nu` as `upsilon`, and fringe-frequency `nu` as Latin `v`. The export script
contains source-and-slide-specific corrections for those verified errors. It
does not apply broad mathematical substitutions.

## Measured cost

The selected original PDFs total 359,670,733 bytes. The installed compact pack
is 3,087,373 bytes. A clean debug test on the reference Apple Silicon system
loaded and indexed 2,315 page-level documents in 2.79 seconds and produced a
14,782,464-byte SQLite index. These are measurements, not hard limits. The v1
pack is enabled by default because the measured install and first-index costs
are modest; no 128 MB or executable-location policy is inferred from them.

## Maintenance

Maintainers regenerate the inventory and compact pack from an audited Oracle
checkout:

```sh
python3 scripts/export-assistant-corpus-inventory.py \
  --oracle-root /path/to/RadioAstronomyOracle
python3 scripts/check-assistant-corpus-inventory.py
```

`--check` proves that the checkout-derived inventory and all compact page files
match the committed pack. `scripts/audit-assistant-corpus-origins.py` is an
explicit networked maintainer audit; it is not an end-user startup action or a
default offline gate. A new selection or changed source content requires a new
pack version, regenerated digests, a fresh origin audit, representative visual
notation verification, retrieval tests, and measured install/index evidence.

The production subscription-agent acceptance run on 2026-07-15 made two
separate bounded `corpus.search` calls and returned the independently checked
locators `Interferometry and Synthesis in Radio Astronomy.pdf, page 806` and
`Perley-Geometry-SIW2026.pdf, slide 15`. It used the user's ChatGPT subscription
through the normal project MCP boundary and did not use shell or web retrieval.
