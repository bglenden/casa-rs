# Wave 4 Issue 121 - VLA IRC+10216 Foundation

Truth class: current descriptive
Last reality check: 2026-04-30
Verification: `cargo test -p casa-ms --test msexplore_cli cli_flag_selected_applies_to_selected_rows_without_plot_region`; `cargo check -p casa-calibration`; `scripts/run-vla-irc10216-issue121.sh`

Wave issue: #141
Child issue: #121

This note records the CASA-to-casa-rs mapping for the first VLA IRC+10216
tutorial segment. It stays inside the existing `casa-ms` / `msexplore` and
`casa-calibration` surfaces; it does not add a new flagging or calibration app
family.

## Dataset

The tutorial inputs are registry artifacts:

- key: `vla/irc10216/ms-10s`
- source artifact: `TDRW0001_10s.ms.tgz`
- expected SHA-256:
  `96292e62103b51a456e9a6620ffab54ca00785448935122eaf714aa5b21308cb`
- local policy:
  `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/vla/irc10216/TDRW0001_10s.ms.tgz`
- key: `vla/irc10216/fors1-fits`
- source artifact: `irc_fors1_dec_header.fits`
- expected SHA-256:
  `9e476e1f98f63d9d870dfa1d72f6705ca40aed3c006115742a0bb2922cbd8071`
- local policy:
  `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/vla/irc10216/irc_fors1_dec_header.fits`

## Tutorial Mapping

| CASA tutorial operation | casa-rs owner | Wave 4 #121 mapping |
|---|---|---|
| `listobs(vis="TDRW0001_10s.ms")` | `casa-ms` / `msexplore` | `msexplore --format json ...` emits structured summary JSON for the real tutorial MS. |
| Initial `plotms` inspection | `casa-ms` / `msexplore` | Side-by-side CASA/casa-rs PNGs cover IRC+10216 scan-6 amplitude vs time and J1229+0203 scan-56 amplitude vs 4-channel bin. |
| `flagdata(mode="list")` for two low-amplitude ranges | `casa-ms` / `msexplore` | `msexplore --flag-action flag --flag-selected --flag-apply` applies row-selection flag edits and writes selected-summary JSON evidence. |
| Prior `gencal` tables: `cal.ant`, `cal.gc`, `cal.tau` | `casa-calibration` | `calibrate gencal` writes CASA-compatible `KAntPos Jones`, `EGainCurve`, and `TOpac` float caltables for the tutorial prior-cal subset. |
| `applycal(... ["cal.ant","cal.gc","cal.tau"])` and `split(datacolumn="corrected")` | `casa-calibration` / `casa-ms` | The prior-cal tables are generated natively in #121; #122 owns applying the full prior-cal chain while solving and producing the corrected calibration handoff. |

The real tutorial MS resolves to four fields: `0=J0954+1743`,
`1=IRC+10216`, `2=J1229+0203`, and `3=1331+305=3C286`, with two spectral
windows.

## Executable Evidence

The wave script stages the registry tarball under `target/wdad-wave4-121`
by default, renders visible CASA-vs-casa-rs plot artifacts, and applies the two
tutorial flagging ranges. Pass an output directory as the first argument to keep
a fresh evidence run separate from earlier mutated MS copies.

```bash
scripts/run-vla-irc10216-issue121.sh
```

Expected visible artifacts and machine-readable evidence:

- `target/wdad-wave4-121/irc10216-target-scan6-amplitude-time-side-by-side.png`
- `target/wdad-wave4-121/irc10216-bandpass-scan56-amplitude-channel-side-by-side.png`
- `target/wdad-wave4-121/irc10216-priorcal-fparam-casa-vs-rust.png`
- `target/wdad-wave4-121/priorcal-comparison.json`
- `target/wdad-wave4-121/priorcal-casa-cli-timing.json`
- `target/wdad-wave4-121/priorcal-rust-cli-timing.json`
- `target/wdad-wave4-121/priorcal-casa-timing.json`
- `target/wdad-wave4-121/priorcal-rust-timing.json`
- `target/wdad-wave4-121/flag-j1229-selection-summary.json`
- `target/wdad-wave4-121/flag-3c286-selection-summary.json`

## Implemented Prior-Cal Surface

The native `gencal` surface is deliberately narrow and tutorial-bound:

- `gencal(caltype="antpos")` writes `KAntPos Jones` `FPARAM` triples for explicit antenna offsets;
- `gencal(caltype="gceff")` reads the CASA VLA `GainCurves` table and writes `EGainCurve` coefficients scaled by the VLA efficiency curve;
- `gencal(caltype="opac")` writes `TOpac` opacity values for selected SPWs.

Automatic VLA antenna-position lookup is not implemented in #121; the script
passes the tutorial offsets explicitly. Native application of `KAntPos Jones`,
`EGainCurve`, and `TOpac` into corrected data remains part of the deeper #122
solve/apply parity work.
