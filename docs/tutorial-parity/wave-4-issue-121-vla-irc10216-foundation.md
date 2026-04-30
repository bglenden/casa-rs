# Wave 4 Issue 121 - VLA IRC+10216 Foundation

Truth class: current descriptive
Last reality check: 2026-04-29
Verification: `cargo test -p casa-ms --test msexplore_cli cli_flag_selected_applies_to_selected_rows_without_plot_region`; `scripts/run-vla-irc10216-issue121.sh`

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
| Prior `gencal` tables: `cal.ant`, `cal.gc`, `cal.tau` | future `casa-calibration` work | Not implemented as native table generation in this slice; the exact tutorial impact is confined to the prior-cal corrected MS handoff before #122. |
| `applycal(... ["cal.ant","cal.gc","cal.tau"])` and `split(datacolumn="corrected")` | `casa-calibration` / `casa-ms` | Existing apply/split coverage does not yet accept these generated prior tables natively; #122 owns the deeper solve/apply parity after this handoff. |

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

Expected visible artifacts:

- `target/wdad-wave4-121/irc10216-target-scan6-amplitude-time-side-by-side.png`
- `target/wdad-wave4-121/irc10216-bandpass-scan56-amplitude-channel-side-by-side.png`
- `target/wdad-wave4-121/flag-j1229-selection-summary.json`
- `target/wdad-wave4-121/flag-3c286-selection-summary.json`

## Unsupported #121 Surface

The unsupported prior-cal surface is deliberately narrow and explicit:

- native `gencal(caltype="antpos")` for `cal.ant`;
- native `gencal(caltype="gceff")` for `cal.gc`;
- native `gencal(caltype="opac")` for `cal.tau`;
- native apply of that prior-cal table chain into a corrected-data split.

Those items require calibration-table generation semantics rather than a new
top-level app family. They should be implemented in `casa-calibration` before
declaring the full prior-cal handoff native.
