# Wave 6 Breadth Expansion Issue Map

Truth class: current descriptive
Last reality check: 2026-05-01
Verification: just docs-check

Wave issue: #143
Child issues: #127, #128, #129

Wave 6 turns the current tutorial inventory into concrete ready implementation
waves. It does not claim tutorial parity by itself. Each issue below must still
run the named CASA Guide flow end-to-end in CASA 6.7.5-9/CASA C++ and in
casa-rs from the same staged inputs before it can move to Review.

## ALMA Assignments

Parent: #127

| Registry area | Guide | Implementation issue | Classification |
|---|---|---:|---|
| `alma/antennae/band7` | Antennae Band 7 Imaging | #161 | Ready breadth wave |
| `alma/iras16293/band9` | IRAS16293 Band 9 Imaging | #162 | Ready breadth wave |
| `alma/m100/band3-combine` | M100 Band 3 Combine | #163 | Ready breadth wave |
| `alma/3c286/band6-pol` | 3C286 Band 6 Polarization Imaging | #164 | Ready breadth wave |
| `alma/sunspot/band6-feathering` | Sunspot Band 6 Feathering | #165 | Ready breadth wave with dataset confirmation required |
| `alma/renormalization` | ALMA Renormalization Correction | #166 | Ready breadth wave; pipeline engines remain out of scope |
| `alma/automasking` | Automasking Guide | #167 | Ready breadth wave |
| `alma/data-weights` | Data Weights And Combination | #168 | Ready breadth wave |
| `alma/na-imaging-template` | Guide to the NA Imaging Template | n/a | Mapping-only reference |
| `alma/pipeline-reprocessing` | ALMA Imaging Pipeline Reprocessing | n/a | External pipeline workflow |
| `alma/pipeline-known-issues` | ALMA Pipeline Known Issues | n/a | Reference-only page |

## VLA Assignments

Parent: #128

| Registry area | Guide | Implementation issue | Classification |
|---|---|---:|---|
| `vla/3c391` | Continuum Imaging, Mosaicking: 3C391 | #169 | Ready breadth wave |
| `vla/3c75-pol` | Polarization Calibration: 3C75 | #170 | Ready breadth wave; pipeline products are inputs only |
| `vla/3c129-pband` | Radio galaxy 3C129 P-band continuum | #171 | Ready breadth wave |
| `vla/mg0414-pband-line` | MG0414+0534 P-band Spectral Line | #172 | Ready breadth wave |
| `vla/hi21-leda44055` | HI 21 cm spectral line: LEDA 44055 | #173 | Ready breadth wave |
| `vla/flagging` | VLA CASA Flagging | #174 | Ready breadth wave |
| `vla/imaging` | VLA CASA Imaging | #175 | Ready breadth wave |
| `vla/selfcal` | VLA Self-calibration Tutorial | #176 | Ready breadth wave |
| `vla/data-combination` | VLA Data Combination - W49A | #177 | Ready breadth wave |
| `vla/source-subtraction` | Source Subtraction in VLA data | #178 | Ready breadth wave |
| `vla/bandpass-slope` | Correcting for a Spectral Index in Bandpass Calibration | #179 | Ready breadth wave |

## Simulation Assignments

Parent: #129

| Registry area | Guide | Implementation issue | Classification |
|---|---|---:|---|
| `simulation/ppdisk-alma-former` | Protoplanetary Disk Simulation | #180 | Ready breadth wave |
| `simulation/simalma` | Simalma | #181 | Ready breadth wave |
| `simulation/aca` | ACA Simulation | #182 | Ready breadth wave with dataset confirmation required |
| `simulation/antenna-configs` | Antenna Configurations Models in CASA | n/a | Support reference |
| `simulation/corruptions` | Corrupting Simulated Data (Simulator Tool) | #157, #158, #159 | Wave 5 follow-up issues cover remaining corruption families |

## Closeout Notes

- Child issue order for the umbrella remains #127 -> #128 -> #129.
- Existing spine guides remain owned by Waves 3, 4, and 5.
- Large artifacts stay under the tutorial-data policy and out of default gates.
- Pipeline, viewer, and reference-only pages are classified without becoming
  implementation blockers.
- New public APIs, persisted formats, provider contracts, app families,
  substantial dependencies, or major algorithms still require the AGENTS.md
  stop-and-ask process in the implementation issue that needs them.
