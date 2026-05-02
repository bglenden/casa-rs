# Current CASA Guide Tutorial And Dataset Inventory

Truth class: current descriptive
Last reality check: 2026-05-02
Verification: just docs-check

Wave issue: #137
Child issue: #115

This inventory records the current CASA Guide tutorial surface that drives the
first tutorial-parity waves. It does not download datasets; the first registry
entries are implemented in `casa-test-support` for resolver/preflight use.

Registry keys are stable logical keys for the #97 resolver and manifest work.
Local path policies are expressed relative to
`CASA_RS_TUTORIAL_DATA_ROOT`; actual local paths should be populated by the dataset
registry when artifacts are mirrored into shared tutorial storage.

## Inventory Policy

- Current ALMA pages on the ALMA tutorials page are primary.
- Current VLA CASA 6.7.2 pages on the VLA tutorials page are primary.
- The Simulation index's current VLA protoplanetary disk guide is primary.
- Older Simulation pages such as CASA 6.6.6 `simalma` and ACA guides are
  breadth/deferred unless a later wave promotes them.
- Pipeline reprocessing and external tools are inventoried but not treated as
  engine-implementation targets.

## ALMA Current Pages

| Registry area | Guide | CASA version | URL | Extracted CASA surface | Input artifacts / data source | Classification |
|---|---|---:|---|---|---|---|
| `alma/getting-started` | Getting Started in CASA | 6.6.6 | <https://casaguides.nrao.edu/index.php/Getting_Started_in_CASA> | `rmtables` | none identified | orientation |
| `alma/first-look/index` | ALMA First Look | current index | <https://casaguides.nrao.edu/index.php/ALMA_First_Look> | index page | none identified | index |
| `alma/first-look/twhya/imaging` | First Look at Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/First_Look_at_Imaging> | `listobs`, `plotms`, `tclean`, `split` | `twhya_calibrated.ms.tar`, `twhya_uncalibrated.ms.tar`, `twhya_calibrated_unflagged.ms.tar` under `FirstLook_TWHya_Band7_*` | Wave 3 spine |
| `alma/first-look/twhya/selfcal` | First Look at Self Calibration | 6.6.6 | <https://casaguides.nrao.edu/index.php/First_Look_at_Self_Calibration> | `listobs`, `plotms`, `tclean`, `gaincal`, `applycal`, `split` | uses TW Hydra first-look MS products | Wave 3 spine |
| `alma/first-look/twhya/line-imaging` | First Look at Line Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/First_Look_at_Line_Imaging> | `plotms`, `uvcontsub`, `tclean` | `twhya_selfcal.ms.tgz` | Wave 3 spine |
| `alma/first-look/twhya/image-analysis` | First Look at Image Analysis | 6.6.6 | <https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis> | `imhead`, `imstat`, `immoments`, `exportfits` | `twhya_cont.image`, `twhya_n2hp.image` | Wave 3 spine |
| `alma/antennae/band7` | Antennae Band 7 Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging> | `plotms`, `tclean`, `uvcontsub`, `imstat`, `immoments`, `exportfits`, `gaincal`, `applycal`, `rmtables` | `Antennae_Band7_CalibratedData.tgz` and `Antennae_Band7_ReferenceImages.tgz` under `Antennae_Band7_6.6.6` | #161 |
| `alma/iras16293/band9` | IRAS16293 Band 9 Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/IRAS16293_Band9_-_Imaging> | `listobs`, `plotms`, `tclean`, `split`, `uvcontsub`, `immoments`, `exportfits`, `immath`, `gaincal`, `applycal`, `flagdata`, `flagmanager` | `https://bulk.cv.nrao.edu/almadata/public/casaguides/IRAS16293_Band9_6.6.6` | #162 |
| `alma/m100/band3-combine` | M100 Band 3 Combine | mixed page history, current ALMA topical | <https://casaguides.nrao.edu/index.php/M100_Band3_Combine> | `listobs`, `tclean`, `split`, `imhead`, `imstat`, `immoments`, `exportfits`, `imregrid`, `immath`, `imsubimage`, image tool `ia.open` | ALMA science-verification data source | #163 |
| `alma/3c286/band6-pol` | 3C286 Band 6 Polarization Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/3C286_Band6Pol_Imaging> | `plotms`, `tclean`, `imhead`, `imstat`, `exportfits`, `immath`, `imfit`, `gaincal`, `applycal` | ALMA polarization products / pipeline docs | #164 |
| `alma/sunspot/band6-feathering` | Sunspot Band 6 Feathering | 6.6.6 | <https://casaguides.nrao.edu/index.php/Sunspot_Band6_Feathering> | `imhead`, `exportfits`, `imregrid`, `immath`, `imsubimage` | guide page does not expose direct archive in API scan | #165 |
| `alma/renormalization` | ALMA Renormalization Correction | 6.6.x | <https://casaguides.nrao.edu/index.php/ALMA_Renormalization_Correction> | `applycal` plus calibration-product handling | ALMA help/science-pipeline references | #166 |
| `alma/automasking` | Automasking Guide | 6.6.6 | <https://casaguides.nrao.edu/index.php/Automasking_Guide> | `listobs`, `tclean` with automasking controls | `twhya_selfcal.ms.contsub.tgz` | #167 |
| `alma/data-weights` | Data Weights And Combination | older page, still topical | <https://casaguides.nrao.edu/index.php/DataWeightsAndCombination> | `listobs`, `plotms`, `applycal`, `concat`, `statwt` | ALMA help/science archive references | #168 |
| `alma/na-imaging-template` | Guide to the NA Imaging Template | current topical | <https://casaguides.nrao.edu/index.php/Guide_NA_ImagingTemplate> | pipeline/script template mapping | ALMA QA2/product references | mapping only |
| `alma/pipeline-reprocessing` | ALMA Imaging Pipeline Reprocessing | current topical | <https://casaguides.nrao.edu/index.php/ALMA_Imaging_Pipeline_Reprocessing> | pipeline execution | pipeline products | external pipeline workflow |
| `alma/pipeline-known-issues` | ALMA Pipeline Known Issues | current topical | <https://casaguides.nrao.edu/index.php/ALMA_Pipeline_Known_Issues> | issue reference | ALMA help pages | reference only |

## VLA Current Pages

| Registry area | Guide | CASA version | URL | Extracted CASA surface | Input artifacts / data source | Classification |
|---|---|---:|---|---|---|---|
| `vla/irc10216` | High frequency spectral line data reduction: IRC+10216 | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216> | `listobs`, `plotms`, `gencal`, `applycal`, `split`, `flagdata`, `setjy`, `gaincal`, `bandpass`, `fluxscale`, `mstransform`, `uvcontsub`, `tclean`, `imstat`, `immoments`, `impv`, `statwt`, `plotcal` | `TDRW0001_10s.ms.tgz`, `irc_fors1_dec_header.fits` | Wave 4 spine |
| `vla/3c391` | Continuum Imaging, Mosaicking: 3C391 | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Continuum_Tutorial_3C391-CASA6.7.2> | `listobs`, `plotms`, `tclean`, `split`, `imstat`, `gaincal`, `applycal`, `bandpass`, `fluxscale`, `setjy`, `gencal`, `flagdata`, `delmod`, `statwt`, `plotcal` | `3c391_ctm_mosaic_10s_spw0.ms.tgz`, `AdvancedEVLAcont.tgz`, script archive | #169 |
| `vla/3c75-pol` | Polarization Calibration: 3C75 | 6.7.2 | <https://casaguides.nrao.edu/index.php/Polarization_Calibration_based_on_CASA_pipeline_standard_reduction:_The_radio_galaxy_3C75> | `plotms`, `tclean`, `split`, `imstat`, `immath`, `imsubimage`, `gaincal`, `applycal`, `bandpass`, `setjy`, `flagdata`, `flagmanager`, `delmod`, `statwt` | `CASA6.7.2_Polarization_Guide_Files.tgz`, calibrated MS, pipeline products | #170 |
| `vla/3c129-pband` | Radio galaxy 3C129 P-band continuum | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Radio_galaxy_3C_129:_P-band_continuum_tutorial> | `listobs`, `plotms`, `tclean`, `split`, `gaincal`, `applycal`, `bandpass`, `setjy`, `gencal`, `flagdata`, `hanningsmooth`, `clearcal`, `statwt` | `P_band_3C129.tgz`, ionosphere file, older tar bundle | #171 |
| `vla/mg0414-pband-line` | MG0414+0534 P-band Spectral Line | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=MG0414%2B0534_P-band_Spectral_Line_Tutorial> | `listobs`, `plotms`, `tclean`, `split`, `mstransform`, `gaincal`, `applycal`, `bandpass`, `setjy`, `gencal`, `flagdata`, `hanningsmooth` | `MG0414_d1_data.ms.tgz`, ionosphere file, tutorial script | #172 |
| `vla/hi21-leda44055` | HI 21 cm spectral line: LEDA 44055 | 6.7.2 | <https://casaguides.nrao.edu/index.php/HI_21cm_(1.4_GHz)_spectral_line_data_reduction:_LEDA_44055-CASA6.7.2> | `listobs`, `plotms`, `tclean`, `split`, `imcontsub`, `gaincal`, `applycal`, `bandpass`, `fluxscale`, `setjy`, `gencal`, `flagdata` | NRAO archive / baseline references | #173 |
| `vla/flagging` | VLA CASA Flagging | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Flagging> | `listobs`, `plotms`, `split`, `gaincal`, `applycal`, `bandpass`, `flagdata`, `flagmanager`, `hanningsmooth`, `plotcal` | `SNR_G55_10s.tar.gz` | #174 |
| `vla/imaging` | VLA CASA Imaging | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Imaging> | `tclean`, legacy `clean`, `imhead`, `immath`, image tool `ia.open`, `rmtables` | `SNR_G55_10s.calib.tar.gz` | #175 |
| `vla/selfcal` | VLA Self-calibration Tutorial | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Self-calibration_Tutorial> | `listobs`, `plotms`, `tclean`, `split`, `gaincal`, `applycal`, `plotcal` | `17B-197...tar`, `VLASelf-calibrationTutorial.tar` | #176 |
| `vla/data-combination` | VLA Data Combination - W49A | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Data_Combination> | `listobs`, `plotms`, `tclean`, `concat`, `statwt` | `VLA-combination-W49A.tar.gz` | #177 |
| `vla/source-subtraction` | Source Subtraction in VLA data | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Source_Subtraction_Topical_Guide> | `listobs`, `plotms`, `tclean`, `imhead`, `imfit`, `ft`, component-list `cl.addcomponent` | `VLA-combination-SgrA-files.tar.gz` | #178 |
| `vla/bandpass-slope` | Correcting for a Spectral Index in Bandpass Calibration | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Bandpass_Slope> | `listobs`, `plotms`, `gaincal`, `bandpass`, `fluxscale`, `setjy` | `G192-BP.ms.tar.gz` | #179 |

## Simulation Pages

| Registry area | Guide | CASA version | URL | Extracted CASA surface | Input artifacts / data source | Classification |
|---|---|---:|---|---|---|---|
| `simulation/vla-ppdisk` | Protoplanetary Disk Simulation - VLA | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=Protoplanetary_Disk_Simulation_-_VLA-CASA6.7.2> | `importfits`, `immath`, `simobserve`, `simanalyze`, `plotms`, `tclean`, `imhead`, `imstat` | `ppdisk672_GHz_50pc.fits` | Wave 5 spine |
| `simulation/ppdisk-alma-former` | Protoplanetary Disk Simulation | 6.6.6 | <https://casaguides.nrao.edu/index.php/Protoplanetary_Disk_Simulation_CASA_6.6.6> | `simobserve`, `simanalyze`, `imhead`, image tool `ia.open` | `ppdisk672_GHz_50pc.fits` under ALMA sim inputs | #180 |
| `simulation/simalma` | Simalma | 6.6.6 | <https://casaguides.nrao.edu/index.php/Simalma> | `simalma` task surface | `https://bulk.cv.nrao.edu/almadata/public/casaguides/SimALMA` | #181 |
| `simulation/aca` | ACA Simulation | 6.5.4 | <https://casaguides.nrao.edu/index.php/ACA_Simulation> | `simobserve`, `simanalyze` | no direct data artifact found in API scan | #182 |
| `simulation/antenna-configs` | Antenna Configurations Models in CASA | 6.6.x | <https://casaguides.nrao.edu/index.php/Antenna_Configurations_Models_in_CASA> | antenna-configuration reference | ALMA simulator/configuration references | support reference |
| `simulation/corruptions` | Corrupting Simulated Data (Simulator Tool) | archived/tool reference | <https://casaguides.nrao.edu/index.php/Corrupting_Simulated_Data_(Simulator_Tool)> | simulator tool `sm.open`, `sm.setnoise`, `sm.setgain`, `sm.corrupt` | none identified | Wave 5/6 corruption reference |

## Expected Output Products

These product families are the inventory target for tutorial parity. Later
implementation waves should turn each row into exact product manifests and
CASA/casacore comparison artifacts for the specific dataset version used.

### ALMA

| Registry area | Expected output products |
|---|---|
| `alma/getting-started` | cleanup behavior only; no durable science product |
| `alma/first-look/index` | index only; no durable science product |
| `alma/first-look/twhya/imaging` | continuum `.image`, `.model`, `.residual`, `.pb`, `.psf`, `.sumwt`, selected split MS products |
| `alma/first-look/twhya/selfcal` | gain calibration tables, corrected/selfcal split MS products, selfcal continuum image products |
| `alma/first-look/twhya/line-imaging` | continuum-subtracted MS, spectral cube `.image` products, masks/residual/model sidecars |
| `alma/first-look/twhya/image-analysis` | image header/stat records, moment maps, FITS exports |
| `alma/antennae/band7` | continuum/line image products, continuum-subtracted MS products, moments, FITS exports, selfcal caltables |
| `alma/iras16293/band9` | split/flagged MS products, calibration tables, continuum and line cubes, moment maps, FITS exports |
| `alma/m100/band3-combine` | combined image cubes, regridded images, subimages, moment maps, FITS exports |
| `alma/3c286/band6-pol` | polarization image products, statistics, FITS exports, fit records, calibration tables |
| `alma/sunspot/band6-feathering` | regridded/subimage products and FITS exports |
| `alma/renormalization` | corrected calibration/MS products from renormalized apply workflow |
| `alma/automasking` | automasked continuum images and mask products |
| `alma/data-weights` | concatenated/reweighted MS products and comparison plots |
| `alma/na-imaging-template` | mapping-only scripts/templates; no direct parity product |
| `alma/pipeline-reprocessing` | external pipeline products; interop target only |
| `alma/pipeline-known-issues` | reference-only; no parity product |

### VLA

| Registry area | Expected output products |
|---|---|
| `vla/irc10216` | prior-cal/gain/bandpass/fluxscale tables, corrected and transformed MS products, continuum-subtracted MS, spectral cube images, moment maps, PV image, statistics |
| `vla/3c391` | calibrated/split MS products, continuum mosaic images, statistics, selfcal/calibration tables |
| `vla/3c75-pol` | calibrated polarization MS products, Stokes image products, subimages/math outputs, statistics |
| `vla/3c129-pband` | smoothed/flagged/calibrated P-band MS products, calibration tables, continuum images |
| `vla/mg0414-pband-line` | smoothed/transformed/calibrated MS products, line cube images, calibration tables |
| `vla/hi21-leda44055` | calibrated MS products, image-domain continuum subtraction products, HI cube and moment products |
| `vla/flagging` | flag-version products, flagged MS state, diagnostic plot/calibration products |
| `vla/imaging` | legacy `clean` comparison products where referenced, `tclean` image sidecars, header/math products |
| `vla/selfcal` | selfcal gain tables, corrected split MS products, successive image products |
| `vla/data-combination` | concatenated/reweighted MS products and combined image products |
| `vla/source-subtraction` | component-list/model products, predicted/model MS state, subtracted image products, fit records |
| `vla/bandpass-slope` | bandpass/gain/fluxscale calibration tables and diagnostic plot products |

### Simulation

| Registry area | Expected output products |
|---|---|
| `simulation/vla-ppdisk` | imported/scaled model image, synthetic MS, simulation report products, dirty/clean image products, image statistics |
| `simulation/ppdisk-alma-former` | synthetic ALMA MS and image-analysis products |
| `simulation/simalma` | ALMA/ACA synthetic MS products and combined images |
| `simulation/aca` | ACA synthetic MS products and analyzed images |
| `simulation/antenna-configs` | support reference only; no direct parity product |
| `simulation/corruptions` | corrupted synthetic MS variants and seeded comparison products |

## First Dataset Registry Candidates

Size values come from HTTP `Content-Length` when the source server advertises
one. Checksum is `not advertised` unless the source page/server exposes one;
the registry should compute and store SHA-256 at mirror time without making this
inventory issue download every artifact.

| Proposed key | Source page | Source artifact URL | Expected filename | Advertised size | Checksum status | Tier | Local path policy | First owning wave |
|---|---|---|---|---:|---|---|---|---|
| `alma/first-look/twhya/calibrated-ms` | <https://casaguides.nrao.edu/index.php/First_Look_at_Imaging> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar> | `twhya_calibrated.ms.tar` | 435742720 bytes | SHA-256 `f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_calibrated.ms.tar` | #140 |
| `alma/first-look/twhya/uncalibrated-ms` | <https://casaguides.nrao.edu/index.php/First_Look_at_Imaging> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_uncalibrated.ms.tar> | `twhya_uncalibrated.ms.tar` | 765388800 bytes | SHA-256 `4eb09a74e9be71fea9761a54884869dce361fee83c0b9d636ffa4b2bdc882835` | slow-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_uncalibrated.ms.tar` | #140 |
| `alma/first-look/twhya/calibrated-unflagged-ms` | <https://casaguides.nrao.edu/index.php/First_Look_at_Imaging> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_calibrated_unflagged.ms.tar> | `twhya_calibrated_unflagged.ms.tar` | 623800320 bytes | SHA-256 `3d2c460c126957d02025ec842c4279718a7a58b2147980d84ce0523e4cf1309d` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_calibrated_unflagged.ms.tar` | #140 |
| `alma/first-look/twhya/selfcal-ms` | <https://casaguides.nrao.edu/index.php/First_Look_at_Line_Imaging> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_selfcal.ms.tgz> | `twhya_selfcal.ms.tgz` | 392786323 bytes | SHA-256 `6d720b89a7b433fbc9b0cc04cde973c03bde1b63945a3f40f6e59816ae6769fc` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_selfcal.ms.tgz` | #140 |
| `alma/first-look/twhya/continuum-image` | <https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_cont.image> | `twhya_cont.image` | 369373 bytes | directory SHA-256 manifest in local mirror | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_cont.image` | #140 |
| `alma/first-look/twhya/n2hp-image` | <https://casaguides.nrao.edu/index.php/First_Look_at_Image_Analysis> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_n2hp.image> | `twhya_n2hp.image` | 3859246 bytes | directory SHA-256 manifest in local mirror | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_n2hp.image` | #140 |
| `alma/antennae/band7/calibrated-data` | <https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging_6.6.6> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_CalibratedData.tgz> | `Antennae_Band7_CalibratedData.tgz` | 912711095 bytes | SHA-256 `1976fea9239dea06c144c963c3750b03e7c53e82787f0a3c46b72fe17b5df339` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/antennae/band7/Antennae_Band7_CalibratedData.tgz` | #161 |
| `alma/antennae/band7/reference-images` | <https://casaguides.nrao.edu/index.php/AntennaeBand7> | <https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_ReferenceImages.tgz> | `Antennae_Band7_ReferenceImages.tgz` | 83981505 bytes | SHA-256 `cd52ffdc8f7b18f28ede2be70f6334f2f3f435fe31d7cff66f6e3a446eed2190` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/antennae/band7/Antennae_Band7_ReferenceImages.tgz` | #161 |
| `vla/irc10216/ms-10s` | <https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216> | <http://casa.nrao.edu/Data/EVLA/IRC10216/TDRW0001_10s.ms.tgz> | `TDRW0001_10s.ms.tgz` | 1068298240 bytes | SHA-256 `96292e62103b51a456e9a6620ffab54ca00785448935122eaf714aa5b21308cb` | slow-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/vla/irc10216/TDRW0001_10s.ms.tgz` | #141 |
| `vla/irc10216/fors1-fits` | <https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216> | <http://casa.nrao.edu/Data/EVLA/IRC10216/irc_fors1_dec_header.fits> | `irc_fors1_dec_header.fits` | 16784640 bytes | SHA-256 `9e476e1f98f63d9d870dfa1d72f6705ca40aed3c006115742a0bb2922cbd8071` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/vla/irc10216/irc_fors1_dec_header.fits` | #141 |
| `simulation/vla-ppdisk/model-fits` | <https://casaguides.nrao.edu/index.php?title=Protoplanetary_Disk_Simulation_-_VLA-CASA6.7.2> | <https://casa.nrao.edu/Data/EVLA/simulation/ppdisk672_GHz_50pc.fits> | `ppdisk672_GHz_50pc.fits` | 276480 bytes | SHA-256 `e4416bfa0732251d5a7fef48e6c6f9cf8426de264626b63e7ad42fa76faef70e` | tutorial-parity | `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits` | #142 |

## Registry Fields For #97

Each future registry entry should include:

- logical key;
- source page URL and source artifact URL;
- expected filename and unpacked root name;
- declared CASA Guide version;
- checksum and size once mirrored;
- dataset tier (`default-fixture`, `tutorial-parity`, `slow-parity`,
  `performance`);
- local path under `CASA_RS_TUTORIAL_DATA_ROOT`;
- optional local staging root for removable/NAS-backed mirrors;
- first owning wave and downstream dependent waves.
