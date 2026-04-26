# Current CASA Guide Tutorial And Dataset Inventory

Truth class: current descriptive
Last reality check: 2026-04-26
Verification: just docs-check

Wave issue: #137
Child issue: #115

This inventory records the current CASA Guide tutorial surface that drives the
first tutorial-parity waves. It does not download datasets or implement the
dataset registry.

Registry keys are proposed stable logical keys for the later #97 resolver and
manifest work. Actual checksums and mirrored local paths should be populated
when datasets are downloaded into the shared tutorial dataset mirror.

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
| `alma/antennae/band7` | Antennae Band 7 Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging> | `plotms`, `tclean`, `uvcontsub`, `imstat`, `immoments`, `exportfits`, `gaincal`, `applycal`, `rmtables` | ALMA science-verification package; page links support docs more than direct archives | Wave 6 breadth |
| `alma/iras16293/band9` | IRAS16293 Band 9 Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/IRAS16293_Band9_-_Imaging> | `listobs`, `plotms`, `tclean`, `split`, `uvcontsub`, `immoments`, `exportfits`, `immath`, `gaincal`, `applycal`, `flagdata`, `flagmanager` | `https://bulk.cv.nrao.edu/almadata/public/casaguides/IRAS16293_Band9_6.6.6` | Wave 6 breadth |
| `alma/m100/band3-combine` | M100 Band 3 Combine | mixed page history, current ALMA topical | <https://casaguides.nrao.edu/index.php/M100_Band3_Combine> | `listobs`, `tclean`, `split`, `imhead`, `imstat`, `immoments`, `exportfits`, `imregrid`, `immath`, `imsubimage`, image tool `ia.open` | ALMA science-verification data source | Wave 6 breadth |
| `alma/3c286/band6-pol` | 3C286 Band 6 Polarization Imaging | 6.6.6 | <https://casaguides.nrao.edu/index.php/3C286_Band6Pol_Imaging> | `plotms`, `tclean`, `imhead`, `imstat`, `exportfits`, `immath`, `imfit`, `gaincal`, `applycal` | ALMA polarization products / pipeline docs | Wave 6 breadth |
| `alma/sunspot/band6-feathering` | Sunspot Band 6 Feathering | 6.6.6 | <https://casaguides.nrao.edu/index.php/Sunspot_Band6_Feathering> | `imhead`, `exportfits`, `imregrid`, `immath`, `imsubimage` | guide page does not expose direct archive in API scan | Wave 6 breadth |
| `alma/renormalization` | ALMA Renormalization Correction | 6.6.x | <https://casaguides.nrao.edu/index.php/ALMA_Renormalization_Correction> | `applycal` plus calibration-product handling | ALMA help/science-pipeline references | Wave 6 breadth |
| `alma/automasking` | Automasking Guide | 6.6.6 | <https://casaguides.nrao.edu/index.php/Automasking_Guide> | `listobs`, `tclean` with automasking controls | `twhya_selfcal.ms.contsub.tgz` | Wave 6 breadth |
| `alma/data-weights` | Data Weights And Combination | older page, still topical | <https://casaguides.nrao.edu/index.php/DataWeightsAndCombination> | `listobs`, `plotms`, `applycal`, `concat`, `statwt` | ALMA help/science archive references | Wave 6 breadth |
| `alma/na-imaging-template` | Guide to the NA Imaging Template | current topical | <https://casaguides.nrao.edu/index.php/Guide_NA_ImagingTemplate> | pipeline/script template mapping | ALMA QA2/product references | mapping only |
| `alma/pipeline-reprocessing` | ALMA Imaging Pipeline Reprocessing | current topical | <https://casaguides.nrao.edu/index.php/ALMA_Imaging_Pipeline_Reprocessing> | pipeline execution | pipeline products | external pipeline workflow |
| `alma/pipeline-known-issues` | ALMA Pipeline Known Issues | current topical | <https://casaguides.nrao.edu/index.php/ALMA_Pipeline_Known_Issues> | issue reference | ALMA help pages | reference only |

## VLA Current Pages

| Registry area | Guide | CASA version | URL | Extracted CASA surface | Input artifacts / data source | Classification |
|---|---|---:|---|---|---|---|
| `vla/irc10216` | High frequency spectral line data reduction: IRC+10216 | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=VLA_high_frequency_Spectral_Line_tutorial_-_IRC%2B10216> | `listobs`, `plotms`, `gencal`, `applycal`, `split`, `flagdata`, `setjy`, `gaincal`, `bandpass`, `fluxscale`, `mstransform`, `uvcontsub`, `tclean`, `imstat`, `immoments`, `impv`, `statwt`, `plotcal` | `TDRW0001_10s.ms.tgz`, `irc_fors1_dec_header.fits` | Wave 4 spine |
| `vla/3c391` | Continuum Imaging, Mosaicking: 3C391 | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Continuum_Tutorial_3C391-CASA6.7.2> | `listobs`, `plotms`, `tclean`, `split`, `imstat`, `gaincal`, `applycal`, `bandpass`, `fluxscale`, `setjy`, `gencal`, `flagdata`, `delmod`, `statwt`, `plotcal` | `3c391_ctm_mosaic_10s_spw0.ms.tgz`, `AdvancedEVLAcont.tgz`, script archive | Wave 6 breadth |
| `vla/3c75-pol` | Polarization Calibration: 3C75 | 6.7.2 | <https://casaguides.nrao.edu/index.php/Polarization_Calibration_based_on_CASA_pipeline_standard_reduction:_The_radio_galaxy_3C75> | `plotms`, `tclean`, `split`, `imstat`, `immath`, `imsubimage`, `gaincal`, `applycal`, `bandpass`, `setjy`, `flagdata`, `flagmanager`, `delmod`, `statwt` | `CASA6.7.2_Polarization_Guide_Files.tgz`, calibrated MS, pipeline products | Wave 6 breadth |
| `vla/3c129-pband` | Radio galaxy 3C129 P-band continuum | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Radio_galaxy_3C_129:_P-band_continuum_tutorial> | `listobs`, `plotms`, `tclean`, `split`, `gaincal`, `applycal`, `bandpass`, `setjy`, `gencal`, `flagdata`, `hanningsmooth`, `clearcal`, `statwt` | `P_band_3C129.tgz`, ionosphere file, older tar bundle | Wave 6 breadth |
| `vla/mg0414-pband-line` | MG0414+0534 P-band Spectral Line | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=MG0414%2B0534_P-band_Spectral_Line_Tutorial> | `listobs`, `plotms`, `tclean`, `split`, `mstransform`, `gaincal`, `applycal`, `bandpass`, `setjy`, `gencal`, `flagdata`, `hanningsmooth` | `MG0414_d1_data.ms.tgz`, ionosphere file, tutorial script | Wave 6 breadth |
| `vla/hi21-leda44055` | HI 21 cm spectral line: LEDA 44055 | 6.7.2 | <https://casaguides.nrao.edu/index.php/HI_21cm_(1.4_GHz)_spectral_line_data_reduction:_LEDA_44055-CASA6.7.2> | `listobs`, `plotms`, `tclean`, `split`, `imcontsub`, `gaincal`, `applycal`, `bandpass`, `fluxscale`, `setjy`, `gencal`, `flagdata` | NRAO archive / baseline references | Wave 6 breadth |
| `vla/flagging` | VLA CASA Flagging | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Flagging> | `listobs`, `plotms`, `split`, `gaincal`, `applycal`, `bandpass`, `flagdata`, `flagmanager`, `hanningsmooth`, `plotcal` | `SNR_G55_10s.tar.gz` | Wave 6 breadth |
| `vla/imaging` | VLA CASA Imaging | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Imaging> | `tclean`, legacy `clean`, `imhead`, `immath`, image tool `ia.open`, `rmtables` | `SNR_G55_10s.calib.tar.gz` | Wave 6 breadth |
| `vla/selfcal` | VLA Self-calibration Tutorial | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Self-calibration_Tutorial> | `listobs`, `plotms`, `tclean`, `split`, `gaincal`, `applycal`, `plotcal` | `17B-197...tar`, `VLASelf-calibrationTutorial.tar` | Wave 6 breadth |
| `vla/data-combination` | VLA Data Combination - W49A | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Data_Combination> | `listobs`, `plotms`, `tclean`, `concat`, `statwt` | `VLA-combination-W49A.tar.gz` | Wave 6 breadth |
| `vla/source-subtraction` | Source Subtraction in VLA data | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_Source_Subtraction_Topical_Guide> | `listobs`, `plotms`, `tclean`, `imhead`, `imfit`, `ft`, component-list `cl.addcomponent` | `VLA-combination-SgrA-files.tar.gz` | Wave 6 breadth |
| `vla/bandpass-slope` | Correcting for a Spectral Index in Bandpass Calibration | 6.7.2 | <https://casaguides.nrao.edu/index.php/VLA_CASA_Bandpass_Slope> | `listobs`, `plotms`, `gaincal`, `bandpass`, `fluxscale`, `setjy` | `G192-BP.ms.tar.gz` | Wave 6 breadth |

## Simulation Pages

| Registry area | Guide | CASA version | URL | Extracted CASA surface | Input artifacts / data source | Classification |
|---|---|---:|---|---|---|---|
| `simulation/vla-ppdisk` | Protoplanetary Disk Simulation - VLA | 6.7.2 | <https://casaguides.nrao.edu/index.php?title=Protoplanetary_Disk_Simulation_-_VLA-CASA6.7.2> | `importfits`, `immath`, `simobserve`, `simanalyze`, `plotms`, `tclean`, `imhead`, `imstat` | `ppdisk672_GHz_50pc.fits` | Wave 5 spine |
| `simulation/ppdisk-alma-former` | Protoplanetary Disk Simulation | 6.6.6 | <https://casaguides.nrao.edu/index.php/Protoplanetary_Disk_Simulation_CASA_6.6.6> | `simobserve`, `simanalyze`, `imhead`, image tool `ia.open` | `ppdisk672_GHz_50pc.fits` under ALMA sim inputs | breadth/deferred |
| `simulation/simalma` | Simalma | 6.6.6 | <https://casaguides.nrao.edu/index.php/Simalma> | `simalma` task surface | `https://bulk.cv.nrao.edu/almadata/public/casaguides/SimALMA` | Wave 6 breadth candidate |
| `simulation/aca` | ACA Simulation | 6.5.4 | <https://casaguides.nrao.edu/index.php/ACA_Simulation> | `simobserve`, `simanalyze` | no direct data artifact found in API scan | breadth/deferred |
| `simulation/antenna-configs` | Antenna Configurations Models in CASA | 6.6.x | <https://casaguides.nrao.edu/index.php/Antenna_Configurations_Models_in_CASA> | antenna-configuration reference | ALMA simulator/configuration references | support reference |
| `simulation/corruptions` | Corrupting Simulated Data (Simulator Tool) | archived/tool reference | <https://casaguides.nrao.edu/index.php/Corrupting_Simulated_Data_(Simulator_Tool)> | simulator tool `sm.open`, `sm.setnoise`, `sm.setgain`, `sm.corrupt` | none identified | Wave 5/6 corruption reference |

## First Dataset Registry Candidates

| Proposed key | Source URL | Expected filename | Tier | First owning wave |
|---|---|---|---|---|
| `alma/first-look/twhya/calibrated-ms` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar> | `twhya_calibrated.ms.tar` | tutorial-parity | #140 |
| `alma/first-look/twhya/uncalibrated-ms` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_uncalibrated.ms.tar> | `twhya_uncalibrated.ms.tar` | tutorial-parity | #140 |
| `alma/first-look/twhya/calibrated-unflagged-ms` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_calibrated_unflagged.ms.tar> | `twhya_calibrated_unflagged.ms.tar` | tutorial-parity | #140 |
| `alma/first-look/twhya/selfcal-ms` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_selfcal.ms.tgz> | `twhya_selfcal.ms.tgz` | tutorial-parity | #140 |
| `alma/first-look/twhya/continuum-image` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_cont.image> | `twhya_cont.image` | tutorial-parity | #140 |
| `alma/first-look/twhya/n2hp-image` | <https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.6/twhya_n2hp.image> | `twhya_n2hp.image` | tutorial-parity | #140 |
| `vla/irc10216/ms-10s` | <http://casa.nrao.edu/Data/EVLA/IRC10216/TDRW0001_10s.ms.tgz> | `TDRW0001_10s.ms.tgz` | tutorial-parity | #141 |
| `vla/irc10216/fors1-fits` | <http://casa.nrao.edu/Data/EVLA/IRC10216/irc_fors1_dec_header.fits> | `irc_fors1_dec_header.fits` | tutorial-parity | #141 |
| `simulation/vla-ppdisk/model-fits` | <https://casa.nrao.edu/Data/EVLA/simulation/ppdisk672_GHz_50pc.fits> | `ppdisk672_GHz_50pc.fits` | tutorial-parity | #142 |

## Registry Fields For #97

Each future registry entry should include:

- logical key;
- source page URL and source artifact URL;
- expected filename and unpacked root name;
- declared CASA Guide version;
- checksum and size once mirrored;
- dataset tier (`default-fixture`, `tutorial-parity`, `slow-parity`,
  `performance`);
- local path under `CASA_RS_TESTDATA_ROOT`;
- optional local staging root for removable/NAS-backed mirrors;
- first owning wave and downstream dependent waves.

