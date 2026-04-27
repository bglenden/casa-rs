// SPDX-License-Identifier: LGPL-3.0-or-later
//! Machine-readable UI schema for `casars-imager`.

use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiInjectedArgument,
    UiManagedOutputSchema, UiValueKind,
};

/// Build the launcher-facing UI schema for the standalone imager.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: 1,
        command_id: "imager".to_string(),
        invocation_name: program_name.to_string(),
        display_name: "Imager".to_string(),
        category: "Imaging".to_string(),
        summary: "Run CASA-compatible dirty and deconvolved imaging from a MeasurementSet"
            .to_string(),
        usage: format!(
            "{program_name} --ms PATH --imagename PREFIX --imsize N --cell-arcsec ARCSEC [options]"
        ),
        arguments: vec![
            option_argument(OptionArgumentConfig {
                id: "ms",
                label: "MeasurementSet",
                order: 0,
                flags: &["--ms"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Input MeasurementSet path",
                group: "Context",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "imagename",
                label: "Image Prefix",
                order: 1,
                flags: &["--imagename"],
                metavar: "PREFIX",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Output CASA image prefix",
                group: "Products",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "imsize",
                label: "Image Size",
                order: 2,
                flags: &["--imsize"],
                metavar: "PIXELS",
                value_kind: UiValueKind::String,
                default: Some("512"),
                choices: &[],
                help: "Square image size in pixels",
                group: "Stage Parameters",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "cell_arcsec",
                label: "Cell Size (arcsec)",
                order: 3,
                flags: &["--cell-arcsec"],
                metavar: "ARCSEC",
                value_kind: UiValueKind::Float,
                default: Some("1.0"),
                choices: &[],
                help: "Image cell size in arcseconds",
                group: "Stage Parameters",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "datacolumn",
                label: "Data Column",
                order: 4,
                flags: &["--datacolumn"],
                metavar: "NAME",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &["DATA", "CORRECTED_DATA", "MODEL_DATA"],
                help: "Visibility column to image; default is auto selection",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "savemodel",
                label: "Save Model",
                order: 5,
                flags: &["--savemodel"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("none"),
                choices: &["none", "modelcolumn"],
                help: "Write the predicted final model into the MeasurementSet",
                group: "Products",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "field",
                label: "Field IDs",
                order: 6,
                flags: &["--field"],
                metavar: "IDS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Selected FIELD_ID values in CASA selector syntax",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "phasecenter_field",
                label: "Phasecenter Field",
                order: 7,
                flags: &["--phasecenter-field"],
                metavar: "ID",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "FIELD_ID used as the imaging phase center",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "phasecenter",
                label: "Explicit Phasecenter",
                order: 8,
                flags: &["--phasecenter"],
                metavar: "DIR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Explicit CASA-style J2000 phase center",
                group: "Context",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "ddid",
                label: "DDID",
                order: 9,
                flags: &["--ddid"],
                metavar: "ID",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Restrict imaging to one DATA_DESC_ID",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "spw",
                label: "SPW Selector",
                order: 9,
                flags: &["--spw"],
                metavar: "SPEC",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "CASA spectral-window selector, optionally with channel clauses",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "channel_start",
                label: "Channel Start",
                order: 10,
                flags: &["--channel-start"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "First selected input channel",
                group: "Context",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "channel_count",
                label: "Channel Count",
                order: 11,
                flags: &["--channel-count"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Number of selected input channels",
                group: "Context",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "polarization",
                label: "Corr / Stokes",
                order: 12,
                flags: &["--corr", "--stokes"],
                metavar: "PLANE",
                value_kind: UiValueKind::Choice,
                default: Some("I"),
                choices: &["I", "Q", "U", "V", "XX", "YY", "RR", "LL"],
                help: "Scalar Stokes plane or explicit raw-correlation plane",
                group: "Context",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "specmode",
                label: "Spectral Mode",
                order: 13,
                flags: &["--specmode"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("mfs"),
                choices: &["mfs", "cube", "cubedata"],
                help: "MFS continuum imaging or per-channel spectral cubes",
                group: "Stages",
                required: true,
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "dirty_only",
                label: "Dirty Only",
                order: 14,
                true_flags: &["--dirty-only"],
                false_flags: &[],
                default: Some("false"),
                help: "Skip CLEAN and only write dirty / residual products",
                group: "Stages",
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "deconvolver",
                label: "Deconvolver",
                order: 15,
                flags: &["--deconvolver"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("hogbom"),
                choices: &["hogbom", "mtmfs", "clark", "multiscale"],
                help: "Minor-cycle algorithm used for deconvolution",
                group: "Stages",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "weighting",
                label: "Weighting",
                order: 16,
                flags: &["--weighting"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("natural"),
                choices: &["natural", "uniform", "briggs"],
                help: "Visibility weighting policy",
                group: "Stages",
                required: true,
                advanced: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "perchanweightdensity",
                label: "Per-Channel Density",
                order: 17,
                true_flags: &["--perchanweightdensity"],
                false_flags: &[],
                default: Some("false"),
                help: "Use per-output-channel density estimates for cube weighting",
                group: "Stages",
                advanced: true,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "usepointing",
                label: "Use POINTING",
                order: 18,
                true_flags: &["--usepointing", "--use-pointing"],
                false_flags: &[],
                default: Some("false"),
                help: "Use POINTING-table directions instead of FIELD phase centers",
                group: "Stages",
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "wterm",
                label: "W-Term",
                order: 19,
                flags: &["--wterm"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("none"),
                choices: &["none", "direct", "wproject"],
                help: "W-term correction mode",
                group: "Stages",
                required: true,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "wprojplanes",
                label: "W-Project Planes",
                order: 20,
                flags: &["--wprojplanes"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Explicit wproject plane budget",
                group: "Stages",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "nterms",
                label: "Taylor Terms",
                order: 21,
                flags: &["--nterms"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: Some("1"),
                choices: &[],
                help: "MTMFS Taylor-term count",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "start",
                label: "Cube Start",
                order: 22,
                flags: &["--start"],
                metavar: "VALUE",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Cube-axis start channel, frequency, or velocity",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "width",
                label: "Cube Width",
                order: 23,
                flags: &["--width"],
                metavar: "VALUE",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Cube-axis width as channels, frequency, or velocity",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "outframe",
                label: "Output Frame",
                order: 24,
                flags: &["--outframe"],
                metavar: "FRAME",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &[
                    "LSRK", "BARY", "TOPO", "REST", "LSRD", "GEO", "GALACTO", "LGROUP", "CMB",
                ],
                help: "Spectral frame used for cube output coordinates",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "veltype",
                label: "Velocity Type",
                order: 25,
                flags: &["--veltype"],
                metavar: "TYPE",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &["RADIO", "OPTICAL", "TRUE", "BETA", "GAMMA"],
                help: "Velocity convention for Doppler-style cube axes",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "interpolation",
                label: "Cube Interp",
                order: 26,
                flags: &["--interpolation"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &["nearest", "linear", "cubic"],
                help: "Spectral interpolation mode for cube gridding",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "restfreq",
                label: "Rest Frequency",
                order: 27,
                flags: &["--restfreq"],
                metavar: "FREQ",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Rest frequency used for velocity-style cube axes",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "uvtaper",
                label: "UV Taper",
                order: 28,
                flags: &["--uvtaper"],
                metavar: "SPEC",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Gaussian UV taper: MAJOR[,MINOR[,PA]]",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "restoringbeam",
                label: "Restoring Beam",
                order: 29,
                flags: &["--restoringbeam"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &["common"],
                help: "Restored-beam policy for cube outputs",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "scales",
                label: "Multiscale Sizes",
                order: 30,
                flags: &["--scales"],
                metavar: "PIXELS",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Comma-separated multiscale kernel sizes in pixels",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "smallscalebias",
                label: "Small-Scale Bias",
                order: 31,
                flags: &["--smallscalebias"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.0"),
                choices: &[],
                help: "CASA multiscale bias in [-1, 1]",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "robust",
                label: "Briggs Robust",
                order: 32,
                flags: &["--robust"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.5"),
                choices: &[],
                help: "Briggs robust parameter when weighting=briggs",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "niter",
                label: "Max Iterations",
                order: 33,
                flags: &["--niter"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: Some("0"),
                choices: &[],
                help: "Minor-cycle iteration budget",
                group: "Stage Parameters",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "gain",
                label: "Loop Gain",
                order: 34,
                flags: &["--gain"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.1"),
                choices: &[],
                help: "Minor-cycle loop gain",
                group: "Stage Parameters",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "threshold_jy",
                label: "Threshold (Jy)",
                order: 35,
                flags: &["--threshold-jy"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.0"),
                choices: &[],
                help: "Absolute CLEAN threshold in Jy/beam",
                group: "Stage Parameters",
                required: false,
                advanced: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "nsigma",
                label: "Nsigma",
                order: 36,
                flags: &["--nsigma"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.0"),
                choices: &[],
                help: "Robust-RMS stopping multiplier",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "psfcutoff",
                label: "PSF Cutoff",
                order: 37,
                flags: &["--psfcutoff"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.35"),
                choices: &[],
                help: "PSF beam-fit cutoff fraction",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "minor_cycle_length",
                label: "Minor Cycle Length",
                order: 38,
                flags: &["--minor-cycle-length"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: Some("8"),
                choices: &[],
                help: "Residual refresh cadence in component updates",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "cyclefactor",
                label: "Cycle Factor",
                order: 39,
                flags: &["--cyclefactor"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("1.0"),
                choices: &[],
                help: "Scale factor used for cycle-threshold derivation",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "minpsffraction",
                label: "Min PSF Fraction",
                order: 40,
                flags: &["--minpsffraction"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.1"),
                choices: &[],
                help: "Lower clamp for the PSF-fraction controller term",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "maxpsffraction",
                label: "Max PSF Fraction",
                order: 41,
                flags: &["--maxpsffraction"],
                metavar: "VALUE",
                value_kind: UiValueKind::Float,
                default: Some("0.8"),
                choices: &[],
                help: "Upper clamp for the PSF-fraction controller term",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "mask_box",
                label: "Mask Box",
                order: 42,
                flags: &["--mask-box"],
                metavar: "X0,Y0,X1,Y1",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Inclusive clean mask box in pixel coordinates",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            option_argument(OptionArgumentConfig {
                id: "mask_image",
                label: "Mask Image",
                order: 43,
                flags: &["--mask-image"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "CASA image mask whose non-zero pixels are cleanable",
                group: "Stage Parameters",
                required: false,
                advanced: true,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "write_preview_pngs",
                label: "Preview PNGs",
                order: 44,
                true_flags: &[],
                false_flags: &["--no-preview-pngs"],
                default: Some("true"),
                help: "Write PNG sidecars for the main CASA image products",
                group: "Products",
                advanced: false,
            }),
            action_argument(45, "ui_schema", &["--ui-schema"], UiActionKind::UiSchema),
            action_argument(46, "help", &["-h", "--help"], UiActionKind::Help),
        ],
        managed_output: Some(UiManagedOutputSchema {
            renderer: "imager-run-v1".to_string(),
            stdout_format: "json".to_string(),
            inject_arguments: vec![UiInjectedArgument {
                flag: "--managed-output".to_string(),
                value: "true".to_string(),
            }],
            raw_stdout_available: true,
            raw_stderr_available: true,
        }),
    }
}

struct OptionArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    default: Option<&'a str>,
    choices: &'a [&'a str],
    help: &'a str,
    group: &'a str,
    required: bool,
    advanced: bool,
}

struct ToggleArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    true_flags: &'a [&'a str],
    false_flags: &'a [&'a str],
    default: Option<&'a str>,
    help: &'a str,
    group: &'a str,
    advanced: bool,
}

fn option_argument(config: OptionArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Option {
            flags: config
                .flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
            metavar: config.metavar.to_string(),
            choices: config
                .choices
                .iter()
                .map(|choice| (*choice).to_string())
                .collect(),
        },
        value_kind: config.value_kind,
        required: config.required,
        default: config.default.map(ToString::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: false,
    }
}

fn toggle_argument(config: ToggleArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Toggle {
            true_flags: config
                .true_flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
            false_flags: config
                .false_flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: config.default.map(ToString::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: false,
    }
}

fn action_argument(
    order: usize,
    id: &str,
    flags: &[&str],
    action: UiActionKind,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: id.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: match action {
            UiActionKind::Help => "Print this help message".to_string(),
            UiActionKind::UiSchema => "Print the machine-readable UI schema".to_string(),
        },
        group: "Meta".to_string(),
        advanced: false,
        hidden_in_tui: true,
    }
}

#[cfg(test)]
mod tests {
    use super::command_schema;
    use casa_ms::ui_schema::{UiArgumentParser, UiValueKind};

    #[test]
    fn schema_exposes_workflow_surface_for_casars() {
        let schema = command_schema("casars-imager");
        assert_eq!(schema.command_id, "imager");
        assert_eq!(schema.display_name, "Imager");
        assert_eq!(schema.category, "Imaging");
        assert_eq!(
            schema
                .managed_output
                .as_ref()
                .map(|output| output.renderer.as_str()),
            Some("imager-run-v1")
        );

        let specmode = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "specmode")
            .expect("specmode argument");
        assert_eq!(specmode.group, "Stages");
        assert!(matches!(specmode.value_kind, UiValueKind::Choice));

        let usepointing = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "usepointing")
            .expect("usepointing argument");
        assert_eq!(usepointing.default.as_deref(), Some("false"));
        assert!(matches!(usepointing.value_kind, UiValueKind::Bool));
        let UiArgumentParser::Toggle { true_flags, .. } = &usepointing.parser else {
            panic!("usepointing should use a toggle parser");
        };
        assert!(true_flags.contains(&"--usepointing".to_string()));

        let savemodel = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "savemodel")
            .expect("savemodel argument");
        assert_eq!(savemodel.default.as_deref(), Some("none"));
        let UiArgumentParser::Option { choices, .. } = &savemodel.parser else {
            panic!("savemodel should use an option parser");
        };
        assert!(choices.contains(&"modelcolumn".to_string()));

        let polarization = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "polarization")
            .expect("polarization argument");
        let UiArgumentParser::Option { choices, .. } = &polarization.parser else {
            panic!("polarization should use an option parser");
        };
        assert!(choices.contains(&"I".to_string()));
        assert!(choices.contains(&"XX".to_string()));
    }
}
