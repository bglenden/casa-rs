// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL help strings for the casacore MSCAL function families.
//! Text from casacore derivedmscal/DerivedMC/Register.cc.
//! Copyright (C) 2010 Associated Universities, Inc. Washington DC, USA.

use super::{TaqlError, eval::ExprValue};

pub(super) fn help(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let topic = match args {
        [] => String::new(),
        [ExprValue::String(topic)] => topic.to_ascii_lowercase(),
        [_] => {
            return Err(TaqlError::TypeError {
                message: format!("{name} requires a string topic"),
            });
        }
        _ => {
            return Err(TaqlError::ArgumentCount {
                name: name.to_owned(),
                expected: "zero or one".into(),
                got: args.len(),
            });
        }
    };
    let body = match topic.as_str() {
        "" => format!(
            "{DERIVED}\n{STOKES_SHORT}\n{SELECTION}\n{SUBTABLE}\nUse 'show function mscal <type> for more information\n"
        ),
        "derived" => DERIVED.into(),
        "stokes" => STOKES.into(),
        "selection" => SELECTION.into(),
        "subtable" | "subtables" => SUBTABLE.into(),
        _ => {
            return Ok(ExprValue::String(format!(
                "{topic} is an unknown mscal subtype; use derived, stokes, selection or subtable\n"
            )));
        }
    };
    Ok(ExprValue::String(body + FOOTER))
}

const DERIVED: &str = concat!(
    "Derived direction coordinates functions\n",
    " A direction parameter can be given to the functions.\n",
    " It can be the name of a FIELD subtable column (e.g., 'DELAY_DIR'),\n",
    " the name of a source (e.g., 'SUN'), or a RA,DEC pair defining\n",
    " the J2000 source direction (e.g., [10h42m31.3, 45d51m16]).\n",
    " If no direction argument is given, column DELAY_DIR is used for the\n",
    " delay functions, otherwise column PHASE_DIR.\n",
    "  double MSCAL.HA()              hourangle of array center\n",
    "  double MSCAL.HA1()             hourangle of ANTENNA1\n",
    "  double MSCAL.HA2()             hourangle of ANTENNA2\n",
    "  doublearray MSCAL.HADEC()      hourangle/declination of array center\n",
    "  doublearray MSCAL.HADEC1()     hourangle/declination of ANTENNA1\n",
    "  doublearray MSCAL.HADEC2()     hourangle/declination of ANTENNA2\n",
    "  doublearray MSCAL.AZEL()       azimuth/elevation of array center\n",
    "  doublearray MSCAL.AZEL1()      azimuth/elevation of ANTENNA1\n",
    "  doublearray MSCAL.AZEL2()      azimuth/elevation of ANTENNA2\n",
    "  doublearray MSCAL.ITRF()       direction in ITRF coordinates\n",
    "  double MSCAL.LAST()            local sidereal time of array center\n",
    "  double MSCAL.LAST1()           local sidereal time of ANTENNA1\n",
    "  double MSCAL.LAST2()           local sidereal time of ANTENNA2\n",
    "  double MSCAL.PA1()             parallactic angle of ANTENNA1\n",
    "  double MSCAL.PA2()             parallactic angle of ANTENNA2\n",
    "  doublearray MSCAL.UVWWVL()     stored UVW coordinates in wvl for reffreq\n",
    "  doublearray MSCAL.UVWWVLS()    stored UVW coordinates in wvl per channel\n",
    "  doublearray MSCAL.UVWJ2000()     calc J2000 UVW coordinates in meters\n",
    "  doublearray MSCAL.UVWJ2000WVL()  calc J2000 UVW coordinates in wvl for reffreq\n",
    "  doublearray MSCAL.UVWJ2000WVLS() calc J2000 UVW coordinates in wvl per channel\n",
    "  doublearray MSCAL.UVWAPP()     calc Apparent UVW coordinates in meters\n",
    "  doublearray MSCAL.UVWAPPWVL()  calc Apparent UVW coordinates in wvl for reffreq\n",
    "  doublearray MSCAL.UVWAPPWVLS() calc Apparent UVW coordinates in wvl per channel\n",
    "  double MSCAL.DELAY1()   calc delay (seconds) of ANTENNA1 w.r.t. array center\n",
    "  double MSCAL.DELAY2()   calc delay (seconds) of ANTENNA2 w.r.t. array center\n",
    "  double MSCAL.DELAY1()   calc delay (seconds) of ANTENNA1 w.r.t. ANTENNA2\n",
);

const STOKES: &str = concat!(
    "Stokes conversion functions:\n",
    "  complexarray MSCAL.STOKES(complexarray, string)   convert the data\n",
    "  doublearray  MSCAL.STOKES(doublearray,  string)   combine the weights\n",
    "  boolarray    MSCAL.STOKES(boolarray,    string)   combine the flags\n",
    "\n",
    "The case-insensitive string argument defines the output Stokes axes.\n",
    "It must be a comma separated list of Stokes names. All values\n",
    "defined in the Casacore class Stokes are possible. Most important are:\n",
    "   XX, XY, YX, and/or YY.  LINEAR or LIN means XX,XY,YX,YY.\n",
    "   RR, RL, LR, and/or LL.  CIRCULAR or CIRC means RR,RL,LR,LL.\n",
    "   I, Q, U, and/or V.      IQUV or STOKES means I,Q,U,V.\n",
    "   PTOTAL is the polarized intensity (sqrt(Q**2+U**2+V**2))\n",
    "   PLINEAR is the linearly polarized intensity (sqrt(Q**2+U**2))\n",
    "   PFTOTAL is the polarization fraction (Ptotal/I)\n",
    "   PFLINEAR is the linear polarization fraction (Plinear/I)\n",
    "   PANGLE is the linear polarization angle (0.5*arctan(U/Q)) (in radians)\n",
    "If not given, the string argument defaults to 'IQUV'.\n",
);

const SELECTION: &str = concat!(
    "CASA selection functions:\n",
    "  bool MSCAL.BASELINE (string)  select using a baseline string\n",
    "  bool MSCAL.TIME (string)      select using a time string\n",
    "  bool MSCAL.FIELD (string)     select using a field string\n",
    "  bool MSCAL.FEED (string)      select using a feed string\n",
    "  bool MSCAL.SCAN (string)      select using a scan string\n",
    "  bool MSCAL.SPW (string)       select using a spectral-window string\n",
    "  bool MSCAL.UVDIST (string)    select using a uv-distance string\n",
    "  bool MSCAL.STATE (string)     select using a state string\n",
    "  bool MSCAL.OBS (string)       select using an observation string\n",
    "  bool MSCAL.ARRAY (string)     select using an array string\n",
    " More information about the CASA selection syntax can be found at\n",
    " http://casacore.github.io/casacore-notes/263.html\n",
);

const SUBTABLE: &str = concat!(
    "Subtable information functions:\n",
    "  MSCAL.ANT1NAME()          the name of ANTENNA1\n",
    "  MSCAL.ANT2NAME()          the name of ANTENNA2\n",
    "  MSCAL.ANT1COL('column')   for ANTENNA1 the value in given column in ANTENNA subtable\n",
    "  MSCAL.ANT2COL('column')   for ANTENNA2 the value in given column in ANTENNA subtable\n",
    "  MSCAL.STATECOL('column')  for STATE_ID the value in given column in STATE subtable\n",
    "  MSCAL.OBSCOL('column')    for OBSERVATION_ID the value in given column in OBSERVATION subtable\n",
    "  MSCAL.SPWCOL('column')    for DATA_DESC_ID the value in given column in SPECTRAL_WINDOW subtable\n",
    "  MSCAL.POLCOL('column')    for DATA_DESC_ID the value in given column in POLARIZATION subtable\n",
    "  MSCAL.FIELDCOL('column')  for FIELD_ID the value in given column in FIELD subtable\n",
    "  MSCAL.PROCCOL('column')   for PROCESSOR_ID the value in given column in PROCESSOR subtable\n",
    "  MSCAL.SUBCOL('subtable', 'column', 'idcolumn') \n",
    "    for the id-column the value in given column in given subtable\n",
);

const STOKES_SHORT: &str = concat!(
    "Stokes conversion functions:\n",
    "  complexarray MSCAL.STOKES(complexarray, string)   convert the data\n",
    "  doublearray  MSCAL.STOKES(doublearray,  string)   combine the weights\n",
    "  boolarray    MSCAL.STOKES(boolarray,    string)   combine the flags\n",
);

const FOOTER: &str = concat!(
    "\n",
    "See also section 'Special MeasurementSet functions' at\n",
    "http://casacore.github.io/casacore-notes/199.html\n",
);
