// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA’s typed calibration agent convenience methods.

use casa_contracts::ParameterValue;
use pyo3::exceptions::PySystemError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::convert::DoubleList;

use super::{PyAgentFlagger, arguments::Selectors};

fn clip_range(value: &Bound<'_, PyAny>) -> PyResult<DoubleList> {
    if value.is_instance_of::<PyTuple>() {
        return Err(PySystemError::new_err(
            "<built-in function agentflagger_parseclipparameters> returned a result with an exception set",
        ));
    }
    value.extract()
}

#[pymethods]
impl PyAgentFlagger {
    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", antint_ref_antenna = "", minchanfrac = 0.6, verbose = false, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', antint_ref_antenna='', minchanfrac=0.6, verbose=False, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parseantintparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        antint_ref_antenna: &str,
        minchanfrac: f64,
        verbose: bool,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("antint");
        parameters.insert(
            "antint_ref_antenna".to_owned(),
            ParameterValue::String(antint_ref_antenna.to_owned()),
        );
        parameters.insert("minchanfrac".to_owned(), ParameterValue::Float(minchanfrac));
        parameters.insert("verbose".to_owned(), ParameterValue::Bool(verbose));
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", datacolumn = "DATA", clipminmax = DoubleList::of(&[]), clipoutside = true, channelavg = false, chanbin = 1, timeavg = false, timebin = "", clipzeros = false, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', datacolumn='DATA', clipminmax=[], clipoutside=True, channelavg=False, chanbin=1, timeavg=False, timebin='', clipzeros=False, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parseclipparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        datacolumn: &str,
        #[pyo3(from_py_with = clip_range)] clipminmax: DoubleList,
        clipoutside: bool,
        channelavg: bool,
        chanbin: i64,
        timeavg: bool,
        timebin: &str,
        clipzeros: bool,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("clip");
        parameters.insert(
            "datacolumn".to_owned(),
            ParameterValue::String(datacolumn.to_owned()),
        );
        parameters.insert(
            "clipminmax".to_owned(),
            ParameterValue::Array(
                clipminmax
                    .0
                    .into_iter()
                    .map(ParameterValue::Float)
                    .collect(),
            ),
        );
        parameters.insert("clipoutside".to_owned(), ParameterValue::Bool(clipoutside));
        parameters.insert("channelavg".to_owned(), ParameterValue::Bool(channelavg));
        parameters.insert("chanbin".to_owned(), ParameterValue::Integer(chanbin));
        parameters.insert("timeavg".to_owned(), ParameterValue::Bool(timeavg));
        parameters.insert(
            "timebin".to_owned(),
            ParameterValue::String(timebin.to_owned()),
        );
        parameters.insert("clipzeros".to_owned(), ParameterValue::Bool(clipzeros));
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", lowerlimit = 0.0, upperlimit = 90.0, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', lowerlimit=0.0, upperlimit=90.0, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parseelevationparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        lowerlimit: f64,
        upperlimit: f64,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("elevation");
        parameters.insert("lowerlimit".to_owned(), ParameterValue::Float(lowerlimit));
        parameters.insert("upperlimit".to_owned(), ParameterValue::Float(upperlimit));
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", ntime = 0.0, combinescans = false, extendpols = true, growtime = 50.0, growfreq = 50.0, growaround = false, flagneartime = false, flagnearfreq = false, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', ntime=0.0, combinescans=False, extendpols=True, growtime=50.0, growfreq=50.0, growaround=False, flagneartime=False, flagnearfreq=False, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parseextendparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        ntime: f64,
        combinescans: bool,
        extendpols: bool,
        growtime: f64,
        growfreq: f64,
        growaround: bool,
        flagneartime: bool,
        flagnearfreq: bool,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("extend");
        parameters.insert("ntime".to_owned(), ParameterValue::Float(ntime));
        parameters.insert(
            "combinescans".to_owned(),
            ParameterValue::Bool(combinescans),
        );
        parameters.insert("extendpols".to_owned(), ParameterValue::Bool(extendpols));
        parameters.insert("growtime".to_owned(), ParameterValue::Float(growtime));
        parameters.insert("growfreq".to_owned(), ParameterValue::Float(growfreq));
        parameters.insert("growaround".to_owned(), ParameterValue::Bool(growaround));
        parameters.insert(
            "flagneartime".to_owned(),
            ParameterValue::Bool(flagneartime),
        );
        parameters.insert(
            "flagnearfreq".to_owned(),
            ParameterValue::Bool(flagnearfreq),
        );
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", autocorr = false, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', autocorr=False, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parsemanualparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        autocorr: bool,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("manual");
        parameters.insert("autocorr".to_owned(), ParameterValue::Bool(autocorr));
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", quackmode = "beg", quackinterval = 1.0, quackincrement = false, apply = true), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', quackmode='beg', quackinterval=1.0, quackincrement=False, apply=True)")]
    #[allow(clippy::too_many_arguments)]
    fn parsequackparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        quackmode: &str,
        quackinterval: f64,
        quackincrement: bool,
        apply: bool,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("quack");
        parameters.insert(
            "quackmode".to_owned(),
            ParameterValue::String(quackmode.to_owned()),
        );
        parameters.insert(
            "quackinterval".to_owned(),
            ParameterValue::Float(quackinterval),
        );
        parameters.insert(
            "quackincrement".to_owned(),
            ParameterValue::Bool(quackincrement),
        );
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", spwchan = false, spwcorr = false, basecnt = false, fieldcnt = false, name = ""), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', spwchan=False, spwcorr=False, basecnt=False, fieldcnt=False, name='')")]
    #[allow(clippy::too_many_arguments)]
    fn parsesummaryparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        spwchan: bool,
        spwcorr: bool,
        basecnt: bool,
        fieldcnt: bool,
        name: &str,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("summary");
        parameters.insert("spwchan".to_owned(), ParameterValue::Bool(spwchan));
        parameters.insert("spwcorr".to_owned(), ParameterValue::Bool(spwcorr));
        parameters.insert("basecnt".to_owned(), ParameterValue::Bool(basecnt));
        parameters.insert("fieldcnt".to_owned(), ParameterValue::Bool(fieldcnt));
        parameters.insert("name".to_owned(), ParameterValue::String(name.to_owned()));
        self.queue(py, &parameters, true)
    }

    #[pyo3(signature = (field = "", spw = "", array = "", feed = "", scan = "", antenna = "", uvrange = "", time = "", correlation = "", intent = "", observation = "", ntime = 0.0, combinescans = false, datacolumn = "DATA", timecutoff = 4.0, freqcutoff = 3.0, timefit = "line", freqfit = "poly", maxnpieces = 7, flagdimension = "freqtime", usewindowstats = "none", halfwin = 1, extendflags = true, apply = true, channelavg = false, chanbin = 1, timeavg = false, timebin = ""), text_signature = "($self, field='', spw='', array='', feed='', scan='', antenna='', uvrange='', time='', correlation='', intent='', observation='', ntime=0.0, combinescans=False, datacolumn='DATA', timecutoff=4.0, freqcutoff=3.0, timefit='line', freqfit='poly', maxnpieces=7, flagdimension='freqtime', usewindowstats='none', halfwin=1, extendflags=True, apply=True, channelavg=False, chanbin=1, timeavg=False, timebin='')")]
    #[allow(clippy::too_many_arguments)]
    fn parsetfcropparameters(
        &self,
        py: Python<'_>,
        field: &str,
        spw: &str,
        array: &str,
        feed: &str,
        scan: &str,
        antenna: &str,
        uvrange: &str,
        time: &str,
        correlation: &str,
        intent: &str,
        observation: &str,
        ntime: f64,
        combinescans: bool,
        datacolumn: &str,
        timecutoff: f64,
        freqcutoff: f64,
        timefit: &str,
        freqfit: &str,
        maxnpieces: i64,
        flagdimension: &str,
        usewindowstats: &str,
        halfwin: i64,
        extendflags: bool,
        apply: bool,
        channelavg: bool,
        chanbin: i64,
        timeavg: bool,
        timebin: &str,
    ) -> PyResult<bool> {
        let mut parameters = Selectors {
            field,
            spw,
            array,
            feed,
            scan,
            antenna,
            uvrange,
            timerange: time,
            correlation,
            intent,
            observation,
        }
        .parameters("tfcrop");
        parameters.insert(
            "correlation".to_owned(),
            ParameterValue::String(correlation.to_owned()),
        );
        parameters.insert("ntime".to_owned(), ParameterValue::Float(ntime));
        parameters.insert(
            "combinescans".to_owned(),
            ParameterValue::Bool(combinescans),
        );
        parameters.insert(
            "datacolumn".to_owned(),
            ParameterValue::String(datacolumn.to_owned()),
        );
        parameters.insert("timecutoff".to_owned(), ParameterValue::Float(timecutoff));
        parameters.insert("freqcutoff".to_owned(), ParameterValue::Float(freqcutoff));
        parameters.insert(
            "timefit".to_owned(),
            ParameterValue::String(timefit.to_owned()),
        );
        parameters.insert(
            "freqfit".to_owned(),
            ParameterValue::String(freqfit.to_owned()),
        );
        parameters.insert("maxnpieces".to_owned(), ParameterValue::Integer(maxnpieces));
        parameters.insert(
            "flagdimension".to_owned(),
            ParameterValue::String(flagdimension.to_owned()),
        );
        parameters.insert(
            "usewindowstats".to_owned(),
            ParameterValue::String(usewindowstats.to_owned()),
        );
        parameters.insert("halfwin".to_owned(), ParameterValue::Integer(halfwin));
        parameters.insert("extendflags".to_owned(), ParameterValue::Bool(extendflags));
        parameters.insert("apply".to_owned(), ParameterValue::Bool(apply));
        parameters.insert("channelavg".to_owned(), ParameterValue::Bool(channelavg));
        parameters.insert("chanbin".to_owned(), ParameterValue::Integer(chanbin));
        parameters.insert("timeavg".to_owned(), ParameterValue::Bool(timeavg));
        parameters.insert(
            "timebin".to_owned(),
            ParameterValue::String(timebin.to_owned()),
        );
        self.queue(py, &parameters, true)
    }
}
