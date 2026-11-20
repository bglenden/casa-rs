// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `coordsys` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1027.

use pyo3::prelude::*;

use super::{PyCoordsys, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCoordsys {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (coordin, absin = None, dopplerin = None, unitsin = None, absout = None, dopplerout = None, unitsout = None, shape = None), text_signature = "($self, coordin, absin=[True], dopplerin='radio', unitsin=['Native'], absout=[True], dopplerout='radio', unitsout=['Native'], shape=[-1])")]
    fn convert(
        &self,
        coordin: &Bound<'_, PyAny>,
        absin: Option<&Bound<'_, PyAny>>,
        dopplerin: Option<&Bound<'_, PyAny>>,
        unitsin: Option<&Bound<'_, PyAny>>,
        absout: Option<&Bound<'_, PyAny>>,
        dopplerout: Option<&Bound<'_, PyAny>>,
        unitsout: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convert")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (frame), text_signature = "($self, frame)")]
    fn convertdirection(&self, frame: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("convertdirection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (coordin = None, absin = None, dopplerin = None, unitsin = None, absout = None, dopplerout = None, unitsout = None, shape = None), text_signature = "($self, coordin=[], absin=[True], dopplerin='radio', unitsin=['Native'], absout=[True], dopplerout='radio', unitsout=['Native'], shape=[-1])")]
    fn convertmany(
        &self,
        coordin: Option<&Bound<'_, PyAny>>,
        absin: Option<&Bound<'_, PyAny>>,
        dopplerin: Option<&Bound<'_, PyAny>>,
        unitsin: Option<&Bound<'_, PyAny>>,
        absout: Option<&Bound<'_, PyAny>>,
        dopplerout: Option<&Bound<'_, PyAny>>,
        unitsout: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("convertmany")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value, frequnit = None, velocity = None), text_signature = "($self, value, frequnit='', velocity=[])")]
    fn frequencytofrequency(
        &self,
        value: &Bound<'_, PyAny>,
        frequnit: Option<&Bound<'_, PyAny>>,
        velocity: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("frequencytofrequency")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value, frequnit = None, doppler = None, velunit = None), text_signature = "($self, value, frequnit='', doppler='radio', velunit='km/s')")]
    fn frequencytovelocity(
        &self,
        value: &Bound<'_, PyAny>,
        frequnit: Option<&Bound<'_, PyAny>>,
        doppler: Option<&Bound<'_, PyAny>>,
        velunit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("frequencytovelocity")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (order), text_signature = "($self, order)")]
    fn reorder(&self, order: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("reorder")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (csys, whichin, whichout), text_signature = "($self, csys, whichin, whichout)")]
    fn replace(
        &self,
        csys: &Bound<'_, PyAny>,
        whichin: &Bound<'_, PyAny>,
        whichout: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("replace")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (direction = None, spectral = None), text_signature = "($self, direction='', spectral='')")]
    fn setconversiontype(
        &self,
        direction: Option<&Bound<'_, PyAny>>,
        spectral: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setconversiontype")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (refcode = None, proj = None, projpar = None, refpix = None, refval = None, incr = None, xform = None, poles = None), text_signature = "($self, refcode='', proj='', projpar=[-1.0], refpix=[-1.0], refval=[], incr=[], xform=[], poles=[])")]
    fn setdirection(
        &self,
        refcode: Option<&Bound<'_, PyAny>>,
        proj: Option<&Bound<'_, PyAny>>,
        projpar: Option<&Bound<'_, PyAny>>,
        refpix: Option<&Bound<'_, PyAny>>,
        refval: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
        xform: Option<&Bound<'_, PyAny>>,
        poles: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setdirection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (r#type = None, value = None), text_signature = "($self, type='', value=[])")]
    fn setlineartransform(
        &self,
        r#type: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setlineartransform")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (r#type, parameters = None), text_signature = "($self, type, parameters=[-1.0])")]
    fn setprojection(
        &self,
        r#type: &Bound<'_, PyAny>,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setprojection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value, r#type = None, adjust = None), text_signature = "($self, value, type='direction', adjust=True)")]
    fn setreferencecode(
        &self,
        value: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyAny>>,
        adjust: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setreferencecode")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixel = None, world = None, mask = None), text_signature = "($self, pixel=[-1], world=[], mask=[False])")]
    fn setreferencelocation(
        &self,
        pixel: Option<&Bound<'_, PyAny>>,
        world: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setreferencelocation")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (refcode = None, restfreq = None, frequencies = None, doppler = None, velocities = None), text_signature = "($self, refcode='', restfreq=[], frequencies=[], doppler='', velocities=[])")]
    fn setspectral(
        &self,
        refcode: Option<&Bound<'_, PyAny>>,
        restfreq: Option<&Bound<'_, PyAny>>,
        frequencies: Option<&Bound<'_, PyAny>>,
        doppler: Option<&Bound<'_, PyAny>>,
        velocities: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("setspectral")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (pixel = None, world = None, which = None), text_signature = "($self, pixel=[-1.0], world=[-1.0], which=0)")]
    fn settabular(
        &self,
        pixel: Option<&Bound<'_, PyAny>>,
        world: Option<&Bound<'_, PyAny>>,
        which: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("settabular")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (originshft = None, newshape = None), text_signature = "($self, originshft=[], newshape=[])")]
    fn subimage(
        &self,
        originshft: Option<&Bound<'_, PyAny>>,
        newshape: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("subimage")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (doppler = None, list = None), text_signature = "($self, doppler='RADIO', list=True)")]
    fn summary(
        &self,
        doppler: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("summary")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, isworld = None), text_signature = "($self, value=[], isworld=-1)")]
    fn toabs(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        isworld: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("toabs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, isworld = None), text_signature = "($self, value=[], isworld=-1)")]
    fn toabsmany(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        isworld: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("toabsmany")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None), text_signature = "($self, value=[])")]
    fn topixel(&self, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("topixel")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, isworld = None), text_signature = "($self, value=[], isworld=-1)")]
    fn torel(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        isworld: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("torel")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, isworld = None), text_signature = "($self, value=[], isworld=-1)")]
    fn torelmany(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        isworld: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("torelmany")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value = None, format = None), text_signature = "($self, value=[], format='n')")]
    fn toworld(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        format: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("toworld")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (order), text_signature = "($self, order)")]
    fn transpose(&self, order: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("transpose")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value, frequnit = None, doppler = None, velunit = None), text_signature = "($self, value, frequnit='', doppler='radio', velunit='km/s')")]
    fn velocitytofrequency(
        &self,
        value: &Bound<'_, PyAny>,
        frequnit: Option<&Bound<'_, PyAny>>,
        doppler: Option<&Bound<'_, PyAny>>,
        velunit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("velocitytofrequency")
    }
}
