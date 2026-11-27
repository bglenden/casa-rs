// SPDX-License-Identifier: LGPL-3.0-or-later

use std::io::{self, BufWriter, Write};
use std::path::Path;

use casa_ms::MsSelection;
use casa_mstransform::tool_lister::{ToolListerOptions, write_visibility_listing};
use pyo3::prelude::*;
use tempfile::NamedTempFile;

use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error, strict_bool};

#[derive(Default)]
struct ListingText(String);

impl<'a, 'py> FromPyObject<'a, 'py> for ListingText {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        selection_text(&value, "lister").map(Self)
    }
}

impl ListingText {
    fn selector(self) -> Option<String> {
        (!self.0.is_empty()).then_some(self.0)
    }
}

#[pymethods]
impl PyMsTool {
    #[allow(clippy::too_many_arguments, unused_variables)]
    #[pyo3(
        signature = (
            options = ListingText::default(), datacolumn = ListingText("data".to_string()),
            field = ListingText::default(), spw = ListingText::default(), antenna = ListingText::default(),
            timerange = ListingText::default(), correlation = ListingText::default(),
            scan = ListingText::default(), feed = ListingText::default(), array = ListingText::default(),
            observation = ListingText::default(), uvrange = ListingText::default(),
            average = ListingText::default(), showflags = false, msselect = ListingText::default(),
            pagerows = 50, listfile = ListingText::default()
        ),
        text_signature = "($self, options='', datacolumn='data', field='', spw='', antenna='', timerange='', correlation='', scan='', feed='', array='', observation='', uvrange='', average='', showflags=False, msselect='', pagerows=50, listfile='')"
    )]
    fn lister(
        &self,
        py: Python<'_>,
        options: ListingText,
        datacolumn: ListingText,
        field: ListingText,
        spw: ListingText,
        antenna: ListingText,
        timerange: ListingText,
        correlation: ListingText,
        scan: ListingText,
        feed: ListingText,
        array: ListingText,
        observation: ListingText,
        uvrange: ListingText,
        average: ListingText,
        #[pyo3(from_py_with = strict_bool)] showflags: bool,
        msselect: ListingText,
        pagerows: i64,
        listfile: ListingText,
    ) -> PyResult<bool> {
        let request = ToolListerOptions {
            data_column: datacolumn.0,
            selection: MsSelection {
                field: field.selector(),
                spw: spw.selector(),
                antenna: antenna.selector(),
                timerange: timerange.selector(),
                scan: scan.selector(),
                observation: observation.selector(),
                uvrange: uvrange.selector(),
                msselect: msselect.selector(),
                ..MsSelection::default()
            },
            correlation: correlation.0,
            page_rows: pagerows as i32,
        };
        self.attachment_info(py, move |attachment| {
            let Some(attachment) = attachment else {
                return Ok(false);
            };
            let link = attachment.working_metadata_link();
            let snapshot = link.snapshot(None).map_err(runtime_error)?;
            if listfile.0.is_empty() {
                let mut stdout = io::stdout().lock();
                let mut stdin = io::stdin().lock();
                write_visibility_listing(&snapshot.ms, &request, &mut stdout, Some(&mut stdin))
                    .map_err(runtime_error)?;
                link.verify_directory().map_err(runtime_error)?;
                return Ok(true);
            }
            let path = Path::new(&listfile.0);
            if path.exists() {
                return Err(runtime_error(format!(
                    "Error in MSLister::list: File {} already exists",
                    path.display()
                )));
            }
            let directory = path
                .parent()
                .filter(|directory| !directory.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let mut temporary = NamedTempFile::new_in(directory).map_err(runtime_error)?;
            {
                let mut output = BufWriter::new(temporary.as_file_mut());
                write_visibility_listing(&snapshot.ms, &request, &mut output, None)
                    .map_err(runtime_error)?;
                output.flush().map_err(runtime_error)?;
            }
            link.verify_directory().map_err(runtime_error)?;
            temporary.persist_noclobber(path).map_err(runtime_error)?;
            Ok(true)
        })
    }
}
