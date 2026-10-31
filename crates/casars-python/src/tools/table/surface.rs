// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's surface: every method of CASA's installed `table`
//! binding (83) with its parameters and effective defaults (CASA
//! 6.7.6.14), in the shared [`crate::tools::surface`] form.
//!
//! `python/tests/test_tools_casa_inventory.py` keeps this table, the pyo3
//! declarations in [`super`] and the installed CASA binding
//! (`python/tests/data/casa_table_signatures.json`) in step.

#[allow(unused_imports)]
use crate::tools::surface::{
    CasaDefault, Kind, Method, Returns, Status, ToolSurface, method, param,
};

#[allow(unused_imports)]
use CasaDefault::{
    Bool, Bools, Empty, EmptyRecord, Float, Floats, Int, Ints, Required, Text, Texts,
};
use Kind as K;
use Returns as R;
#[allow(unused_imports)]
use Status::{Implemented as Yes, NotYetImplemented as No};

/// Every public method of CASA's installed `table` binding.
const METHODS: &[Method] = &[
    method(
        "addcols",
        &[
            param("desc", K::Any, Required),
            param("dminfo", K::Record, EmptyRecord),
        ],
        R::Any,
        Yes,
    ),
    method(
        "addreadmeline",
        &[param("value", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method("addrows", &[param("nrow", K::Int, Int(1))], R::Any, Yes),
    method("browse", &[], R::Any, No),
    method(
        "calc",
        &[
            param("expr", K::Any, Required),
            param(
                "prefix",
                K::Text,
                Text("using style base0, endincl, fortranorder"),
            ),
            param("showtaql", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method("clearlocks", &[], R::Any, Yes),
    method("close", &[], R::Any, Yes),
    method(
        "colarraytype",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "coldatatype",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "colfieldnames",
        &[
            param("columnname", K::Any, Required),
            param("keyword", K::Text, Text("")),
        ],
        R::Any,
        Yes,
    ),
    method(
        "colkeywordnames",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method("colnames", &[], R::Any, Yes),
    method(
        "copy",
        &[
            param("newtablename", K::Any, Required),
            param("deep", K::Bool, Bool(false)),
            param("valuecopy", K::Bool, Bool(false)),
            param("dminfo", K::Record, EmptyRecord),
            param("endian", K::Text, Text("aipsrc")),
            param("memorytable", K::Bool, Bool(false)),
            param("returnobject", K::Bool, Bool(false)),
            param("norows", K::Bool, Bool(false)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "copyrows",
        &[
            param("outtable", K::Any, Required),
            param("startrowin", K::Int, Int(0)),
            param("startrowout", K::Int, Int(-1)),
            param("nrow", K::Int, Int(-1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "create",
        &[
            param("tablename", K::Text, Text("")),
            param("tabledesc", K::Record, EmptyRecord),
            param("lockoptions", K::Record, EmptyRecord),
            param("endianformat", K::Text, Text("")),
            param("memtype", K::Text, Text("")),
            param("nrow", K::Int, Int(0)),
            param("dminfo", K::Record, EmptyRecord),
        ],
        R::Any,
        Yes,
    ),
    method(
        "createmultitable",
        &[
            param("outputTableName", K::Any, Required),
            param("tables", K::Any, Required),
            param("subdirname", K::Any, Required),
        ],
        R::Any,
        Yes,
    ),
    method("datachanged", &[], R::Any, Yes),
    method("done", &[], R::Any, Yes),
    method("endianformat", &[], R::Any, Yes),
    method(
        "fieldnames",
        &[param("keyword", K::Text, Text(""))],
        R::Any,
        Yes,
    ),
    method("flush", &[], R::Any, Yes),
    method(
        "fromASDM",
        &[
            param("tablename", K::Any, Required),
            param("xmlfile", K::Any, Required),
        ],
        R::Any,
        No,
    ),
    method(
        "fromascii",
        &[
            param("tablename", K::Any, Required),
            param("asciifile", K::Any, Required),
            param("headerfile", K::Text, Text("")),
            param("autoheader", K::Bool, Bool(false)),
            param("autoshape", K::Ints, Ints(&[-1])),
            param("sep", K::Text, Text("")),
            param("commentmarker", K::Text, Text("")),
            param("firstline", K::Int, Int(0)),
            param("lastline", K::Int, Int(-1)),
            param("nomodify", K::Bool, Bool(true)),
            param("columnnames", K::Any, Empty),
            param("datatypes", K::Any, Empty),
        ],
        R::Any,
        No,
    ),
    method(
        "fromfits",
        &[
            param("tablename", K::Any, Required),
            param("fitsfile", K::Any, Required),
            param("whichhdu", K::Int, Int(1)),
            param("storage", K::Text, Text("standard")),
            param("convention", K::Text, Text("none")),
            param("nomodify", K::Bool, Bool(true)),
            param("ack", K::Bool, Bool(true)),
        ],
        R::Any,
        No,
    ),
    method(
        "getcell",
        &[
            param("columnname", K::Any, Required),
            param("rownr", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getcellslice",
        &[
            param("columnname", K::Any, Required),
            param("rownr", K::Any, Required),
            param("blc", K::Any, Required),
            param("trc", K::Any, Required),
            param("incr", K::Ints, Ints(&[1])),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getcol",
        &[
            param("columnname", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getcoldesc",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "getcoliter",
        &[
            param("columnname", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
            param("torecord", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "getcolkeyword",
        &[
            param("columnname", K::Any, Required),
            param("keyword", K::Any, Empty),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getcolkeywords",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "getcolshapestring",
        &[
            param("columnname", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getcolslice",
        &[
            param("columnname", K::Any, Required),
            param("blc", K::Any, Required),
            param("trc", K::Any, Required),
            param("incr", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "getdesc",
        &[param("actual", K::Bool, Bool(true))],
        R::Any,
        Yes,
    ),
    method("getdminfo", &[], R::Any, Yes),
    method(
        "getkeyword",
        &[param("keyword", K::Any, Empty)],
        R::Any,
        Yes,
    ),
    method("getkeywords", &[], R::Any, Yes),
    method(
        "getvarcol",
        &[
            param("columnname", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "haslock",
        &[param("write", K::Bool, Bool(true))],
        R::Any,
        Yes,
    ),
    method("info", &[], R::Any, Yes),
    method(
        "iscelldefined",
        &[
            param("columnname", K::Any, Required),
            param("rownr", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "ismultiused",
        &[param("checksubtables", K::Bool, Bool(false))],
        R::Any,
        Yes,
    ),
    method(
        "isopened",
        &[param("tablename", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "isscalarcol",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "isvarcol",
        &[param("columnname", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method("iswritable", &[], R::Any, Yes),
    method("keywordnames", &[], R::Any, Yes),
    method("listlocks", &[], R::Any, Yes),
    method(
        "lock",
        &[
            param("write", K::Bool, Bool(true)),
            param("nattempts", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method("lockoptions", &[], R::Any, Yes),
    method("name", &[], R::Any, Yes),
    method("ncols", &[], R::Any, Yes),
    method("nrows", &[], R::Any, Yes),
    method("ok", &[], R::Any, Yes),
    method(
        "open",
        &[
            param("tablename", K::Text, Text("")),
            param("lockoptions", K::Record, EmptyRecord),
            param("nomodify", K::Bool, Bool(true)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcell",
        &[
            param("columnname", K::Any, Required),
            param("rownr", K::Any, Required),
            param("thevalue", K::Any, Empty),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcellslice",
        &[
            param("columnname", K::Any, Required),
            param("rownr", K::Any, Required),
            param("value", K::Any, Empty),
            param("blc", K::Any, Empty),
            param("trc", K::Any, Empty),
            param("incr", K::Ints, Ints(&[1])),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcol",
        &[
            param("columnname", K::Any, Required),
            param("value", K::Any, Empty),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcolkeyword",
        &[
            param("columnname", K::Any, Required),
            param("keyword", K::Any, Empty),
            param("value", K::Any, Empty),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcolkeywords",
        &[
            param("columnname", K::Any, Required),
            param("value", K::Any, Required),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putcolslice",
        &[
            param("columnname", K::Any, Required),
            param("value", K::Any, Empty),
            param("blc", K::Any, Empty),
            param("trc", K::Any, Empty),
            param("incr", K::Ints, Ints(&[1])),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method("putinfo", &[param("value", K::Any, Required)], R::Any, Yes),
    method(
        "putkeyword",
        &[
            param("keyword", K::Any, Empty),
            param("value", K::Any, Empty),
            param("makesubrecord", K::Bool, Bool(false)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "putkeywords",
        &[param("value", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "putvarcol",
        &[
            param("columnname", K::Any, Required),
            param("value", K::Any, Required),
            param("startrow", K::Int, Int(0)),
            param("nrow", K::Int, Int(-1)),
            param("rowincr", K::Int, Int(1)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "query",
        &[
            param("query", K::Text, Text("String")),
            param("name", K::Text, Text("")),
            param("sortlist", K::Text, Text("")),
            param("columns", K::Text, Text("")),
            param("style", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "removecolkeyword",
        &[
            param("columnname", K::Any, Required),
            param("keyword", K::Any, Empty),
        ],
        R::Any,
        Yes,
    ),
    method(
        "removecols",
        &[param("columnames", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "removekeyword",
        &[param("keyword", K::Any, Empty)],
        R::Any,
        Yes,
    ),
    method(
        "removerows",
        &[param("rownrs", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "renamecol",
        &[
            param("oldname", K::Any, Required),
            param("newname", K::Any, Required),
        ],
        R::Any,
        Yes,
    ),
    method("resync", &[], R::Any, Yes),
    method(
        "row",
        &[
            param("columnnames", K::Any, Empty),
            param("exclude", K::Bool, Bool(false)),
        ],
        R::Any,
        No,
    ),
    method(
        "rownumbers",
        &[
            param("tab", K::Record, EmptyRecord),
            param("nbytes", K::Int, Int(0)),
        ],
        R::Any,
        Yes,
    ),
    method(
        "selectrows",
        &[
            param("rownrs", K::Any, Required),
            param("name", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method(
        "setmaxcachesize",
        &[
            param("columnname", K::Any, Required),
            param("nbytes", K::Any, Required),
        ],
        R::Any,
        Yes,
    ),
    method(
        "showcache",
        &[param("verbose", K::Bool, Bool(true))],
        R::Any,
        Yes,
    ),
    method(
        "statistics",
        &[
            param("column", K::Any, Required),
            param("complex_value", K::Text, Text("")),
        ],
        R::Any,
        Yes,
    ),
    method(
        "summary",
        &[param("recurse", K::Bool, Bool(false))],
        R::Any,
        No,
    ),
    method(
        "taql",
        &[param("taqlcommand", K::Text, Text("TaQL expression"))],
        R::Any,
        No,
    ),
    method(
        "testincrstman",
        &[param("column", K::Any, Required)],
        R::Any,
        Yes,
    ),
    method(
        "toasciifmt",
        &[
            param("asciifile", K::Any, Required),
            param("headerfile", K::Text, Text("")),
            param("columns", K::Any, Empty),
            param("sep", K::Text, Text("")),
        ],
        R::Any,
        No,
    ),
    method("unlock", &[], R::Any, Yes),
];

/// The `table` tool's surface.
pub(crate) const SURFACE: ToolSurface = ToolSurface {
    name: "table",
    module: "casars.tools",
    constructor: &[],
    methods: METHODS,
    lifecycle: &["open", "close", "done"],
};
