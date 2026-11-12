# Independent IncrementalStMan shape fixture

Produced by installed CASA 6.7.6.14, using
`tools/simulation/array_shapes_oracle.py` with a synthetic MeasurementSet
DATA descriptor. `reference.json` records the C++ shape/undefinedness results.
Rows 0 through 4 are undefined, [2,1], carried-forward [2,1], [1,5], [2,4].
The complex payloads are all 7; the table contains no observational data.

The metadata reader follows casacore ISMColumn/ISMBucket row registration and
StManArrayFile::getShape; it must not decode the numerical array payload.
