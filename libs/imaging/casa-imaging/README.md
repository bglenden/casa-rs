# casa-imaging

Run native CASA-style synthesis imaging and reconstruction.

## Main types

- `ResourcePolicy` — Resource limits the imaging execution must honor.
- `TcleanRefusal` — A reason the requested CASA imaging operation cannot proceed.
- `BackendChoice` — The CPU or device backend requested for imaging.
- `CleanStop` — The condition that stopped minor-cycle reconstruction.

Start with the current defaults for a CASA-style tclean request.

```rust
use casa_imaging::default_tclean_parameters;

let parameters = default_tclean_parameters();
assert!(parameters.contains_key("niter"));
```
