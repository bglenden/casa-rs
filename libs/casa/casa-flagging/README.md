# casa-flagging

Calculate, write, and manage visibility flags in MeasurementSets.

## Main types

- `FlagDataRequest` — Visibility selection, flagging agent, and requested action.
- `FlagDataMode` — A flagging algorithm or summary request.
- `FlagDataAction` — Whether to calculate or apply flags.
- `FlagVersionEntry` — A named saved set of flags and its comment.

Build a request to clear manual flags on the selected data.

```rust
use casa_flagging::{FlagDataAction, FlagDataMode, FlagDataRequest};

let request = FlagDataRequest {
    action: FlagDataAction::Unflag,
    ..FlagDataRequest::default()
};
assert!(matches!(request.mode, FlagDataMode::Manual));
```
