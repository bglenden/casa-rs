# casa-aipsio

Read and write casacore AipsIO records and primitive values.

## Main types

- `AipsIo` — Versioned object framing with typed scalar and array I/O.
- `AipsIoStream` — Primitive I/O with explicit byte order.
- `ByteOrder` — Byte ordering for casacore wire values.

Write a framed integer into a byte buffer.

```rust
use casa_aipsio::AipsIo;
use std::io::Cursor;

let mut io = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
io.putstart("Example", 1).unwrap();
io.put_i32(42).unwrap();
io.putend().unwrap();
```
