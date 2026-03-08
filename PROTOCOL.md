# Sparsync experimental protocol (`sparsync://`)

## Status
This document specifies the current `sparsync://` wire behavior as implemented by `sparsync` today.

It is **experimental**. Compatibility is intentionally strict in v1, and breaking changes are possible between releases.

## Scope
This spec describes:
- Endpoint and transport requirements for `sparsync://`.
- On-wire frame encoding and payload formats.
- Request/response and streaming operation semantics.
- Authentication and authorization behavior relevant to interoperability.

This spec does **not** define CLI UX, profile storage, or bootstrap orchestration details.

## Endpoint and transport
`sparsync://` endpoints use QUIC with TLS 1.3.

URI form:
```text
sparsync://<host>[:<port>]/<path>
```

Notes:
- Default port is implementation-configured (`28792` in current CLI defaults).
- The URI path is consumed by higher-level orchestration, not by the protocol frames themselves.
- The protocol transfers **relative paths** rooted at the server's configured destination root.

QUIC/TLS requirements:
- ALPN: `sparsync/1`
- TLS version: 1.3
- Server authentication: required (client validates server cert against configured CA trust)
- mTLS: optional at transport level, required when server is configured with client CA

## Connection and stream model
- A QUIC connection may carry many concurrent bidirectional streams.
- Protocol frames are exchanged over bidirectional streams.
- A single stream may carry multiple request/response frame pairs in sequence.
- Request pipelining on one stream is allowed; responses are emitted in request order.
- `SourceStreamRequest` is a special streaming mode on a stream (server emits a frame sequence rather than one response frame).

## Frame envelope
Each frame on a stream is:
```text
[16-byte prefix][rkyv-serialized Frame header][optional payload bytes]
```

Prefix layout (all multi-byte integers are big-endian):

| Offset | Size | Field | Value / meaning |
|---|---:|---|---|
| 0 | 4 | magic | ASCII `SPS1` |
| 4 | 2 | wire_protocol | `1` (current `PROTOCOL_VERSION`) |
| 6 | 1 | codec_id | `1` (`rkyv`) |
| 7 | 1 | endianness_id | `1` little, `2` big |
| 8 | 4 | header_len | serialized `Frame` byte length |
| 12 | 4 | payload_len | payload byte length |

Validation rules:
- Prefix checks happen before frame decode.
- Magic, protocol id, codec id, and endianness must match local expectations.
- Total frame size is `16 + header_len + payload_len`.
- Trailing or truncated bytes are errors.
- `Frame` decode uses rkyv validation (`check_bytes`).

## Compatibility handshake
Clients should perform a compatibility probe early:
- Send `HelloRequest`.
- Expect `HelloResponse`.
- Proceed only if `accepted=true`.

`HelloRequest` fields:
- `protocol_version: u16`
- `codec: WireCodec` (`Rkyv`)
- `endianness: WireEndianness` (`Little` or `Big`)
- `binary_version: String` (build version string)

Current server acceptance policy is strict equality on all of:
- `protocol_version`
- `codec`
- `endianness`
- `binary_version`

If any mismatch occurs, server returns `HelloResponse { accepted: false, message: ... }`.

## Frame catalog
All frame types are rkyv-serialized enum variants of `Frame`.

`Frame` variants:
- `HelloRequest`
- `HelloResponse`
- `InitFileRequest`
- `InitFileResponse`
- `InitBatchRequest`
- `InitBatchResponse`
- `UploadBatchRequest`
- `UploadBatchResponse`
- `UploadSmallBatchRequest`
- `UploadSmallBatchResponse`
- `UploadColdBatchRequest`
- `UploadColdBatchResponse`
- `SyncSymlinkBatchRequest`
- `SyncSymlinkBatchResponse`
- `SyncFileMetadataBatchRequest`
- `SyncFileMetadataBatchResponse`
- `SourceStreamRequest`
- `SourceStreamFileStart`
- `SourceStreamChunk`
- `SourceStreamFileEnd`
- `SourceStreamDone`
- `DeletePlanRequest`
- `DeletePlanResponse`
- `Error`

Payload usage by frame:

| Frame | Payload expectation |
|---|---|
| `UploadBatchRequest` | Required: chunk-batch encoded payload |
| `UploadSmallBatchRequest` | Required: concatenated file payload bytes |
| `UploadColdBatchRequest` | Required: concatenated file payload bytes |
| `DeletePlanRequest(stage=AddKeepChunk)` | Optional/typical: UTF-8 keep-list lines |
| `SourceStreamChunk` | Required in server->client stream mode |
| All other frames | Must be empty |

## Path and metadata conventions
- File paths on wire are relative to server destination root.
- Absolute paths and parent traversal (`..`) are rejected.
- Path separators are normalized logically as `/`.
- `file_hash` is the lowercase hex BLAKE3 digest of full file bytes.
- `uid/gid/mode/xattrs` are platform-sensitive. Unix servers apply POSIX metadata when enabled; non-supporting platforms may ignore parts of metadata.

## Request/response semantics
Unless noted otherwise:
- Requests expect exactly one response frame.
- Response payload must be empty.
- Server errors may be returned as `Frame::Error { message }`.

### Init and resume
`InitFileRequest` and `InitBatchRequest` establish upload intent and resume position.

Core fields:
- `relative_path`, `size`, `mode`, `mtime_sec`
- `xattr_sig` (optional xattr signature)
- `update_only` (skip if destination newer)
- `file_hash`
- `chunk_size`, `total_chunks`
- `resume`

Responses (`InitFileResponse`, `InitBatchResult`) include:
- `action`: `Skip` or `Upload`
- `next_chunk`: resume cursor
- `metadata_sync_required`: true when content is already complete but metadata differs
- `message`

### Large/resumed chunked upload
`UploadBatchRequest` carries chunked file payload.

Request fields:
- file identity and metadata (`relative_path`, `size`, `mode`, `mtime_sec`, `uid`, `gid`, `xattrs`, `file_hash`)
- chunk progress fields (`total_chunks`, `start_chunk`, `chunk_size`, `sent_chunks`, `finalize`)

Payload:
- Chunk-batch payload format (see payload formats section).

`UploadBatchResponse`:
- `accepted`
- `message`
- `next_chunk`
- `completed`
- `bytes_written`

Behavior:
- Upload requires prior successful init for the file.
- Out-of-order batches are rejected.
- Duplicate/replayed already-recorded ranges are acknowledged without rewrite.
- `finalize=true` requires full coverage through `total_chunks`.

### Small-file batch upload
`UploadSmallBatchRequest` uploads many small files in one request.

Header:
- `files: Vec<UploadSmallFileMeta>`

Per-file meta includes:
- `relative_path`, `size`, `mode`, `mtime_sec`, `uid`, `gid`, `xattrs`, `file_hash`, `total_chunks`
- compression info: `compressed`, `raw_len`, `data_len`

Payload:
- Concatenation of each file payload in the same order as `files`, lengths from `data_len`.

Response:
- `UploadSmallBatchResponse { results: Vec<UploadSmallFileResult> }`
- each result: `accepted`, `skipped`, `message`, `bytes_written`

### Cold/direct batch upload
`UploadColdBatchRequest` uploads files directly to final destination path.

Header:
- `allow_skip: bool`
- `files: Vec<UploadColdFileMeta>` (same shape as small meta)

Payload:
- Concatenation of file payloads in request order (`data_len`).

Response:
- `UploadColdBatchResponse { results: Vec<UploadColdFileResult> }`

`allow_skip=true` permits server-side skip if a complete file hash/size match already exists.

### Symlink sync
`SyncSymlinkBatchRequest` has no payload and carries:
- `entries: Vec<SymlinkMeta>`

`SymlinkMeta`:
- `relative_path`, `target`, `mode`, `mtime_sec`, `uid`, `gid`, `xattrs`

Response:
- `SyncSymlinkBatchResponse { results: Vec<SyncSymlinkResult> }`

### File metadata sync (without content upload)
`SyncFileMetadataBatchRequest` has no payload and carries:
- `entries: Vec<FileMetadataSyncEntry>`

`FileMetadataSyncEntry`:
- `relative_path`, `size`, `file_hash`, `mode`, `mtime_sec`, `uid`, `gid`, `xattrs`

Response:
- `SyncFileMetadataBatchResponse { results: Vec<SyncFileMetadataResult> }`

Server updates metadata only when complete file state still matches `file_hash` and `size`.

### Delete-plan protocol
Delete is a staged transaction:

1. `DeletePlanRequest { stage=Begin, dry_run, include, exclude }`
2. Zero or more `DeletePlanRequest { stage=AddKeepChunk, ... }` with keep-list payload chunks
3. `DeletePlanRequest { stage=Apply, ... }`

Response each step:
- `DeletePlanResponse { accepted, message, deleted, keep_paths }`

Keep-list payload format:
- UTF-8 text
- one relative path per line
- blank lines ignored
- paths sanitized by server before applying

### Source streaming (pull)
`SourceStreamRequest` starts server-to-client source streaming on that stream.

Request fields:
- `chunk_size`
- `metadata_only`
- `preserve_metadata`
- `preserve_xattrs`
- `include`, `exclude`

Request payload must be empty.

`preserve_metadata` in `SourceStreamRequest` is currently advisory; file/symlink metadata fields are still populated in stream frames.

Server then emits:
1. `SourceStreamFileStart` for each entry.
2. For regular files (unless `metadata_only=true`): repeated `SourceStreamChunk` with raw data payload.
3. `SourceStreamFileEnd`.
4. Final `SourceStreamDone`.

Frame meanings:
- `SourceStreamFileStart`: entry metadata and kind (`File` or `Symlink`).
- `SourceStreamChunk`: header `chunk_len`, payload contains exactly `chunk_len` bytes.
- `SourceStreamFileEnd`: closes current file entry.
- `SourceStreamDone`: stream summary (`files`, `bytes`).

On source-stream failure, server may emit `Frame::Error`.

## Payload binary formats
### Chunk-batch payload (`UploadBatchRequest`)
Payload is `sent_chunks` concatenated entries:

| Field | Size | Description |
|---|---:|---|
| `raw_len` | 4 | original (decompressed) chunk length |
| `encoded_len` | 4 | encoded chunk byte length |
| `flags` | 1 | bit0: compressed (`1`) |
| `data` | `encoded_len` | chunk bytes |

If compressed flag is set, data is zstd-compressed and must decode to `raw_len`.

### Small/cold batch payload
For both `UploadSmallBatchRequest` and `UploadColdBatchRequest`:
- payload is raw concatenation of per-file `data` blobs.
- lengths come from per-file `data_len`.
- each blob may be compressed or raw per `compressed` and `raw_len`.

### Delete-plan keep chunk payload
- UTF-8 text lines.
- each line is one path entry.

## Authentication and authorization
Transport security:
- QUIC + TLS 1.3.
- Server cert is always validated by client against configured trust root.

mTLS and authz (optional, server-configured):
- Server may require client certificates.
- Server may map peer cert fingerprint to policy entry.
- Policy may include allowed destination prefixes.
- Mutating operations are denied if `relative_path` is outside allowed prefixes.

If authz rejects an operation, server returns an error frame message (or closes stream on hard errors).

## Error handling
- Unexpected frame type for a context returns `Frame::Error`.
- Payload where not allowed is an error.
- Frame decode/prefix violations are stream errors.
- Clients should treat `Frame::Error` as operation failure.

## Interoperability checklist
To interoperate with current `sparsync`:
1. Implement QUIC transport with ALPN `sparsync/1`.
2. Implement frame prefix parsing exactly (including big-endian fields).
3. Implement rkyv frame serialization/validation compatible with current Rust schema.
4. Send and validate `HelloRequest/HelloResponse`.
5. Support staged upload (`Init*`, `Upload*`, optional metadata/symlink sync).
6. Support delete plan stages and keep-list payload chunking.
7. Support source-stream pull sequence frames.
8. Enforce path sanitization and relative-path semantics.

## Known limitations of protocol v1
- Strict binary version matching is required in handshake.
- Endianness must match across peers.
- The schema uses `usize` fields in multiple messages; cross-architecture behavior is not specified for non-Rust implementations.
- Filter pattern language is implementation-defined by current path-filter logic.
- No on-wire negotiation exists for max frame size; it is configured out-of-band.
- No destination-root selection frame exists; destination root is server-side configuration.
