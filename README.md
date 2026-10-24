# t3code-core

Go core for the T3 Code Android client. It implements the T3 orchestration
protocol (Effect RPC over WebSocket plus the HTTP API), keeps live client-side
projections, and exposes a pull-based JNI facade for Kotlin.

## Layout

```
internal/rpc/  Effect RPC v4 transport
t3/            typed SDK, projections, commands, terminal, files, git, PRs
mobile/        JNI entry points (com.t3codes.core.Native)
cmd/t3ctl/     terminal client for live protocol probing
```

## Android libraries

CI cross-compiles `libt3core.so` for `arm64-v8a`, `armeabi-v7a`, and `x86_64`
with the Android NDK. Tagged builds publish all three as release assets:

- `libt3core-arm64-v8a.so`
- `libt3core-armeabi-v7a.so`
- `libt3core-x86_64.so`

The Android app downloads the ABI it needs from a release instead of committing
binaries to its own repository.

## Tests

```bash
go test ./...
```

Live tests are skipped unless `T3_TEST_TOKEN` (or `.secrets/token.txt`) is
present. Write-path tests require `T3_TEST_WRITE=1`.
