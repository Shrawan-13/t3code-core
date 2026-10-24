#!/usr/bin/env python3
"""Remove the Termux rpath from a Go c-shared Android library.

The Termux clang driver injects a DT_RUNPATH pointing at $PREFIX/lib. That path
does not exist on a normal Android device, so drop it by emptying the string in
.dynstr (the dynamic tag then resolves to an empty search path).
"""
import sys

TERMUX_RUNPATH = b"/data/data/com.termux/files/usr/bin/../../usr/lib\x00"

def main(path: str) -> int:
    data = bytearray(open(path, "rb").read())
    index = data.find(TERMUX_RUNPATH)
    if index < 0:
        print(f"{path}: runpath not found (already clean?)")
        return 0
    data[index] = 0
    open(path, "wb").write(data)
    print(f"{path}: cleared runpath at {index:#x}")
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: patch-android-so.py <libt3core.so>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
