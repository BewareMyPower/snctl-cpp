# snctl-cpp

The CLI tool to manage clusters on StreamNative Cloud.

## Build

You must have a C++ compiler that supports C++17.

```bash
git submodule update --init --recursive
cmake -B build
cmake --build build
```

Then you can find the executable in `build/snctl`.
