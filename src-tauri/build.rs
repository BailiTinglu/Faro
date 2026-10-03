fn main() {
    // Unit-test binaries link tauri (and with it comctl32's TaskDialogIndirect)
    // but get no application manifest, so Windows refuses to start them
    // (STATUS_ENTRYPOINT_NOT_FOUND). Ask the linker for a side-by-side
    // `.exe.manifest` naming Common Controls v6: the test binary picks it up,
    // while the app binary's embedded tauri-build manifest takes precedence.
    if std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!(
            "cargo:rustc-link-arg=/MANIFESTDEPENDENCY:type='win32' \
             name='Microsoft.Windows.Common-Controls' version='6.0.0.0' \
             processorArchitecture='*' publicKeyToken='6595b64144ccf1df' language='*'"
        );
    }
    tauri_build::build()
}
