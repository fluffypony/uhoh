pub fn sandbox_supported() -> bool {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        true
    }
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    {
        false
    }
}
