fn main() {
    // Embed the ICO into the Windows PE resources (exe icon / taskbar icon)
    #[cfg(windows)]
    embed_icon();
}

#[cfg(windows)]
fn embed_icon() {
    let png_path = "assets/icon.png";
    let ico_path = "assets/icon.ico";

    // Re-run if the source PNG changes
    println!("cargo:rerun-if-changed={png_path}");

    let img = image::open(png_path).expect("assets/icon.png not found");

    let mut icon_dir = ico::IconDir::new(ico::ResourceType::Icon);
    for size in [16u32, 32, 48, 256] {
        let resized = img.resize(size, size, image::imageops::FilterType::Lanczos3);
        let rgba = resized.to_rgba8();
        let (w, h) = rgba.dimensions();
        let ico_img = ico::IconImage::from_rgba_data(w, h, rgba.into_raw());
        icon_dir.add_entry(ico::IconDirEntry::encode(&ico_img).expect("ICO encode failed"));
    }

    let f = std::fs::File::create(ico_path).expect("could not create assets/icon.ico");
    icon_dir.write(f).expect("could not write assets/icon.ico");

    winresource::WindowsResource::new()
        .set_icon(ico_path)
        .compile()
        .expect("winresource compile failed");
}
