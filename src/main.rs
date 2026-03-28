#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;
mod db;
mod highlighter;
mod history;
mod profiles;
mod recent;
mod schema;
mod snippets;
mod utils;

fn main() -> iced::Result {
    let icon = load_icon();

    iced::application(
        "PostgreSQL Query Launcher",
        app::App::update,
        app::App::view,
    )
    .window(iced::window::Settings {
        size: iced::Size::new(1200.0, 800.0),
        icon,
        ..Default::default()
    })
    .theme(app::App::theme)
    .run()
}

fn load_icon() -> Option<iced::window::Icon> {
    let bytes = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory(bytes).ok()?.to_rgba8();
    let (w, h) = img.dimensions();
    iced::window::icon::from_rgba(img.into_raw(), w, h).ok()
}
