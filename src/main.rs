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
    iced::application(
        "PostgreSQL Query Launcher",
        app::App::update,
        app::App::view,
    )
    .window(iced::window::Settings {
        size: iced::Size::new(1200.0, 800.0),
        ..Default::default()
    })
    .theme(app::App::theme)
    .run()
}
