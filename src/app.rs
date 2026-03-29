use chrono::{Datelike, NaiveDate, Timelike};
use iced::widget::{
    button, checkbox, column, container, horizontal_rule, horizontal_space,
    pick_list, row, scrollable, text, text_editor, text_input, Column, Row,
};
use iced::{Alignment, Color, Element, Length, Task, Theme};
use std::sync::Arc;
use zeroize::Zeroize;

use crate::highlighter::{SqlHighlighter, SqlSettings};

use crate::db::{ConnConfig, PlanNode, QueryResult, PgHandle, explain_plan_sync, fetch_schema_sync, query_sync};
use crate::history::{HistoryEntry, HistoryStore};
use crate::profiles::{ConnectionProfile, ProfileStore, load_password, save_password};
use crate::recent::{RecentConnection, RecentStore};
use crate::schema::SchemaCache;
use crate::snippets::{Snippet, SnippetStore};

const SLOW_QUERY_MS: u128 = 1000;

// ---------------------------------------------------------------------------
// Palette — two concrete themes, selected at view time
// ---------------------------------------------------------------------------
#[derive(Clone, Copy)]
struct Pal {
    sidebar:    Color,
    action:     Color,
    active:     Color,
    bg:         Color,
    panel:      Color,
    border:     Color,
    text:       Color,
    muted:      Color,
    error:      Color,
    success:    Color,
    warning:    Color,
    // nav inactive fg
    nav_inactive_fg: Color,
    // tab row bg
    tab_bar_bg: Color,
    // status bar bg
    status_bg:  Color,
    // row alternating
    row_alt:    Color,
    // accent (interactive highlight)
    accent:      Color,
    accent_text: Color,
}

impl Pal {
    fn dark() -> Self {
        let g = |v: f32| Color { r: v, g: v, b: v, a: 1.0 };
        Self {
            bg:              g(0.039), // #0A0A0A
            sidebar:         g(0.067), // #111111
            panel:           g(0.098), // #191919
            border:          g(0.173), // #2C2C2C
            action:          g(0.122), // #1F1F1F
            active:          g(0.165), // #2A2A2A
            text:            g(0.922), // #EBEBEB
            muted:           g(0.400), // #666666
            nav_inactive_fg: g(0.333), // #555555
            tab_bar_bg:      g(0.027), // #070707
            status_bg:       g(0.039), // #0A0A0A
            row_alt:         g(0.071), // #121212
            // accent: near-white for active/selected states (inverted contrast)
            accent:          g(0.878), // #E0E0E0
            accent_text:     g(0.039), // dark text on light accent
            // functional — very desaturated, never overpowering
            error:   Color { r: 0.741, g: 0.435, b: 0.435, a: 1.0 }, // muted rose
            success: Color { r: 0.435, g: 0.647, b: 0.435, a: 1.0 }, // muted sage
            warning: Color { r: 0.663, g: 0.580, b: 0.376, a: 1.0 }, // muted sand
        }
    }

    fn light() -> Self {
        let g = |v: f32| Color { r: v, g: v, b: v, a: 1.0 };
        Self {
            bg:              g(0.976), // #F9F9F9
            sidebar:         g(0.949), // #F2F2F2
            panel:           g(1.000), // #FFFFFF
            border:          g(0.878), // #E0E0E0
            action:          g(0.937), // #EFEFEF
            active:          g(0.894), // #E4E4E4
            text:            g(0.086), // #161616
            muted:           g(0.565), // #909090
            nav_inactive_fg: g(0.541), // #8A8A8A
            tab_bar_bg:      g(0.949), // #F2F2F2
            status_bg:       g(0.949),
            row_alt:         g(0.965), // #F6F6F6
            // accent: near-black for active/selected states
            accent:          g(0.102), // #1A1A1A
            accent_text:     g(1.000), // white text on dark accent
            // functional — desaturated for light bg
            error:   Color { r: 0.565, g: 0.275, b: 0.275, a: 1.0 }, // muted rose
            success: Color { r: 0.259, g: 0.439, b: 0.259, a: 1.0 }, // muted sage
            warning: Color { r: 0.471, g: 0.400, b: 0.196, a: 1.0 }, // muted sand
        }
    }

    fn for_dark(dark: bool) -> Self {
        if dark { Self::dark() } else { Self::light() }
    }
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct SavedResult {
    name:    String,
    columns: Vec<String>,
    rows:    Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
struct PinboardEntry {
    label: String,
    value: String,
}

#[derive(Debug, Clone)]
struct ColStats {
    col_name:   String,
    min:        String,
    max:        String,
    avg:        Option<f64>,
    null_count: usize,
    total:      usize,
}

impl ColStats {
    fn compute(col_name: &str, rows: &[Vec<String>], col_idx: usize) -> Self {
        let mut null_count = 0usize;
        let mut numeric_vals: Vec<f64> = Vec::new();
        let mut min_str = String::new();
        let mut max_str = String::new();
        let mut first = true;
        for row in rows {
            let val = row.get(col_idx).map(String::as_str).unwrap_or("NULL");
            if val == "NULL" { null_count += 1; continue; }
            if first { min_str = val.to_owned(); max_str = val.to_owned(); first = false; }
            else {
                if val < min_str.as_str() { min_str = val.to_owned(); }
                if val > max_str.as_str() { max_str = val.to_owned(); }
            }
            if let Ok(n) = val.parse::<f64>() { numeric_vals.push(n); }
        }
        let avg = if numeric_vals.is_empty() { None }
                  else { Some(numeric_vals.iter().sum::<f64>() / numeric_vals.len() as f64) };
        ColStats {
            col_name: col_name.to_owned(),
            min: if min_str.is_empty() { "-".to_owned() } else { min_str },
            max: if max_str.is_empty() { "-".to_owned() } else { max_str },
            avg, null_count, total: rows.len(),
        }
    }
}

#[derive(Debug)]
struct QueryTab {
    label:        String,
    content:      text_editor::Content,
    last_result:  Option<QueryResult>,
    result_error: Option<String>,
    sort_col:     Option<usize>,
    sort_asc:     bool,
    sorted_rows:  Option<Vec<Vec<String>>>,
    pivot_view:   bool,
    col_stats:    Option<ColStats>,
    plan_result:  Option<Vec<PlanNode>>,
    show_plan:    bool,
}

impl QueryTab {
    fn new(n: usize) -> Self {
        Self {
            label:        format!("Query {n}"),
            content:      text_editor::Content::new(),
            last_result:  None,
            result_error: None,
            sort_col:     None,
            sort_asc:     true,
            sorted_rows:  None,
            pivot_view:   false,
            col_stats:    None,
            plan_result:  None,
            show_plan:    false,
        }
    }

    fn sql(&self) -> String { self.content.text() }

    fn update_label(&mut self) {
        let base = self.label.split('(').next().unwrap_or(&self.label).trim().to_owned();
        if let Some(r) = &self.last_result {
            let slow = if r.duration_ms >= SLOW_QUERY_MS { " (!)" } else { "" };
            self.label = format!("{base} ({} rows, {}ms{slow})", r.row_count, r.duration_ms);
        } else {
            self.label = base;
        }
    }

    fn apply_sort(&mut self) {
        if let Some(result) = &self.last_result {
            let mut rows = result.rows.clone();
            if let Some(col) = self.sort_col {
                rows.sort_by(|a, b| {
                    let av = a.get(col).map(String::as_str).unwrap_or("");
                    let bv = b.get(col).map(String::as_str).unwrap_or("");
                    match (av.parse::<f64>(), bv.parse::<f64>()) {
                        (Ok(an), Ok(bn)) => {
                            let ord = an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal);
                            if self.sort_asc { ord } else { ord.reverse() }
                        }
                        _ => if self.sort_asc { av.cmp(bv) } else { bv.cmp(av) },
                    }
                });
            }
            self.sorted_rows = Some(rows);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum View {
    Connection, Schema, QueryEditor, History,
    Snippets, Pinboard, Diff, Automation, Shortcuts,
}

#[derive(Debug, Clone)]
struct ToolbarQuery { label: String, sql: String }

#[derive(Debug, Clone, PartialEq)]
enum Schedule {
    EveryNSeconds(u64),
    Hourly  { minute: u32 },
    Daily   { hour: u32, minute: u32 },
    Weekly  { weekday: u32, hour: u32, minute: u32 },
    Monthly { day: u32, hour: u32, minute: u32 },
}

impl Schedule {
    fn label(&self) -> String {
        match self {
            Schedule::EveryNSeconds(s) => {
                if *s < 60 { format!("every {s}s") }
                else if *s < 3600 { format!("every {}m", s / 60) }
                else { format!("every {}h", s / 3600) }
            }
            Schedule::Hourly  { minute }           => format!("hourly :{minute:02}"),
            Schedule::Daily   { hour, minute }      => format!("daily {hour:02}:{minute:02}"),
            Schedule::Weekly  { weekday, hour, minute } => {
                let day = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].get(*weekday as usize).unwrap_or(&"?");
                format!("weekly {day} {hour:02}:{minute:02}")
            }
            Schedule::Monthly { day, hour, minute } => format!("monthly day{day} {hour:02}:{minute:02}"),
        }
    }

    fn is_due(&self, last_run: Option<chrono::DateTime<chrono::Local>>) -> bool {
        let now = chrono::Local::now();
        match self {
            Schedule::EveryNSeconds(s) => match last_run {
                None => true, Some(t) => (now - t).num_seconds() >= *s as i64,
            },
            Schedule::Hourly { minute } => {
                if now.minute() != *minute || now.second() > 10 { return false; }
                match last_run { None => true, Some(t) => (now - t).num_minutes() >= 59 }
            }
            Schedule::Daily { hour, minute } => {
                if now.hour() != *hour || now.minute() != *minute || now.second() > 10 { return false; }
                match last_run { None => true, Some(t) => (now - t).num_hours() >= 23 }
            }
            Schedule::Weekly { weekday, hour, minute } => {
                if now.weekday().num_days_from_monday() != *weekday || now.hour() != *hour || now.minute() != *minute || now.second() > 10 { return false; }
                match last_run { None => true, Some(t) => (now - t).num_days() >= 6 }
            }
            Schedule::Monthly { day, hour, minute } => {
                if now.day() != *day || now.hour() != *hour || now.minute() != *minute || now.second() > 10 { return false; }
                match last_run { None => true, Some(t) => (now - t).num_days() >= 27 }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct AutoJob {
    label:    String,
    sql:      String,
    schedule: Schedule,
    last_run: Option<chrono::DateTime<chrono::Local>>,
    enabled:  bool,
}

#[derive(Debug, Clone, Default)]
struct ScheduleBuilder {
    kind:         usize,
    secs:         String,
    hour:         String,
    minute:       String,
    weekday:      usize,
    day_of_month: String,
}

impl ScheduleBuilder {
    fn new() -> Self {
        Self { kind: 0, secs: "60".into(), hour: "9".into(),
               minute: "0".into(), weekday: 0, day_of_month: "1".into() }
    }
    fn build(&self) -> Option<Schedule> {
        let hour: u32 = self.hour.parse().ok().filter(|h: &u32| *h < 24)?;
        let min:  u32 = self.minute.parse().ok().filter(|m: &u32| *m < 60)?;
        let dom:  u32 = self.day_of_month.parse().ok().filter(|d: &u32| (1..=28).contains(d))?;
        let secs: u64 = self.secs.parse().ok().filter(|s: &u64| *s >= 1)?;
        Some(match self.kind {
            0 => Schedule::EveryNSeconds(secs),
            1 => Schedule::Hourly { minute: min },
            2 => Schedule::Daily  { hour, minute: min },
            3 => Schedule::Weekly { weekday: self.weekday as u32, hour, minute: min },
            4 => Schedule::Monthly { day: dom, hour, minute: min },
            _ => return None,
        })
    }
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Message {
    SetView(View),
    ToggleTheme,
    DismissNotice,
    // connection
    ConnHostChanged(String),
    ConnPortChanged(String),
    ConnDbnameChanged(String),
    ConnUserChanged(String),
    ConnPasswordChanged(String),
    ConnUseTlsToggled(bool),
    ReadOnlyToggled(bool),
    ConnectPressed,
    DisconnectPressed,
    ConnectionResult(Result<(), String>),
    LoadRecent(usize),
    // profiles
    ProfileNameChanged(String),
    SavePwToggled(bool),
    SaveProfilePressed,
    SelectProfile(usize),
    DeleteProfilePressed,
    // query editor
    TabSelected(usize),
    TabAdd,
    TabClose,
    QueryEdited(usize, text_editor::Action),
    ExecutePressed,
    ExplainPressed,
    FormatPressed,
    CancelPressed,
    ClearPressed,
    PivotToggled,
    SnapshotPressed,
    SnapshotNameChanged(String),
    BatchModeToggled(bool),
    ExportCsv,
    ExportJson,
    ExportXlsx,
    SortColumn(usize),
    ShowColStats(usize, String),
    CloseColStats,
    CopyCell(String),
    PinCell(String, String),
    ConfirmRun,
    CancelConfirm,
    QueryResult(Result<QueryResult, String>),
    // toolbar
    ToolbarLabelChanged(String),
    PinToToolbar,
    RunToolbarQuery(usize),
    RemoveToolbarQuery(usize),
    MoveToolbarLeft(usize),
    MoveToolbarRight(usize),
    // transaction
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    // history
    HistorySearchChanged(String),
    FilterFromChanged(String),
    FilterToChanged(String),
    HistoryFilter,
    HistoryShowAll,
    LoadHistoryQuery(String),
    // snippets
    SnippetNameChanged(String),
    SaveSnippet,
    InsertSnippet(usize),
    DeleteSnippet(usize),
    // schema
    LoadSchema,
    SchemaResult(Result<Vec<crate::db::SchemaTable>, String>),
    SchemaSearchChanged(String),
    ToggleSchemaTable(String),
    // database switcher
    DatabasesLoaded(Result<Vec<String>, String>),
    SwitchDatabase(String),
    // pinboard
    PinLabelChanged(String),
    AddPin,
    DeletePin(usize),
    PinValueChanged(usize, String),
    PastePinToParam(usize),
    ParamValueChanged(usize, String),
    // diff
    DiffLeftSelected(usize),
    DiffRightSelected(usize),
    ClearSnapshots,
    // automation
    AutoLabelChanged(String),
    AutoSqlChanged(String),
    SchedKindChanged(usize),
    SchedSecsChanged(String),
    SchedHourChanged(String),
    SchedMinuteChanged(String),
    SchedWeekdayChanged(usize),
    SchedDomChanged(String),
    AddAutoJob,
    RemoveAutoJob(usize),
    ToggleAutoJob(usize, bool),
    FireAutoJobNow(usize),
    Tick,
    ExplainPlanResult(Result<Vec<PlanNode>, String>),
    ShowPlanView(bool),
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------
pub struct App {
    conn_host:     String,
    conn_port:     String,
    conn_dbname:   String,
    conn_user:     String,
    conn_password: String,
    conn_use_tls:  bool,
    conn_status:   String,
    connected:     bool,
    active_config: Option<Arc<ConnConfig>>,
    pg_handle:     Option<PgHandle>,
    read_only:     bool,
    batch_mode:    bool,
    dark_theme:    bool,
    pending_sql:         Option<String>,
    show_confirm_dialog: bool,
    in_transaction: bool,
    tx_error:       String,
    query_running:  bool,
    query_tabs:    Vec<QueryTab>,
    active_tab:    usize,
    max_display_rows:  usize,
    slow_threshold_ms: usize,
    param_names:   Vec<String>,
    param_values:  Vec<String>,
    batch_results: Vec<(String, Result<QueryResult, String>)>,
    history_store:   HistoryStore,
    history_view:    Vec<HistoryEntry>,
    history_search:  String,
    filter_from_str: String,
    filter_to_str:   String,
    history_error:   String,
    profile_store:        ProfileStore,
    profile_name_input:   String,
    profile_save_pw:      bool,
    selected_profile_idx: Option<usize>,
    recent_store:  RecentStore,
    snippet_store:       SnippetStore,
    snippet_name_input:  String,
    schema_cache:     SchemaCache,
    schema_loading:   bool,
    schema_search:    String,
    schema_expanded:  std::collections::HashSet<String>,
    available_dbs:    Vec<String>,
    dbs_loading:      bool,
    saved_results:     Vec<SavedResult>,
    snapshot_name_buf: String,
    diff_left_idx:     Option<usize>,
    diff_right_idx:    Option<usize>,
    pinboard:      Vec<PinboardEntry>,
    pin_label_buf: String,
    toolbar_queries:   Vec<ToolbarQuery>,
    toolbar_label_buf: String,
    auto_jobs:          Vec<AutoJob>,
    auto_job_label_buf: String,
    auto_job_sql_buf:   String,
    schedule_builder:   ScheduleBuilder,
    notice:       Option<String>,
    current_view: View,
}

impl Default for App {
    fn default() -> Self {
        let data_dir = crate::utils::app_data_dir();
        let history_path = data_dir.join("query_history.db");
        let history_store = HistoryStore::open(history_path.to_str().unwrap_or("query_history.db"))
            .unwrap_or_else(|e| {
                eprintln!("warning: could not open history db: {e}");
                HistoryStore::open(":memory:").expect("in-memory history db failed")
            });
        let profile_store = ProfileStore::load(&data_dir.join("connection_profiles.json"));
        let snippet_store = SnippetStore::load(&data_dir.join("snippets.json"));
        let recent_store  = RecentStore::load(&data_dir.join("recent_connections.json"));
        Self {
            conn_host: "localhost".into(), conn_port: "5432".into(),
            conn_dbname: String::new(), conn_user: String::new(), conn_password: String::new(),
            conn_use_tls: true, conn_status: "Not connected".into(),
            connected: false, active_config: None, pg_handle: None,
            read_only: false, batch_mode: false, dark_theme: true,
            pending_sql: None, show_confirm_dialog: false,
            in_transaction: false, tx_error: String::new(), query_running: false,
            query_tabs: vec![QueryTab::new(1)], active_tab: 0,
            max_display_rows: 2000, slow_threshold_ms: SLOW_QUERY_MS as usize,
            param_names: Vec::new(), param_values: Vec::new(), batch_results: Vec::new(),
            history_store, history_view: Vec::new(),
            history_search: String::new(), filter_from_str: String::new(),
            filter_to_str: String::new(), history_error: String::new(),
            profile_store, profile_name_input: String::new(), profile_save_pw: false,
            selected_profile_idx: None, recent_store,
            snippet_store, snippet_name_input: String::new(),
            schema_cache: SchemaCache::new(), schema_loading: false,
            schema_search: String::new(), schema_expanded: std::collections::HashSet::new(),
            available_dbs: Vec::new(), dbs_loading: false,
            saved_results: Vec::new(), snapshot_name_buf: String::new(),
            diff_left_idx: None, diff_right_idx: None,
            pinboard: Vec::new(), pin_label_buf: String::new(),
            toolbar_queries: Vec::new(), toolbar_label_buf: String::new(),
            auto_jobs: Vec::new(), auto_job_label_buf: String::new(),
            auto_job_sql_buf: String::new(), schedule_builder: ScheduleBuilder::new(),
            notice: None, current_view: View::Connection,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn is_destructive(sql: &str) -> bool {
    let u = sql.trim().to_uppercase();
    ["DROP","DELETE","TRUNCATE","ALTER","UPDATE"]
        .iter().any(|kw| u.starts_with(kw) || u.contains(&format!(" {kw} ")))
}

fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect()
}

fn build_csv(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut out = csv_row(columns);
    for row in rows { out.push_str(&csv_row(row)); }
    out
}
fn csv_row(fields: &[String]) -> String {
    let cells: Vec<String> = fields.iter().map(|f| {
        if f.contains(',') || f.contains('"') || f.contains('\n') {
            format!("\"{}\"", f.replace('"', "\"\""))
        } else { f.clone() }
    }).collect();
    format!("{}\n", cells.join(","))
}
fn build_json(columns: &[String], rows: &[Vec<String>]) -> String {
    let objects: Vec<serde_json::Value> = rows.iter().map(|row| {
        let map: serde_json::Map<String,serde_json::Value> = columns.iter().zip(row.iter())
            .map(|(c,v)| (c.clone(), serde_json::Value::String(v.clone()))).collect();
        serde_json::Value::Object(map)
    }).collect();
    serde_json::to_string_pretty(&objects).unwrap_or_default()
}
fn build_xlsx(columns: &[String], rows: &[Vec<String>]) -> Result<Vec<u8>, String> {
    use rust_xlsxwriter::{Format, Workbook};
    let mut wb = Workbook::new();
    let sheet  = wb.add_worksheet();
    let hdr    = Format::new().set_bold();
    for (ci, col) in columns.iter().enumerate() {
        sheet.write_with_format(0, ci as u16, col.as_str(), &hdr).map_err(|e| e.to_string())?;
    }
    for (ri, row) in rows.iter().enumerate() {
        for (ci, cell) in row.iter().enumerate() {
            if let Ok(n) = cell.parse::<f64>() {
                sheet.write(ri as u32 + 1, ci as u16, n).map_err(|e| e.to_string())?;
            } else {
                sheet.write(ri as u32 + 1, ci as u16, cell.as_str()).map_err(|e| e.to_string())?;
            }
        }
    }
    wb.save_to_buffer().map_err(|e| e.to_string())
}

impl App {
    fn substitute_params(&self, sql: &str) -> String {
        let mut r = sql.to_owned();
        for (n, v) in self.param_names.iter().zip(self.param_values.iter()) {
            r = r.replace(&format!("${n}"), v);
        }
        r
    }

    fn refresh_params(&mut self) {
        let sql = self.query_tabs[self.active_tab].sql();
        let mut names: Vec<String> = Vec::new();
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '$' {
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') { j += 1; }
                if j > start {
                    let name: String = chars[start..j].iter().collect();
                    if !names.contains(&name) { names.push(name); }
                }
                i = j;
            } else { i += 1; }
        }
        let old_names  = std::mem::replace(&mut self.param_names, names.clone());
        let old_values = std::mem::replace(&mut self.param_values, vec![String::new(); names.len()]);
        for (ni, name) in names.iter().enumerate() {
            if let Some(oi) = old_names.iter().position(|n| n == name) {
                self.param_values[ni] = old_values[oi].clone();
            } else if let Some(pin) = self.pinboard.iter().find(|p| p.label == *name) {
                self.param_values[ni] = pin.value.clone();
            }
        }
    }

    fn fire_query_sync(&mut self, sql: String) -> Task<Message> {
        if sql.trim().is_empty() { return Task::none(); }
        let Some(cfg) = self.active_config.clone() else { return Task::none(); };
        self.query_running = true;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || {
                    PgHandle::connect_sync(&cfg).and_then(|h| query_sync(&h, &sql))
                }).await.unwrap_or_else(|e| Err(e.to_string()))
            },
            Message::QueryResult,
        )
    }

    fn try_fire_query(&mut self, sql: String) -> Task<Message> {
        let trimmed = sql.trim().to_owned();
        if trimmed.is_empty() { return Task::none(); }
        if self.batch_mode { return self.fire_batch(trimmed); }
        if self.read_only {
            let u = trimmed.to_uppercase();
            if !u.starts_with("SELECT") && !u.starts_with("EXPLAIN")
                && !u.starts_with("SHOW") && !u.starts_with("WITH")
            {
                self.notice = Some("Read-only mode: only SELECT / EXPLAIN / SHOW / WITH allowed.".into());
                return Task::none();
            }
        }
        if is_destructive(&trimmed) {
            self.pending_sql = Some(trimmed);
            self.show_confirm_dialog = true;
            return Task::none();
        }
        self.fire_query_sync(trimmed)
    }

    fn fire_batch(&mut self, sql: String) -> Task<Message> {
        let Some(cfg) = self.active_config.clone() else { return Task::none(); };
        let stmts = split_statements(&sql);
        if stmts.is_empty() { return Task::none(); }
        self.query_running = true;
        self.batch_results.clear();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Vec<(String, Result<QueryResult, String>)> {
                    stmts.into_iter().map(|stmt| {
                        let res = PgHandle::connect_sync(&cfg).and_then(|h| query_sync(&h, &stmt));
                        (stmt, res)
                    }).collect()
                }).await.unwrap_or_default()
            },
            |results| Message::BatchResults(results),
        )
    }

    fn fire_explain_plan(&mut self, sql: String) -> Task<Message> {
        if sql.trim().is_empty() { return Task::none(); }
        let Some(cfg) = self.active_config.clone() else { return Task::none(); };
        self.query_running = true;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || {
                    PgHandle::connect_sync(&cfg).and_then(|h| explain_plan_sync(&h, &sql))
                }).await.unwrap_or_else(|e| Err(e.to_string()))
            },
            Message::ExplainPlanResult,
        )
    }

    fn launch_schema_fetch(&mut self) -> Task<Message> {
        let Some(cfg) = self.active_config.clone() else { return Task::none(); };
        self.schema_loading = true;
        self.schema_cache.clear();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || {
                    PgHandle::connect_sync(&cfg).and_then(|h| fetch_schema_sync(&h))
                }).await.unwrap_or_else(|e| Err(e.to_string()))
            },
            Message::SchemaResult,
        )
    }

    fn launch_db_fetch(&mut self) -> Task<Message> {
        let Some(cfg) = self.active_config.clone() else { return Task::none(); };
        self.dbs_loading = true;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || {
                    let h = PgHandle::connect_sync(&cfg)?;
                    let result = query_sync(&h, "SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn = true ORDER BY datname")?;
                    Ok(result.rows.into_iter().filter_map(|r| r.into_iter().next()).collect::<Vec<String>>())
                }).await.unwrap_or_else(|e| Err(e.to_string()))
            },
            Message::DatabasesLoaded,
        )
    }

    pub fn theme(&self) -> Theme {
        if self.dark_theme { Theme::Dark } else { Theme::Light }
    }
}

// add BatchResults variant we reference above
impl Message {
    #[allow(non_snake_case)]
    fn BatchResults(r: Vec<(String, Result<QueryResult, String>)>) -> Self {
        Message::QueryResult(Err(format!("__BATCH_INTERNAL__:{}", r.len())))
    }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------
impl App {
    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::SetView(v) => {
                if v == View::History && self.history_view.is_empty() {
                    if let Ok(e) = self.history_store.fetch_all() { self.history_view = e; }
                }
                self.current_view = v;
                Task::none()
            }
            Message::ToggleTheme    => { self.dark_theme = !self.dark_theme; Task::none() }
            Message::DismissNotice  => { self.notice = None; Task::none() }

            Message::ConnHostChanged(s)     => { self.conn_host = s;     Task::none() }
            Message::ConnPortChanged(s)     => { self.conn_port = s;     Task::none() }
            Message::ConnDbnameChanged(s)   => { self.conn_dbname = s;   Task::none() }
            Message::ConnUserChanged(s)     => { self.conn_user = s;     Task::none() }
            Message::ConnPasswordChanged(s) => { self.conn_password = s; Task::none() }
            Message::ConnUseTlsToggled(b)   => { self.conn_use_tls = b;  Task::none() }
            Message::ReadOnlyToggled(b)     => { self.read_only = b;     Task::none() }

            Message::ConnectPressed => {
                let port: u16 = match self.conn_port.parse() {
                    Ok(p) => p,
                    Err(_) => { self.conn_status = format!("Invalid port \"{}\"", self.conn_port); return Task::none(); }
                };
                let cfg = Arc::new(ConnConfig {
                    host: self.conn_host.clone(), port, dbname: self.conn_dbname.clone(),
                    user: self.conn_user.clone(), password: self.conn_password.clone(), use_tls: self.conn_use_tls,
                });
                Task::perform(
                    async move { tokio::task::spawn_blocking(move || PgHandle::connect_sync(&cfg).map(|_| ())).await.unwrap_or_else(|e| Err(e.to_string())) },
                    Message::ConnectionResult,
                )
            }

            Message::ConnectionResult(Ok(())) => {
                let port: u16 = self.conn_port.parse().unwrap_or(5432);
                let cfg = ConnConfig {
                    host: self.conn_host.clone(), port, dbname: self.conn_dbname.clone(),
                    user: self.conn_user.clone(), password: self.conn_password.clone(), use_tls: self.conn_use_tls,
                };
                self.conn_status = format!("Connected to {} @ {}:{}", cfg.dbname, cfg.host, cfg.port);
                self.connected = true;
                self.active_config = Some(Arc::new(cfg.clone()));
                self.recent_store.push(RecentConnection { host: cfg.host.clone(), port, dbname: cfg.dbname.clone(), user: cfg.user.clone(), use_tls: cfg.use_tls });
                let _ = self.recent_store.save();
                let schema_task = self.launch_schema_fetch();
                let db_task = self.launch_db_fetch();
                Task::batch([schema_task, db_task])
            }
            Message::ConnectionResult(Err(e)) => { self.conn_status = format!("Error: {e}"); self.connected = false; Task::none() }

            Message::DisconnectPressed => {
                self.pg_handle = None; self.connected = false; self.in_transaction = false;
                self.active_config = None; self.conn_password.zeroize();
                self.conn_status = "Disconnected".into(); self.schema_cache.clear();
                self.available_dbs.clear();
                Task::none()
            }

            Message::LoadRecent(i) => {
                if let Some(e) = self.recent_store.entries.get(i) {
                    self.conn_host = e.host.clone(); self.conn_port = e.port.to_string();
                    self.conn_dbname = e.dbname.clone(); self.conn_user = e.user.clone();
                    self.conn_use_tls = e.use_tls; self.conn_password.zeroize();
                }
                Task::none()
            }

            Message::ProfileNameChanged(s) => { self.profile_name_input = s; Task::none() }
            Message::SavePwToggled(b)      => { self.profile_save_pw = b;    Task::none() }

            Message::SaveProfilePressed => {
                let port: u16 = self.conn_port.parse().unwrap_or(5432);
                let p = ConnectionProfile { name: self.profile_name_input.clone(), host: self.conn_host.clone(), port, dbname: self.conn_dbname.clone(), user: self.conn_user.clone(), save_password: self.profile_save_pw };
                if self.profile_save_pw && !self.conn_password.is_empty() {
                    if let Err(e) = save_password(&p.name, &self.conn_password) { self.notice = Some(format!("credential error: {e}")); }
                }
                self.profile_store.add_or_replace(p);
                match self.profile_store.save() { Ok(_) => self.profile_name_input.clear(), Err(e) => self.notice = Some(format!("save profiles: {e}")) }
                Task::none()
            }

            Message::SelectProfile(i) => {
                self.selected_profile_idx = Some(i);
                if let Some(p) = self.profile_store.profiles.get(i) {
                    self.conn_host = p.host.clone(); self.conn_port = p.port.to_string();
                    self.conn_dbname = p.dbname.clone(); self.conn_user = p.user.clone();
                    if p.save_password { if let Some(pw) = load_password(&p.name) { self.conn_password = pw; } }
                }
                Task::none()
            }

            Message::DeleteProfilePressed => {
                if let Some(i) = self.selected_profile_idx {
                    if let Some(name) = self.profile_store.profiles.get(i).map(|p| p.name.clone()) {
                        self.profile_store.delete(&name); let _ = self.profile_store.save();
                        self.selected_profile_idx = None;
                    }
                }
                Task::none()
            }

            Message::TabSelected(i) => { self.active_tab = i; Task::none() }
            Message::TabAdd => {
                let n = self.query_tabs.len() + 1;
                self.query_tabs.push(QueryTab::new(n));
                self.active_tab = self.query_tabs.len() - 1;
                Task::none()
            }
            Message::TabClose => {
                if self.query_tabs.len() > 1 {
                    self.query_tabs.remove(self.active_tab);
                    if self.active_tab >= self.query_tabs.len() { self.active_tab = self.query_tabs.len() - 1; }
                }
                Task::none()
            }

            Message::QueryEdited(idx, action) => {
                if let Some(tab) = self.query_tabs.get_mut(idx) { tab.content.perform(action); }
                self.refresh_params();
                Task::none()
            }

            Message::ExecutePressed => {
                let raw = self.query_tabs[self.active_tab].sql();
                let sql = self.substitute_params(&raw);
                self.try_fire_query(sql)
            }
            Message::ExplainPressed => {
                let raw = self.query_tabs[self.active_tab].sql();
                let sql = self.substitute_params(&raw);
                self.fire_explain_plan(sql)
            }
            Message::FormatPressed => {
                let sql = self.query_tabs[self.active_tab].sql();
                let opts = sqlformat::FormatOptions { indent: sqlformat::Indent::Spaces(2), uppercase: Some(true), lines_between_queries: 1, ignore_case_convert: None };
                let formatted = sqlformat::format(&sql, &sqlformat::QueryParams::None, &opts);
                self.query_tabs[self.active_tab].content = text_editor::Content::with_text(&formatted);
                Task::none()
            }
            Message::CancelPressed  => { self.query_running = false; Task::none() }
            Message::ClearPressed   => {
                let tab = &mut self.query_tabs[self.active_tab];
                tab.content = text_editor::Content::new(); tab.last_result = None;
                tab.result_error = None; tab.sorted_rows = None; tab.col_stats = None;
                tab.plan_result = None; tab.show_plan = false;
                self.param_names.clear(); self.param_values.clear(); self.batch_results.clear();
                Task::none()
            }
            Message::PivotToggled => { self.query_tabs[self.active_tab].pivot_view ^= true; Task::none() }
            Message::SnapshotNameChanged(s) => { self.snapshot_name_buf = s; Task::none() }
            Message::SnapshotPressed => {
                if let Some(result) = self.query_tabs[self.active_tab].last_result.clone() {
                    let name = if self.snapshot_name_buf.trim().is_empty() { format!("Snapshot {}", self.saved_results.len() + 1) } else { self.snapshot_name_buf.trim().to_owned() };
                    self.saved_results.push(SavedResult { name, columns: result.columns, rows: result.rows });
                    self.snapshot_name_buf.clear();
                    self.notice = Some(format!("Saved snapshot #{}", self.saved_results.len()));
                }
                Task::none()
            }
            Message::BatchModeToggled(b) => { self.batch_mode = b; Task::none() }

            Message::ExportCsv => {
                if let Some(r) = &self.query_tabs[self.active_tab].last_result {
                    let rows = self.query_tabs[self.active_tab].sorted_rows.as_ref().unwrap_or(&r.rows).clone();
                    let cols = r.columns.clone();
                    if let Some(p) = rfd::FileDialog::new().add_filter("CSV",&["csv"]).save_file() { let _ = std::fs::write(p, build_csv(&cols, &rows)); }
                }
                Task::none()
            }
            Message::ExportJson => {
                if let Some(r) = &self.query_tabs[self.active_tab].last_result {
                    let rows = self.query_tabs[self.active_tab].sorted_rows.as_ref().unwrap_or(&r.rows).clone();
                    let cols = r.columns.clone();
                    if let Some(p) = rfd::FileDialog::new().add_filter("JSON",&["json"]).save_file() { let _ = std::fs::write(p, build_json(&cols, &rows)); }
                }
                Task::none()
            }
            Message::ExportXlsx => {
                if let Some(r) = &self.query_tabs[self.active_tab].last_result {
                    let rows = self.query_tabs[self.active_tab].sorted_rows.as_ref().unwrap_or(&r.rows).clone();
                    let cols = r.columns.clone();
                    if let Some(p) = rfd::FileDialog::new().add_filter("Excel",&["xlsx"]).save_file() { if let Ok(data) = build_xlsx(&cols, &rows) { let _ = std::fs::write(p, data); } }
                }
                Task::none()
            }

            Message::SortColumn(ci) => {
                let tab = &mut self.query_tabs[self.active_tab];
                let asc = if tab.sort_col == Some(ci) { !tab.sort_asc } else { true };
                tab.sort_col = Some(ci); tab.sort_asc = asc; tab.apply_sort();
                Task::none()
            }

            Message::ShowColStats(ci, col_name) => {
                let rows = {
                    let tab = &self.query_tabs[self.active_tab];
                    tab.sorted_rows.as_ref().or_else(|| tab.last_result.as_ref().map(|r| &r.rows)).cloned()
                };
                if let Some(rows) = rows {
                    self.query_tabs[self.active_tab].col_stats = Some(ColStats::compute(&col_name, &rows, ci));
                }
                Task::none()
            }
            Message::CloseColStats => { self.query_tabs[self.active_tab].col_stats = None; Task::none() }
            Message::CopyCell(_)   => Task::none(), // clipboard handled by Iced automatically via text selection
            Message::PinCell(label, value) => {
                if let Some(e) = self.pinboard.iter_mut().find(|p| p.label == label) { e.value = value; }
                else { self.pinboard.push(PinboardEntry { label, value }); }
                self.notice = Some("Pinned.".into());
                Task::none()
            }
            Message::ConfirmRun => {
                self.show_confirm_dialog = false;
                if let Some(sql) = self.pending_sql.take() { return self.fire_query_sync(sql); }
                Task::none()
            }
            Message::CancelConfirm => { self.show_confirm_dialog = false; self.pending_sql = None; Task::none() }

            Message::QueryResult(result) => {
                self.query_running = false;
                let tab = &mut self.query_tabs[self.active_tab];
                match result {
                    Ok(qr) => {
                        let _ = self.history_store.insert(&HistoryEntry { id: 0, timestamp: chrono::Local::now(), query: tab.sql(), row_count: Some(qr.row_count), error: None });
                        tab.result_error = None; tab.sort_col = None; tab.col_stats = None;
                        tab.last_result = Some(qr); tab.apply_sort(); tab.update_label();
                    }
                    Err(e) => {
                        let _ = self.history_store.insert(&HistoryEntry { id: 0, timestamp: chrono::Local::now(), query: tab.sql(), row_count: None, error: Some(e.clone()) });
                        tab.result_error = Some(e); tab.last_result = None; tab.sorted_rows = None; tab.col_stats = None; tab.update_label();
                    }
                }
                Task::none()
            }

            Message::ExplainPlanResult(result) => {
                self.query_running = false;
                let tab = &mut self.query_tabs[self.active_tab];
                match result {
                    Ok(nodes) => { tab.plan_result = Some(nodes); tab.show_plan = true; tab.result_error = None; }
                    Err(e)    => { tab.result_error = Some(e); tab.plan_result = None; tab.show_plan = false; }
                }
                Task::none()
            }
            Message::ShowPlanView(show) => {
                self.query_tabs[self.active_tab].show_plan = show;
                Task::none()
            }

            Message::ToolbarLabelChanged(s) => { self.toolbar_label_buf = s; Task::none() }
            Message::PinToToolbar => {
                let sql = self.query_tabs[self.active_tab].sql();
                if sql.trim().is_empty() { return Task::none(); }
                let label = if self.toolbar_label_buf.trim().is_empty() { format!("Query {}", self.toolbar_queries.len() + 1) } else { self.toolbar_label_buf.trim().to_owned() };
                self.toolbar_queries.push(ToolbarQuery { label, sql }); self.toolbar_label_buf.clear();
                Task::none()
            }
            Message::RunToolbarQuery(i) => {
                if let Some(tq) = self.toolbar_queries.get(i) { let sql = tq.sql.clone(); return self.try_fire_query(sql); }
                Task::none()
            }
            Message::RemoveToolbarQuery(i) => { if i < self.toolbar_queries.len() { self.toolbar_queries.remove(i); } Task::none() }
            Message::MoveToolbarLeft(i)    => { if i > 0 { self.toolbar_queries.swap(i-1,i); } Task::none() }
            Message::MoveToolbarRight(i)   => { if i+1 < self.toolbar_queries.len() { self.toolbar_queries.swap(i,i+1); } Task::none() }

            Message::BeginTransaction => {
                if let Some(h) = &mut self.pg_handle { match h.begin_sync() { Ok(_) => { self.in_transaction = true; self.tx_error.clear(); } Err(e) => self.tx_error = e } }
                Task::none()
            }
            Message::CommitTransaction => {
                if let Some(h) = &mut self.pg_handle { match h.commit_sync() { Ok(_) => { self.in_transaction = false; self.tx_error.clear(); } Err(e) => self.tx_error = e } }
                Task::none()
            }
            Message::RollbackTransaction => {
                if let Some(h) = &mut self.pg_handle { match h.rollback_sync() { Ok(_) => { self.in_transaction = false; self.tx_error.clear(); } Err(e) => self.tx_error = e } }
                Task::none()
            }

            Message::HistorySearchChanged(s) => { self.history_search = s; Task::none() }
            Message::FilterFromChanged(s)    => { self.filter_from_str = s; Task::none() }
            Message::FilterToChanged(s)      => { self.filter_to_str = s; Task::none() }
            Message::HistoryFilter => {
                let from = NaiveDate::parse_from_str(&self.filter_from_str, "%Y-%m-%d").ok();
                let to   = NaiveDate::parse_from_str(&self.filter_to_str,   "%Y-%m-%d").ok();
                match (from, to) {
                    (Some(f), Some(t)) => match self.history_store.fetch_between(f, t) { Ok(e) => { self.history_view = e; self.history_error.clear(); } Err(e) => self.history_error = e.to_string() },
                    _ => self.history_error = "Invalid date. Use YYYY-MM-DD.".into(),
                }
                Task::none()
            }
            Message::HistoryShowAll => {
                match self.history_store.fetch_all() { Ok(e) => { self.history_view = e; self.history_error.clear(); } Err(e) => self.history_error = e.to_string() }
                Task::none()
            }
            Message::LoadHistoryQuery(q) => {
                self.query_tabs[self.active_tab].content = text_editor::Content::with_text(&q);
                self.current_view = View::QueryEditor; Task::none()
            }

            Message::SnippetNameChanged(s) => { self.snippet_name_input = s; Task::none() }
            Message::SaveSnippet => {
                let sql = self.query_tabs[self.active_tab].sql();
                if !self.snippet_name_input.is_empty() && !sql.trim().is_empty() {
                    self.snippet_store.add_or_replace(Snippet { name: self.snippet_name_input.clone(), sql: sql.trim().to_owned() });
                    match self.snippet_store.save() { Ok(_) => self.snippet_name_input.clear(), Err(e) => self.notice = Some(format!("save snippets: {e}")) }
                }
                Task::none()
            }
            Message::InsertSnippet(i) => {
                if let Some(s) = self.snippet_store.snippets.get(i) {
                    let sql = s.sql.clone();
                    let tab = &mut self.query_tabs[self.active_tab];
                    let existing = tab.sql();
                    tab.content = text_editor::Content::with_text(&if existing.trim().is_empty() { sql } else { format!("{existing}\n{sql}") });
                    self.current_view = View::QueryEditor;
                }
                Task::none()
            }
            Message::DeleteSnippet(i) => {
                if let Some(name) = self.snippet_store.snippets.get(i).map(|s| s.name.clone()) { self.snippet_store.delete(&name); let _ = self.snippet_store.save(); }
                Task::none()
            }

            Message::LoadSchema => { self.schema_expanded.clear(); self.launch_schema_fetch() }
            Message::SchemaResult(Ok(tables)) => { self.schema_cache.tables = tables; self.schema_cache.loaded = true; self.schema_cache.error = None; self.schema_loading = false; Task::none() }
            Message::SchemaResult(Err(e))     => { self.schema_cache.error = Some(e); self.schema_cache.loaded = false; self.schema_loading = false; Task::none() }
            Message::SchemaSearchChanged(s) => { self.schema_search = s; Task::none() }
            Message::ToggleSchemaTable(key) => {
                if !self.schema_expanded.remove(&key) { self.schema_expanded.insert(key); }
                Task::none()
            }
            Message::DatabasesLoaded(Ok(dbs)) => { self.available_dbs = dbs; self.dbs_loading = false; Task::none() }
            Message::DatabasesLoaded(Err(_))  => { self.dbs_loading = false; Task::none() }
            Message::SwitchDatabase(dbname) => {
                if self.conn_dbname == dbname { return Task::none(); }
                self.conn_dbname = dbname;
                let port: u16 = self.conn_port.parse().unwrap_or(5432);
                let cfg = Arc::new(ConnConfig {
                    host: self.conn_host.clone(), port, dbname: self.conn_dbname.clone(),
                    user: self.conn_user.clone(), password: self.conn_password.clone(), use_tls: self.conn_use_tls,
                });
                self.conn_status = format!("Switching to {}…", self.conn_dbname);
                Task::perform(
                    async move { tokio::task::spawn_blocking(move || PgHandle::connect_sync(&cfg).map(|_| ())).await.unwrap_or_else(|e| Err(e.to_string())) },
                    Message::ConnectionResult,
                )
            }

            Message::PinLabelChanged(s) => { self.pin_label_buf = s; Task::none() }
            Message::AddPin => {
                let label = self.pin_label_buf.trim().to_owned();
                if !label.is_empty() && !self.pinboard.iter().any(|p| p.label == label) { self.pinboard.push(PinboardEntry { label, value: String::new() }); }
                self.pin_label_buf.clear(); Task::none()
            }
            Message::DeletePin(i)          => { if i < self.pinboard.len() { self.pinboard.remove(i); } Task::none() }
            Message::PinValueChanged(i, v) => { if let Some(p) = self.pinboard.get_mut(i) { p.value = v; } Task::none() }
            Message::PastePinToParam(i) => {
                if let Some(pin) = self.pinboard.get(i) {
                    let label = pin.label.clone(); let value = pin.value.clone();
                    if let Some(pi) = self.param_names.iter().position(|n| *n == label) { self.param_values[pi] = value; self.notice = Some(format!("Pasted ${label}.")); }
                    else { self.notice = Some(format!("No active param ${label}.")); }
                }
                Task::none()
            }
            Message::ParamValueChanged(i, v) => { if let Some(pv) = self.param_values.get_mut(i) { *pv = v; } Task::none() }

            Message::DiffLeftSelected(i)  => { self.diff_left_idx  = Some(i); Task::none() }
            Message::DiffRightSelected(i) => { self.diff_right_idx = Some(i); Task::none() }
            Message::ClearSnapshots       => { self.saved_results.clear(); self.diff_left_idx = None; self.diff_right_idx = None; Task::none() }

            Message::AutoLabelChanged(s)    => { self.auto_job_label_buf = s; Task::none() }
            Message::AutoSqlChanged(s)      => { self.auto_job_sql_buf = s;   Task::none() }
            Message::SchedKindChanged(k)    => { self.schedule_builder.kind = k; Task::none() }
            Message::SchedSecsChanged(s)    => { self.schedule_builder.secs = s; Task::none() }
            Message::SchedHourChanged(s)    => { self.schedule_builder.hour = s; Task::none() }
            Message::SchedMinuteChanged(s)  => { self.schedule_builder.minute = s; Task::none() }
            Message::SchedWeekdayChanged(w) => { self.schedule_builder.weekday = w; Task::none() }
            Message::SchedDomChanged(s)     => { self.schedule_builder.day_of_month = s; Task::none() }

            Message::AddAutoJob => {
                if let Some(schedule) = self.schedule_builder.build() {
                    self.auto_jobs.push(AutoJob { label: self.auto_job_label_buf.trim().to_owned(), sql: self.auto_job_sql_buf.trim().to_owned(), schedule, last_run: None, enabled: true });
                    self.auto_job_label_buf.clear(); self.auto_job_sql_buf.clear(); self.schedule_builder = ScheduleBuilder::new();
                }
                Task::none()
            }
            Message::RemoveAutoJob(i) => { if i < self.auto_jobs.len() { self.auto_jobs.remove(i); } Task::none() }
            Message::ToggleAutoJob(i, b) => { if let Some(j) = self.auto_jobs.get_mut(i) { j.enabled = b; } Task::none() }
            Message::FireAutoJobNow(i) => {
                if let Some(j) = self.auto_jobs.get_mut(i) { j.last_run = Some(chrono::Local::now()); let sql = j.sql.clone(); return self.fire_query_sync(sql); }
                Task::none()
            }

            Message::Tick => {
                if !self.connected || self.query_running { return Task::none(); }
                let mut fire_sql: Option<String> = None;
                for job in &mut self.auto_jobs {
                    if !job.enabled { continue; }
                    if job.schedule.is_due(job.last_run) { job.last_run = Some(chrono::Local::now()); if fire_sql.is_none() { fire_sql = Some(job.sql.clone()); } }
                }
                if let Some(sql) = fire_sql { return self.fire_query_sync(sql); }
                Task::none()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// View helpers
// ---------------------------------------------------------------------------
const PAGE_PAD: f32    = 24.0;
const BTN_PAD:  [u16; 2] = [7, 18];
const RADIUS:   f32    = 8.0;

#[allow(dead_code)]
fn card<'a>(p: Pal, content: impl Into<Element<'a, Message>>) -> iced::widget::Container<'a, Message> {
    let panel = p.panel;
    let border = p.border;
    container(content)
        .padding(20)
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(panel)),
            border: iced::Border { color: border, width: 1.0, radius: RADIUS.into() },
            ..Default::default()
        })
}

fn nav_btn<'a>(p: Pal, label: &'a str, is_active: bool) -> iced::widget::Button<'a, Message> {
    let accent      = p.accent;
    let active_fg   = p.accent_text;
    let inactive_fg = p.nav_inactive_fg;
    let text_col    = p.text;
    let fg = if is_active { active_fg } else { inactive_fg };

    let inner: Element<Message> = container(
        text(label).size(13).color(fg)
    )
    .padding(iced::Padding { top: 0.0, right: 0.0, bottom: 0.0, left: 12.0 })
    .height(34)
    .align_y(iced::alignment::Vertical::Center)
    .into();

    button(inner)
        .width(Length::Fill)
        .padding(iced::Padding { top: 2.0, right: 8.0, bottom: 2.0, left: 4.0 })
        .style(move |_, status| {
            let hovered = matches!(status, button::Status::Hovered);
            button::Style {
                background: if is_active {
                    Some(iced::Background::Color(accent))
                } else if hovered {
                    Some(iced::Background::Color(Color { r: text_col.r, g: text_col.g, b: text_col.b, a: 0.07 }))
                } else {
                    None
                },
                border: iced::Border { radius: RADIUS.into(), ..Default::default() },
                text_color: fg,
                ..Default::default()
            }
        })
}

fn badge(label: &str, color: Color) -> Element<'static, Message> {
    container(text(label.to_owned()).size(10).color(Color::WHITE))
        .padding([3, 8])
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(color)),
            border: iced::Border { radius: 4.0.into(), ..Default::default() },
            ..Default::default()
        })
        .into()
}

fn section_heading<'a>(p: Pal, label: &'a str) -> Element<'a, Message> {
    let text_col = p.text;
    let bar_col  = Color { a: 0.35, ..text_col };
    row![
        container(horizontal_space().width(3))
            .height(16)
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(bar_col)),
                border: iced::Border { radius: 2.0.into(), ..Default::default() },
                ..Default::default()
            }),
        text(label).size(14).color(text_col),
    ].spacing(8).align_y(Alignment::Center).into()
}

fn lbl(s: &str) -> Element<'_, Message> { text(s).size(13).into() }

fn muted_txt<'a>(p: Pal, s: &'a str) -> Element<'a, Message> {
    text(s).size(12).color(p.muted).into()
}

fn err_lbl(p: Pal, s: impl ToString) -> Element<'static, Message> {
    let err = p.error;
    text(s.to_string()).size(13).color(err).into()
}

fn action_btn<'a>(p: Pal, label: &'a str, enabled: bool) -> iced::widget::Button<'a, Message> {
    let action   = p.action;
    let text_col = p.text;
    let border   = p.border;
    button(text(label).size(13))
        .padding(BTN_PAD)
        .style(move |_, status| {
            let alpha   = if enabled { 1.0_f32 } else { 0.38 };
            let hovered = matches!(status, button::Status::Hovered) && enabled;
            let bg = Color {
                r: action.r + if hovered { 0.05 } else { 0.0 },
                g: action.g + if hovered { 0.05 } else { 0.0 },
                b: action.b + if hovered { 0.05 } else { 0.0 },
                a: alpha,
            };
            button::Style {
                background: Some(iced::Background::Color(bg)),
                text_color: Color { a: alpha, ..text_col },
                border: iced::Border { color: Color { a: 0.5 * alpha, ..border }, width: 1.0, radius: RADIUS.into() },
                ..Default::default()
            }
        })
}

fn ghost_btn<'a>(p: Pal, label: &'a str) -> iced::widget::Button<'a, Message> {
    let muted    = p.muted;
    let border   = p.border;
    let hover_bg = p.active;
    button(text(label).size(12).color(muted))
        .padding([5, 12])
        .style(move |_, status| {
            let hovered = matches!(status, button::Status::Hovered);
            button::Style {
                background: if hovered { Some(iced::Background::Color(Color { a: 0.25, ..hover_bg })) } else { None },
                border: iced::Border { color: Color { a: 0.6, ..border }, width: 1.0, radius: RADIUS.into() },
                text_color: muted,
                ..Default::default()
            }
        })
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------
impl App {
    pub fn view(&self) -> Element<'_, Message> {
        let p = Pal::for_dark(self.dark_theme);
        let sidebar = self.view_sidebar(p);
        let toolbar = self.view_toolbar_panel(p);
        let status  = self.view_status_bar(p);

        let content: Element<Message> = match self.current_view {
            View::Connection  => self.view_connection(p),
            View::Schema      => self.view_schema(p),
            View::QueryEditor => self.view_query_editor(p),
            View::History     => self.view_history(p),
            View::Snippets    => self.view_snippets(p),
            View::Pinboard    => self.view_pinboard(p),
            View::Diff        => self.view_diff(p),
            View::Automation  => self.view_automation(p),
            View::Shortcuts   => self.view_shortcuts(p),
        };

        let err = p.error; let panel = p.panel; let _border = p.border; let muted = p.muted;
        let main_content: Element<Message> = if self.show_confirm_dialog {
            let dlg = container(column![
                text("Confirm destructive query").size(15).color(err),
                text(self.pending_sql.as_deref().unwrap_or("")).size(12).color(muted),
                row![
                    ghost_btn(p, "Cancel").on_press(Message::CancelConfirm),
                    button(text("Run Anyway").size(13).color(err))
                        .padding(BTN_PAD)
                        .style(move |_, _| button::Style {
                            background: Some(iced::Background::Color(Color { a: 0.15, ..err })),
                            border: iced::Border { radius: RADIUS.into(), ..Default::default() },
                            text_color: err,
                            ..Default::default()
                        })
                        .on_press(Message::ConfirmRun),
                ].spacing(10),
            ].spacing(16).padding(24))
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(panel)),
                border: iced::Border { color: err, width: 1.0, radius: RADIUS.into() },
                ..Default::default()
            });
            column![content, dlg].spacing(12).padding(12).into()
        } else { content };

        let bg = p.bg;
        let body = row![
            sidebar,
            container(
                column![toolbar, main_content].spacing(0)
            ).width(Length::Fill).style(move |_| container::Style {
                background: Some(iced::Background::Color(bg)),
                ..Default::default()
            }),
        ].spacing(0);

        column![body, status].spacing(0)
            .height(Length::Fill)
            .into()
    }

    fn view_sidebar(&self, p: Pal) -> Element<'_, Message> {
        let v = &self.current_view;

        let logo_text_color = p.text;
        let logo_accent = p.accent;
        let logo = container(
            row![
                container(horizontal_space().width(4)).height(20)
                    .style(move |_| container::Style {
                        background: Some(iced::Background::Color(logo_accent)),
                        border: iced::Border { radius: 3.0.into(), ..Default::default() },
                        ..Default::default()
                    }),
                text("PQL").size(16).color(logo_text_color),
            ].spacing(8).align_y(Alignment::Center)
        ).padding(iced::Padding { top: 20.0, right: 16.0, bottom: 16.0, left: 16.0 });

        let nav_items: Vec<Element<Message>> = vec![
            nav_btn(p, "Connection", v == &View::Connection).on_press(Message::SetView(View::Connection)).into(),
            nav_btn(p, "Schema",     v == &View::Schema).on_press(Message::SetView(View::Schema)).into(),
            nav_btn(p, "Query",      v == &View::QueryEditor).on_press(Message::SetView(View::QueryEditor)).into(),
            nav_btn(p, "Snippets",   v == &View::Snippets).on_press(Message::SetView(View::Snippets)).into(),
            nav_btn(p, "Pinboard",   v == &View::Pinboard).on_press(Message::SetView(View::Pinboard)).into(),
            nav_btn(p, "Diff",       v == &View::Diff).on_press(Message::SetView(View::Diff)).into(),
            nav_btn(p, "Automation", v == &View::Automation).on_press(Message::SetView(View::Automation)).into(),
            nav_btn(p, "History",    v == &View::History).on_press(Message::SetView(View::History)).into(),
            nav_btn(p, "Shortcuts",  v == &View::Shortcuts).on_press(Message::SetView(View::Shortcuts)).into(),
        ];

        let _muted = p.muted;

        let footer = container(
            ghost_btn(p, if self.dark_theme { "Light mode" } else { "Dark mode" })
                .width(Length::Fill)
                .on_press(Message::ToggleTheme)
        ).padding(iced::Padding { top: 12.0, right: 12.0, bottom: 16.0, left: 12.0 });

        let sidebar_bg = p.sidebar; let border = p.border;
        container(
            column![
                logo,
                container(Column::with_children(nav_items).spacing(2).padding([0, 6]))
                    .width(Length::Fill),
                horizontal_space().height(Length::Fill),
                footer,
            ].spacing(0).height(Length::Fill)
        )
        .width(168.0)
        .height(Length::Fill)
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(sidebar_bg)),
            border: iced::Border { color: border, width: 1.0, ..Default::default() },
            ..Default::default()
        })
        .into()
    }

    fn view_toolbar_panel(&self, p: Pal) -> Element<'_, Message> {
        if self.toolbar_queries.is_empty() { return horizontal_space().height(0).into(); }
        let muted = p.muted; let text_col = p.text; let panel = p.panel; let border = p.border;
        let action = p.action;
        let mut btns: Vec<Element<Message>> = vec![text("Quick run:").size(11).color(muted).into()];
        for (i, tq) in self.toolbar_queries.iter().enumerate() {
            btns.push(
                button(text(&tq.label).size(12).color(text_col))
                    .padding([4, 12])
                    .style(move |_, status| {
                        let hovered = matches!(status, button::Status::Hovered);
                        button::Style {
                            background: Some(iced::Background::Color(if hovered {
                                Color { r: action.r + 0.04, g: action.g + 0.04, b: action.b + 0.04, a: 1.0 }
                            } else {
                                action
                            })),
                            border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                            text_color: text_col,
                            ..Default::default()
                        }
                    })
                    .on_press(Message::RunToolbarQuery(i))
                    .into()
            );
        }
        container(Row::with_children(btns).spacing(8).align_y(Alignment::Center))
            .width(Length::Fill).padding([6, 16])
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(panel)),
                ..Default::default()
            })
            .into()
    }

    fn view_status_bar(&self, p: Pal) -> Element<'_, Message> {
        let muted = p.muted; let text_col = p.text;
        let mut items: Vec<Element<Message>> = vec![];
        if self.read_only      { items.push(badge("READ-ONLY", p.muted)); }
        if self.batch_mode     { items.push(badge("BATCH",     p.muted)); }
        if self.in_transaction { items.push(badge("TXN",       p.warning)); }
        if self.query_running  { items.push(badge("● RUNNING", p.active)); }
        if self.connected {
            let db_label = format!("{}", self.conn_dbname);
            items.push(
                container(text(db_label).size(11).color(muted))
                    .padding([2, 8])
                    .style(move |_| container::Style {
                        border: iced::Border { color: Color { a: 0.22, ..muted }, width: 1.0, radius: 4.0.into() },
                        ..Default::default()
                    })
                    .into()
            );
        }
        items.push(horizontal_space().into());
        if let Some(notice) = &self.notice {
            items.push(
                button(text(format!("✕  {notice}")).size(11).color(text_col))
                    .padding([2, 10])
                    .style(|_, _| button::Style { background: None, ..Default::default() })
                    .on_press(Message::DismissNotice)
                    .into()
            );
        }
        let status_bg = p.status_bg; let border = p.border;
        container(Row::with_children(items).spacing(8).align_y(Alignment::Center))
            .width(Length::Fill).height(28).padding([0, 14])
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(status_bg)),
                border: iced::Border { color: Color { a: 0.5, ..border }, width: 1.0, radius: 0.0.into() },
                ..Default::default()
            })
            .into()
    }

    fn view_connection(&self, p: Pal) -> Element<'_, Message> {
        let profile_names: Vec<String> = self.profile_store.profiles.iter().map(|p| p.name.clone()).collect();
        let selected_name = self.selected_profile_idx.and_then(|i| profile_names.get(i)).cloned();
        let can_connect = !self.connected && !self.conn_host.is_empty() && !self.conn_dbname.is_empty() && !self.conn_user.is_empty();
        let sc = if self.connected { p.success } else { p.error };
        let dot_col = sc;
        let text_col = p.text;
        let muted = p.muted;
        let panel = p.panel;
        let border = p.border;
        let action = p.action;

        // ── helpers for inline field rows ─────────────────────────────────
        let inp = |placeholder: &'static str, val: &str, msg: fn(String) -> Message| -> Element<Message> {
            text_input(placeholder, val).on_input(msg).width(Length::Fill).into()
        };
        let inp_secure = |placeholder: &'static str, val: &str, msg: fn(String) -> Message| -> Element<Message> {
            text_input(placeholder, val).on_input(msg).secure(true).width(Length::Fill).into()
        };

        // ── connect / disconnect row ───────────────────────────────────────
        let connect_bg = if can_connect { p.accent } else { Color { a: 0.25, ..p.accent } };
        let connect_fg = if can_connect { p.accent_text } else { Color { a: 0.40, ..p.accent_text } };
        let connect_label = if self.connected { "Reconnect" } else { "Connect" };
        let connect_btn = button(text(connect_label).size(13).color(connect_fg))
            .padding([9, 20])
            .style(move |_, status: button::Status| {
                let hovered = matches!(status, button::Status::Hovered) && can_connect;
                let bump = if hovered { 0.07 } else { 0.0 };
                button::Style {
                    background: Some(iced::Background::Color(Color {
                        r: (connect_bg.r + bump).min(1.0),
                        g: (connect_bg.g + bump).min(1.0),
                        b: (connect_bg.b + bump).min(1.0),
                        a: connect_bg.a,
                    })),
                    border: iced::Border { radius: RADIUS.into(), ..Default::default() },
                    text_color: connect_fg,
                    ..Default::default()
                }
            })
            .on_press_maybe(if can_connect { Some(Message::ConnectPressed) } else { None });

        let conn_row: Element<Message> = if self.connected {
            row![
                connect_btn,
                horizontal_space(),
                button(text("Disconnect").size(12).color(p.error))
                    .padding([9, 0])
                    .style(|_, _: button::Status| button::Style { background: None, ..Default::default() })
                    .on_press(Message::DisconnectPressed),
            ].align_y(Alignment::Center).into()
        } else {
            connect_btn.into()
        };

        // ── connection form — centered card, max 440px ────────────────────
        let conn_form = container(
            column![
                // Page title, not a section header — large and prominent
                text("Connect to PostgreSQL").size(22).color(text_col),
                text("Enter your database credentials below.").size(13).color(muted),
                // divider
                container(horizontal_space().height(1)).width(Length::Fill)
                    .style(move |_| container::Style {
                        background: Some(iced::Background::Color(Color { a: 0.15, ..muted })),
                        ..Default::default()
                    }),
                // host + port inline
                row![
                    column![
                        text("Host").size(12).color(muted),
                        inp("localhost", &self.conn_host, Message::ConnHostChanged),
                    ].spacing(5).width(Length::FillPortion(3)),
                    column![
                        text("Port").size(12).color(muted),
                        inp("5432", &self.conn_port, Message::ConnPortChanged),
                    ].spacing(5).width(Length::FillPortion(1)),
                ].spacing(12),
                column![
                    text("Database").size(12).color(muted),
                    {
                        let db_widget: Element<Message> = if self.connected && !self.available_dbs.is_empty() {
                            let dbs: Vec<String> = self.available_dbs.clone();
                            let current = self.conn_dbname.clone();
                            pick_list(dbs, Some(current), Message::SwitchDatabase)
                                .width(Length::Fill)
                                .into()
                        } else {
                            inp("", &self.conn_dbname, Message::ConnDbnameChanged).into()
                        };
                        db_widget
                    },
                ].spacing(5),
                // username + password inline
                row![
                    column![
                        text("Username").size(12).color(muted),
                        inp("", &self.conn_user, Message::ConnUserChanged),
                    ].spacing(5).width(Length::Fill),
                    column![
                        text("Password").size(12).color(muted),
                        inp_secure("", &self.conn_password, Message::ConnPasswordChanged),
                    ].spacing(5).width(Length::Fill),
                ].spacing(12),
                row![
                    checkbox("Use TLS", self.conn_use_tls).on_toggle(Message::ConnUseTlsToggled),
                    checkbox("Read-only", self.read_only).on_toggle(Message::ReadOnlyToggled),
                ].spacing(20),
                conn_row,
                // status row
                row![
                    container(horizontal_space().width(7)).height(7)
                        .style(move |_| container::Style {
                            background: Some(iced::Background::Color(dot_col)),
                            border: iced::Border { radius: 4.0.into(), ..Default::default() },
                            ..Default::default()
                        }),
                    text(&self.conn_status).size(12).color(sc),
                ].spacing(6).align_y(Alignment::Center),
            ]
            .spacing(16)
            .width(Length::Fill)
        )
        .padding(32)
        .max_width(460)
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(panel)),
            border: iced::Border { color: border, width: 1.0, radius: (RADIUS * 1.5).into() },
            ..Default::default()
        });

        // ── right panel: recent + profiles ───────────────────────────────
        let mut recents_items: Vec<Element<Message>> = vec![];
        if self.recent_store.entries.is_empty() {
            recents_items.push(
                container(text("No recent connections").size(13).color(muted))
                    .padding([12, 14])
                    .into()
            );
        }
        for (i, e) in self.recent_store.entries.iter().enumerate() {
            let is_last = i + 1 == self.recent_store.entries.len();
            let item_border = border;
            recents_items.push(
                button(
                    column![
                        text(format!("{}@{}", e.user, e.dbname)).size(13).color(text_col),
                        text(format!("{}:{}  ·  {}", e.host, e.port, if e.use_tls { "TLS" } else { "no TLS" })).size(11).color(muted),
                    ].spacing(3)
                )
                .width(Length::Fill)
                .padding([10, 14])
                .style(move |_, status| {
                    let hovered = matches!(status, button::Status::Hovered);
                    button::Style {
                        background: if hovered { Some(iced::Background::Color(Color { a: 0.07, ..action })) } else { None },
                        border: if !is_last {
                            iced::Border { color: Color { a: 0.3, ..item_border }, width: 0.0, radius: 0.0.into() }
                        } else { iced::Border::default() },
                        text_color: text_col,
                        ..Default::default()
                    }
                })
                .on_press(Message::LoadRecent(i))
                .into()
            );
            if !is_last {
                recents_items.push(
                    container(horizontal_space().height(1)).width(Length::Fill)
                        .padding([0, 14])
                        .style(move |_| container::Style {
                            background: Some(iced::Background::Color(Color { a: 0.12, ..border })),
                            ..Default::default()
                        })
                        .into()
                );
            }
        }

        let recents_panel = container(
            column![
                container(text("Recent").size(13).color(muted))
                    .padding([12, 14]),
                Column::with_children(recents_items).spacing(0),
            ].spacing(0)
        )
        .width(Length::Fill)
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(panel)),
            border: iced::Border { color: border, width: 1.0, radius: RADIUS.into() },
            ..Default::default()
        });

        // profiles panel
        let profiles_panel = container(
            column![
                container(text("Saved Profiles").size(13).color(muted)).padding([12, 14]),
                container(
                    column![
                        row![
                            pick_list(profile_names.clone(), selected_name, {
                                let profile_names = profile_names.clone();
                                move |name: String| {
                                    let i = profile_names.iter().position(|n| n == &name).unwrap_or(0);
                                    Message::SelectProfile(i)
                                }
                            }).placeholder("Choose a profile…").width(Length::Fill),
                            action_btn(p, "Delete", self.selected_profile_idx.is_some())
                                .on_press_maybe(self.selected_profile_idx.map(|_| Message::DeleteProfilePressed)),
                        ].spacing(8).align_y(Alignment::Center),
                        container(horizontal_space().height(1)).width(Length::Fill)
                            .style(move |_| container::Style {
                                background: Some(iced::Background::Color(Color { a: 0.12, ..border })),
                                ..Default::default()
                            }),
                        text("Save current credentials as a profile").size(12).color(muted),
                        row![
                            text_input("Profile name", &self.profile_name_input)
                                .on_input(Message::ProfileNameChanged).width(Length::Fill),
                            action_btn(p, "Save", !self.profile_name_input.is_empty())
                                .on_press_maybe(if !self.profile_name_input.is_empty() { Some(Message::SaveProfilePressed) } else { None }),
                        ].spacing(8).align_y(Alignment::Center),
                        checkbox("Save password", self.profile_save_pw).on_toggle(Message::SavePwToggled),
                    ].spacing(10)
                ).padding([0, 14]).padding(iced::Padding { top: 0.0, right: 14.0, bottom: 14.0, left: 14.0 }),
            ].spacing(0)
        )
        .width(Length::Fill)
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(panel)),
            border: iced::Border { color: border, width: 1.0, radius: RADIUS.into() },
            ..Default::default()
        });

        let right_col = column![recents_panel, profiles_panel]
            .spacing(16)
            .width(Length::FillPortion(2));

        // Both columns start from the top; the form card has its own internal padding
        scrollable(
            row![
                container(conn_form)
                    .width(Length::FillPortion(3))
                    .align_x(iced::alignment::Horizontal::Center),
                right_col,
            ]
            .spacing(28)
            .align_y(Alignment::Start)
            .padding(iced::Padding { top: 32.0, right: 28.0, bottom: 28.0, left: 28.0 })
        )
        .width(Length::Fill)
        .height(Length::Fill)
        .into()
    }

    fn view_schema(&self, p: Pal) -> Element<'_, Message> {
        let muted = p.muted; let text_col = p.text; let border = p.border; let panel = p.panel;
        let pad = iced::Padding::from([PAGE_PAD, PAGE_PAD]);

        let toolbar: Element<Message> = row![
            action_btn(p, "Refresh", true).on_press(Message::LoadSchema),
            text_input("Filter tables…", &self.schema_search)
                .on_input(Message::SchemaSearchChanged)
                .width(Length::Fill),
        ].spacing(8).align_y(Alignment::Center).into();

        let mut col: Vec<Element<Message>> = vec![
            section_heading(p, "Schema Browser"),
            toolbar,
        ];

        if !self.connected {
            col.push(err_lbl(p, "Not connected."));
            return scrollable(Column::with_children(col).spacing(12).padding(pad)).into();
        }
        if self.schema_loading {
            col.push(muted_txt(p, "Loading…"));
            return scrollable(Column::with_children(col).spacing(12).padding(pad)).into();
        }
        if let Some(e) = &self.schema_cache.error {
            col.push(err_lbl(p, e.clone()));
            return scrollable(Column::with_children(col).spacing(12).padding(pad)).into();
        }
        if !self.schema_cache.loaded {
            return scrollable(Column::with_children(col).spacing(12).padding(pad)).into();
        }

        let needle = self.schema_search.to_lowercase();

        let mut schemas: Vec<&str> = Vec::new();
        for t in &self.schema_cache.tables {
            if !schemas.contains(&t.schema.as_str()) { schemas.push(&t.schema); }
        }

        for schema_name in schemas {
            let tables: Vec<_> = self.schema_cache.tables.iter()
                .filter(|t| t.schema == schema_name)
                .filter(|t| needle.is_empty() || t.name.to_lowercase().contains(&needle))
                .collect();

            if tables.is_empty() { continue; }

            col.push(
                text(format!("{schema_name}  ({} tables)", tables.len()))
                    .size(11).color(muted).into()
            );

            for t in tables {
                let key = format!("{}.{}", t.schema, t.name);
                let expanded = self.schema_expanded.contains(&key);
                let arrow = if expanded { "▾" } else { "▸" };
                let is_view = t.kind.contains("VIEW");
                let kind_tag = if is_view { "  view" } else { "" };
                let col_count = t.columns.len();

                let header = button(
                    row![
                        text(arrow).size(11).color(muted),
                        text(format!("{}{}", t.name, kind_tag))
                            .size(13).color(text_col),
                        horizontal_space(),
                        text(format!("{col_count} col{}", if col_count == 1 { "" } else { "s" }))
                            .size(11).color(muted),
                    ].spacing(6).align_y(Alignment::Center)
                )
                .padding([4, 8])
                .width(Length::Fill)
                .style(move |_, status| button::Style {
                    background: if matches!(status, button::Status::Hovered) {
                        Some(iced::Background::Color(Color { a: 0.06, ..panel }))
                    } else { None },
                    border: iced::Border { radius: 4.0.into(), ..Default::default() },
                    ..Default::default()
                })
                .on_press(Message::ToggleSchemaTable(key));

                if expanded {
                    let col_rows: Vec<Element<Message>> = t.columns.iter().map(|c| {
                        let null_tag = if c.is_nullable { "" } else { " ·  NOT NULL" };
                        row![
                            horizontal_space().width(20),
                            text(&c.name).size(12).color(text_col).font(iced::Font::MONOSPACE).width(Length::Fill),
                            text(format!("{}{null_tag}", c.data_type)).size(11).color(muted),
                        ].spacing(8).align_y(Alignment::Center).into()
                    }).collect();

                    col.push(column![
                        header,
                        container(
                            Column::with_children(col_rows).spacing(2).padding(iced::Padding { top: 2.0, right: 8.0, bottom: 4.0, left: 8.0 })
                        )
                        .width(Length::Fill)
                        .style(move |_| container::Style {
                            border: iced::Border { color: Color { a: 0.15, ..border }, width: 0.0, radius: 0.0.into() },
                            background: Some(iced::Background::Color(Color { a: 0.03, ..panel })),
                            ..Default::default()
                        }),
                    ].spacing(0).into());
                } else {
                    col.push(header.into());
                }
            }

            col.push(
                container(horizontal_space().height(1))
                    .width(Length::Fill)
                    .style(move |_| container::Style {
                        background: Some(iced::Background::Color(Color { a: 0.08, ..border })),
                        ..Default::default()
                    })
                    .into()
            );
        }

        scrollable(Column::with_children(col).spacing(4).padding(pad)).into()
    }

    fn view_query_editor(&self, p: Pal) -> Element<'_, Message> {
        let panel = p.panel; let border = p.border; let text_col = p.text; let muted = p.muted;
        let bg = p.bg; let tab_bar_bg = p.tab_bar_bg; let action = p.action;

        let tabs_row: Vec<Element<Message>> = {
            let mut v: Vec<Element<Message>> = self.query_tabs.iter().enumerate().map(|(i, tab)| {
                let is_active = i == self.active_tab;
                button(text(&tab.label).size(12))
                    .padding([6, 14])
                    .style(move |_, _| button::Style {
                        background: if is_active { Some(iced::Background::Color(panel)) } else { None },
                        text_color: if is_active { text_col } else { muted },
                        border: iced::Border {
                            color: if is_active { border } else { Color::TRANSPARENT },
                            width: if is_active { 1.0 } else { 0.0 },
                            radius: iced::border::Radius { top_left: RADIUS, top_right: RADIUS, bottom_right: 0.0, bottom_left: 0.0 },
                        },
                        ..Default::default()
                    })
                    .on_press(Message::TabSelected(i))
                    .into()
            }).collect();
            v.push(
                button(text("+").size(14).color(muted))
                    .padding([6, 10])
                    .style(move |_, status| button::Style {
                        background: if matches!(status, button::Status::Hovered) {
                            Some(iced::Background::Color(Color { a: 0.4, ..action }))
                        } else { None },
                        border: iced::Border { radius: 4.0.into(), ..Default::default() },
                        text_color: muted,
                        ..Default::default()
                    })
                    .on_press(Message::TabAdd)
                    .into()
            );
            if self.query_tabs.len() > 1 {
                v.push(
                    button(text("✕").size(11).color(muted))
                        .padding([6, 8])
                        .style(move |_, status| button::Style {
                            background: if matches!(status, button::Status::Hovered) {
                                Some(iced::Background::Color(Color { a: 0.4, ..action }))
                            } else { None },
                            border: iced::Border { radius: 4.0.into(), ..Default::default() },
                            text_color: muted,
                            ..Default::default()
                        })
                        .on_press(Message::TabClose)
                        .into()
                );
            }
            v
        };

        let ai = self.active_tab;
        let active = &self.query_tabs[ai];

        let editor = text_editor(&active.content)
            .on_action(move |a| Message::QueryEdited(ai, a))
            .height(220)
            .font(iced::Font::MONOSPACE)
            .highlight_with::<SqlHighlighter>(
                SqlSettings { dark_theme: self.dark_theme },
                crate::highlighter::to_format,
            );

        let ro_warn: Element<Message> = if self.read_only {
            let sql = active.sql().trim().to_uppercase();
            let blocked = !sql.is_empty() && !sql.starts_with("SELECT") && !sql.starts_with("EXPLAIN") && !sql.starts_with("SHOW") && !sql.starts_with("WITH");
            if blocked { err_lbl(p, "Read-only mode: this query will be blocked on execute.") } else { horizontal_space().height(0).into() }
        } else { horizontal_space().height(0).into() };

        let params_row: Element<Message> = if !self.param_names.is_empty() {
            let mut r: Vec<Element<Message>> = vec![text("Params:").size(12).color(muted).into()];
            for (i, name) in self.param_names.iter().enumerate() {
                r.push(text(format!("${name}:")).size(12).color(text_col).into());
                let val = self.param_values.get(i).cloned().unwrap_or_default();
                r.push(text_input("", &val).on_input(move |v| Message::ParamValueChanged(i, v)).width(100).into());
            }
            container(Row::with_children(r).spacing(8).align_y(Alignment::Center))
                .padding([6, 12])
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(panel)),
                    ..Default::default()
                })
                .into()
        } else { horizontal_space().height(0).into() };

        let can_run      = self.active_config.is_some() && !active.sql().trim().is_empty();
        let has_res      = active.last_result.is_some();
        let not_run      = !self.query_running;
        let can_tx_begin = self.pg_handle.is_some() && !self.in_transaction && not_run;
        let can_tx_end   = self.pg_handle.is_some() &&  self.in_transaction && not_run;

        const TB: [u16; 2] = [6, 12];

        let mk_tb_btn = |label: String, is_active: bool| {
            button(text(label).size(12))
                .padding(TB)
                .style(move |_, status| {
                    let hovered = matches!(status, button::Status::Hovered) && is_active;
                    let alpha = if is_active { 1.0f32 } else { 0.28 };
                    button::Style {
                        background: Some(iced::Background::Color(Color {
                            r: action.r + if hovered { 0.06 } else { 0.0 },
                            g: action.g + if hovered { 0.06 } else { 0.0 },
                            b: action.b + if hovered { 0.06 } else { 0.0 },
                            a: alpha,
                        })),
                        border: iced::Border { color: Color { a: 0.5 * alpha, ..border }, width: 1.0, radius: 6.0.into() },
                        text_color: Color { a: alpha, ..text_col },
                        ..Default::default()
                    }
                })
        };

        // small toggle button for binary states (Batch)
        let mk_toggle_btn = |label: &str, active: bool| {
            let bg = if active { Color { a: 0.14, ..text_col } } else { Color { a: 0.0, ..action } };
            let fg = if active { text_col } else { Color { a: 0.38, ..text_col } };
            let bd = if active { Color { a: 0.45, ..text_col } } else { Color { a: 0.25, ..border } };
            button(text(label.to_owned()).size(11).color(fg))
                .padding([4, 10])
                .style(move |_, status| {
                    let hovered = matches!(status, button::Status::Hovered);
                    button::Style {
                        background: Some(iced::Background::Color(Color {
                            a: bg.a + if hovered { 0.06 } else { 0.0 }, ..bg
                        })),
                        border: iced::Border { color: bd, width: 1.0, radius: 6.0.into() },
                        text_color: fg,
                        ..Default::default()
                    }
                })
        };

        let execute_label = if self.query_running { "Running…" } else { "Execute" };
        let pivot_label   = if active.pivot_view { "Normal" } else { "Pivot" };
        let has_sql       = !active.sql().trim().is_empty();
        let divider = || container(horizontal_space().width(1))
            .height(18)
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(Color { a: 0.35, ..border })),
                ..Default::default()
            });

        let toolbar = container(
            row![
                mk_tb_btn(execute_label.to_owned(), can_run && not_run)
                    .on_press_maybe(if can_run && not_run { Some(Message::ExecutePressed) } else { None }),
                mk_tb_btn("Explain".to_owned(), can_run && not_run)
                    .on_press_maybe(if can_run && not_run { Some(Message::ExplainPressed) } else { None }),
                mk_tb_btn("Format".to_owned(), has_sql)
                    .on_press_maybe(if has_sql { Some(Message::FormatPressed) } else { None }),
                mk_tb_btn("Cancel".to_owned(), self.query_running)
                    .on_press_maybe(if self.query_running { Some(Message::CancelPressed) } else { None }),
                mk_tb_btn("Clear".to_owned(), true).on_press(Message::ClearPressed),
                divider(),
                mk_tb_btn("CSV".to_owned(),  has_res).on_press_maybe(if has_res { Some(Message::ExportCsv)  } else { None }),
                mk_tb_btn("JSON".to_owned(), has_res).on_press_maybe(if has_res { Some(Message::ExportJson) } else { None }),
                mk_tb_btn("XLSX".to_owned(), has_res).on_press_maybe(if has_res { Some(Message::ExportXlsx) } else { None }),
                divider(),
                mk_tb_btn(pivot_label.to_owned(), has_res)
                    .on_press_maybe(if has_res { Some(Message::PivotToggled) } else { None }),
                text_input("Snapshot…", &self.snapshot_name_buf).on_input(Message::SnapshotNameChanged).width(110),
                mk_tb_btn("Snapshot".to_owned(), has_res).on_press_maybe(if has_res { Some(Message::SnapshotPressed) } else { None }),
                horizontal_space(),
                mk_toggle_btn("Batch", self.batch_mode)
                    .on_press(Message::BatchModeToggled(!self.batch_mode)),
            ].spacing(4).align_y(Alignment::Center)
        )
        .padding([7, 10])
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(panel)),
            ..Default::default()
        });

        let tx_row = container(row![
            text("Transaction").size(10).color(Color { a: 0.5, ..muted }),
            ghost_btn(p, "Begin").on_press_maybe(if can_tx_begin { Some(Message::BeginTransaction) } else { None }),
            ghost_btn(p, "Commit").on_press_maybe(if can_tx_end { Some(Message::CommitTransaction) } else { None }),
            ghost_btn(p, "Rollback").on_press_maybe(if can_tx_end { Some(Message::RollbackTransaction) } else { None }),
            if !self.tx_error.is_empty() {
                let e: Element<Message> = text(&self.tx_error).size(11).color(p.error).into();
                e
            } else { horizontal_space().width(0).into() },
        ].spacing(6).align_y(Alignment::Center))
        .padding([4, 10]);

        let editor_pane = container(
            column![
                container(Row::with_children(tabs_row).spacing(2).padding(iced::Padding { top: 6.0, right: 8.0, bottom: 0.0, left: 8.0 }))
                    .style(move |_| container::Style {
                        background: Some(iced::Background::Color(tab_bar_bg)),
                        ..Default::default()
                    }),
                container(editor).padding([0, 0]),
                ro_warn,
                params_row,
                toolbar,
                tx_row,
            ].spacing(0)
        )
        .style(move |_| container::Style {
            background: Some(iced::Background::Color(bg)),
            border: iced::Border { color: border, width: 1.0, radius: RADIUS.into() },
            ..Default::default()
        });

        let results_pane = container(self.view_results(p))
            .padding([12, 16])
            .width(Length::Fill)
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(bg)),
                ..Default::default()
            });

        column![
            container(editor_pane).padding(iced::Padding { top: 16.0, right: 16.0, bottom: 8.0, left: 16.0 }).width(Length::Fill),
            scrollable(results_pane).height(Length::Fill),
        ]
        .spacing(0)
        .height(Length::Fill)
        .into()
    }

    fn view_results(&self, p: Pal) -> Element<'_, Message> {
        let tab = &self.query_tabs[self.active_tab];
        let muted = p.muted; let text_col = p.text; let panel = p.panel; let border = p.border;
        let bg = p.bg; let row_alt = p.row_alt;

        if !self.batch_results.is_empty() {
            let mut col: Vec<Element<Message>> = vec![text(format!("{} statements", self.batch_results.len())).size(13).into()];
            for (i, (sql, res)) in self.batch_results.iter().enumerate() {
                let (color, label) = match res {
                    Ok(qr) => (p.success, format!("#{} OK - {} rows", i+1, qr.row_count)),
                    Err(e) => (p.error,   format!("#{} ERROR: {e}", i+1)),
                };
                col.push(text(label).size(12).color(color).into());
                col.push(text(sql).size(11).color(muted).font(iced::Font::MONOSPACE).into());
            }
            return Column::with_children(col).spacing(4).into();
        }

        if let Some(err) = &tab.result_error {
            let msg = format!("Error: {err}");
            return text(msg).size(13).color(p.error).into();
        }

        let Some(result) = &tab.last_result else {
            let hint = if self.active_config.is_some() {
                "Write a query above and press Execute (or Ctrl+Enter)"
            } else {
                "Connect to a database first, then run a query"
            };
            return column![
                text("No results yet").size(14).color(muted),
                text(hint).size(12).color(Color { a: 0.5, ..muted }),
            ].spacing(4).padding([24, 0]).into();
        };

        let slow     = result.duration_ms >= self.slow_threshold_ms as u128;
        let info_col = if slow { p.warning } else { p.success };
        let slow_lbl = if slow { " (SLOW)" } else { "" };
        let info     = text(format!("{} row(s) in {}ms{slow_lbl}", result.row_count, result.duration_ms)).size(13).color(info_col);

        let stats_banner: Element<Message> = if let Some(s) = &tab.col_stats {
            row![
                text(format!("Stats: {}  MIN:{}  MAX:{}{}  NULLs:{}/{}", s.col_name, s.min, s.max, s.avg.map(|a| format!("  AVG:{a:.4}")).unwrap_or_default(), s.null_count, s.total)).size(12).color(text_col),
                button(text("x").size(11)).padding([2,6]).on_press(Message::CloseColStats),
            ].spacing(8).align_y(Alignment::Center).into()
        } else { horizontal_space().width(0).into() };

        let max = self.max_display_rows;
        let display_rows: Vec<&Vec<String>> = tab.sorted_rows.as_ref().unwrap_or(&result.rows).iter().take(max).collect();
        let columns = &result.columns;

        let mut hcells: Vec<Element<Message>> = vec![
            container(text("#").size(11).color(muted)).width(36).padding([4, 6]).into()
        ];
        for (ci, col_name) in columns.iter().enumerate() {
            let arrow = match tab.sort_col { Some(sc) if sc == ci => if tab.sort_asc { " ↑" } else { " ↓" }, _ => "" };
            hcells.push(
                button(text(format!("{col_name}{arrow}")).size(12).color(muted))
                    .padding([4, 8])
                    .style(move |_, status| button::Style {
                        background: if matches!(status, button::Status::Hovered) {
                            Some(iced::Background::Color(Color { a: 0.5, ..panel }))
                        } else { None },
                        text_color: muted,
                        border: iced::Border { radius: 3.0.into(), ..Default::default() },
                        ..Default::default()
                    })
                    .on_press(Message::SortColumn(ci))
                    .into()
            );
        }
        // header background is slightly darker/lighter than panel for distinction
        let header_bg = Color {
            r: panel.r * if self.dark_theme { 0.75 } else { 0.94 },
            g: panel.g * if self.dark_theme { 0.75 } else { 0.94 },
            b: panel.b * if self.dark_theme { 0.75 } else { 0.94 },
            a: 1.0,
        };
        let header = container(Row::with_children(hcells).spacing(0))
            .width(Length::Fill)
            .style(move |_| container::Style {
                background: Some(iced::Background::Color(header_bg)),
                border: iced::Border { color: border, width: 0.0, radius: iced::border::Radius { top_left: 4.0, top_right: 4.0, bottom_right: 0.0, bottom_left: 0.0 } },
                ..Default::default()
            });

        let mut rows_col: Vec<Element<Message>> = vec![header.into(), horizontal_rule(1).into()];
        for (ri, row) in display_rows.iter().enumerate() {
            let row_bg = if ri % 2 == 0 { row_alt } else { bg };
            let mut cells: Vec<Element<Message>> = vec![
                container(text(format!("{}", ri+1)).size(11).color(muted)).width(36).padding([3, 6]).into()
            ];
            for cell in *row {
                let color = if cell == "NULL" { muted } else { text_col };
                cells.push(
                    button(text(cell).size(11).font(iced::Font::MONOSPACE).color(color))
                        .padding([3, 8])
                        .style(move |_, status| button::Style {
                            background: if matches!(status, button::Status::Hovered) {
                                Some(iced::Background::Color(Color { a: 0.6, ..panel }))
                            } else {
                                Some(iced::Background::Color(row_bg))
                            },
                            text_color: color,
                            border: iced::Border { radius: 0.0.into(), ..Default::default() },
                            ..Default::default()
                        })
                        .on_press(Message::CopyCell(cell.clone()))
                        .into()
                );
            }
            rows_col.push(
                container(Row::with_children(cells).spacing(0))
                    .style(move |_| container::Style { background: Some(iced::Background::Color(row_bg)), ..Default::default() })
                    .width(Length::Fill)
                    .into()
            );
        }

        if result.row_count > max {
            rows_col.push(
                container(text(format!("Showing {max} of {} rows — scroll to see more", result.row_count)).size(11).color(muted))
                    .padding([6, 8])
                    .into()
            );
        }

        let results_body = column![info, stats_banner, scrollable(Column::with_children(rows_col).spacing(0))].spacing(8);

        // Sub-tab toggle: show [Results] [Plan] when a plan is available
        if tab.plan_result.is_some() {
            let tab_btn = |label: &'static str, active: bool| -> Element<Message> {
                let bg = if active { panel } else { Color { a: 0.0, ..panel } };
                container(text(label).size(12).color(if active { text_col } else { muted }))
                    .padding([4, 12])
                    .style(move |_| container::Style {
                        background: Some(iced::Background::Color(bg)),
                        border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                        ..Default::default()
                    })
                    .into()
            };
            let subtabs = row![
                button(tab_btn("Results", !tab.show_plan))
                    .padding(0)
                    .style(|_, _| button::Style::default())
                    .on_press(Message::ShowPlanView(false)),
                button(tab_btn("Plan", tab.show_plan))
                    .padding(0)
                    .style(|_, _| button::Style::default())
                    .on_press(Message::ShowPlanView(true)),
            ].spacing(4);

            if tab.show_plan {
                column![subtabs, self.view_plan(p)].spacing(8).into()
            } else {
                column![subtabs, results_body].spacing(8).into()
            }
        } else {
            results_body.into()
        }
    }

    fn view_plan(&self, p: Pal) -> Element<'_, Message> {
        let tab = &self.query_tabs[self.active_tab];
        let Some(nodes) = &tab.plan_result else {
            return text("No plan data").size(13).color(p.muted).into();
        };

        let total_ms = nodes.first().map(|n| n.actual_total_ms).unwrap_or(1.0).max(0.001);
        const BAR_MAX: f32 = 260.0;

        let mut rows_col: Vec<Element<Message>> = vec![
            row![
                container(text("Node").size(11).color(p.muted)).width(Length::Fill),
                container(text("Excl. ms").size(11).color(p.muted)).width(80),
                container(text("Rows").size(11).color(p.muted)).width(60),
            ].spacing(8).into(),
            horizontal_rule(1).into(),
        ];

        for node in nodes {
            let pct = (node.exclusive_ms / total_ms * 100.0).min(100.0);
            let bar_w = (pct as f32 / 100.0 * BAR_MAX).max(2.0);

            // colour: red ≥50%, orange ≥25%, yellow ≥10%, teal <10%
            let bar_color = if pct >= 50.0 {
                Color { r: 0.87, g: 0.26, b: 0.21, a: 1.0 }
            } else if pct >= 25.0 {
                Color { r: 0.95, g: 0.55, b: 0.15, a: 1.0 }
            } else if pct >= 10.0 {
                Color { r: 0.93, g: 0.80, b: 0.20, a: 1.0 }
            } else {
                Color { r: 0.25, g: 0.70, b: 0.55, a: 1.0 }
            };

            let indent: f32 = node.depth as f32 * 14.0;
            let label = match &node.relation {
                Some(rel) => format!("{} on {rel}", node.node_type),
                None      => node.node_type.clone(),
            };

            let bar = container(horizontal_space())
                .width(bar_w)
                .height(14)
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(bar_color)),
                    border: iced::Border { radius: 2.0.into(), ..Default::default() },
                    ..Default::default()
                });

            let row_el: Element<Message> = row![
                horizontal_space().width(indent),
                column![
                    bar,
                    text(label).size(11).color(p.text).font(iced::Font::MONOSPACE),
                ].spacing(2).width(Length::Fill),
                container(
                    text(format!("{:.2}", node.exclusive_ms)).size(11).color(p.text).font(iced::Font::MONOSPACE)
                ).width(80),
                container(
                    text(format!("{}", node.actual_rows)).size(11).color(p.muted).font(iced::Font::MONOSPACE)
                ).width(60),
            ].spacing(8).align_y(Alignment::Center).into();

            rows_col.push(
                container(row_el)
                    .padding([4, 6])
                    .width(Length::Fill)
                    .into()
            );
        }

        let total_label = text(format!("Total execution: {:.3} ms", total_ms))
            .size(12).color(p.muted);

        column![
            scrollable(Column::with_children(rows_col).spacing(2)),
            total_label,
        ].spacing(8).into()
    }

    fn view_history(&self, p: Pal) -> Element<'_, Message> {
        let needle = self.history_search.to_lowercase();
        let entries: Vec<&HistoryEntry> = self.history_view.iter()
            .filter(|e| needle.is_empty() || e.query.to_lowercase().contains(&needle)).collect();

        let muted = p.muted; let text_col = p.text; let panel = p.panel; let border = p.border;

        // filter row: search fills, dates fixed at 130, count sits at the right
        let filter_row: Element<Message> = row![
            text_input("Search…", &self.history_search)
                .on_input(Message::HistorySearchChanged)
                .width(Length::Fill),
            text_input("From YYYY-MM-DD", &self.filter_from_str)
                .on_input(Message::FilterFromChanged)
                .width(130),
            text_input("To YYYY-MM-DD", &self.filter_to_str)
                .on_input(Message::FilterToChanged)
                .width(130),
            ghost_btn(p, "Filter").on_press(Message::HistoryFilter),
            ghost_btn(p, "Show All").on_press(Message::HistoryShowAll),
            horizontal_space(),
            text(format!("{} entries", entries.len())).size(11).color(muted),
        ].spacing(8).align_y(Alignment::Center).into();

        let mut col: Vec<Element<Message>> = vec![
            section_heading(p, "Query History"),
            filter_row,
        ];
        if !self.history_error.is_empty() { col.push(err_lbl(p, self.history_error.clone())); }

        for entry in &entries {
            let ts       = entry.timestamp.format("%Y-%m-%d %H:%M").to_string();
            let ts_color = match entry.row_count { Some(_) => p.success, None => p.error };
            let row_info = match entry.row_count { Some(n) => format!("{n} rows"), None => "ERROR".to_owned() };
            let q        = entry.query.clone();
            let preview  = entry.query.chars().take(120).collect::<String>();
            col.push(
                container(column![
                    row![
                        text(ts).size(11).color(muted),
                        horizontal_space(),
                        text(row_info).size(11).color(ts_color),
                    ].align_y(Alignment::Center),
                    text(preview).size(11).font(iced::Font::MONOSPACE).color(text_col),
                    ghost_btn(p, "Load into editor").on_press(Message::LoadHistoryQuery(q)),
                ].spacing(6))
                .padding([10, 14])
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(panel)),
                    border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                    ..Default::default()
                })
                .into()
            );
        }
        scrollable(Column::with_children(col).spacing(8).padding([PAGE_PAD, PAGE_PAD])).into()
    }

    fn view_snippets(&self, p: Pal) -> Element<'_, Message> {
        let can_save = !self.snippet_name_input.is_empty() && !self.query_tabs[self.active_tab].sql().trim().is_empty();
        let muted = p.muted; let text_col = p.text; let panel = p.panel; let border = p.border;
        let mut col: Vec<Element<Message>> = vec![
            section_heading(p, "Query Snippets"),
            row![
                text_input("Snippet name…", &self.snippet_name_input)
                    .on_input(Message::SnippetNameChanged)
                    .width(Length::Fill),
                action_btn(p, "Save from Editor", can_save)
                    .on_press_maybe(if can_save { Some(Message::SaveSnippet) } else { None }),
            ].spacing(8).align_y(Alignment::Center).into(),
        ];
        for (i, s) in self.snippet_store.snippets.iter().enumerate() {
            col.push(
                container(column![
                    text(&s.name).size(13).color(text_col),
                    text(&s.sql).size(11).font(iced::Font::MONOSPACE).color(muted),
                    row![
                        ghost_btn(p, "Insert into editor").on_press(Message::InsertSnippet(i)),
                        ghost_btn(p, "Delete").on_press(Message::DeleteSnippet(i)),
                    ].spacing(8),
                ].spacing(8))
                .padding([12, 14])
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(panel)),
                    border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                    ..Default::default()
                })
                .into()
            );
        }
        scrollable(Column::with_children(col).spacing(8).padding([PAGE_PAD, PAGE_PAD])).into()
    }

    fn view_pinboard(&self, p: Pal) -> Element<'_, Message> {
        let can_add = !self.pin_label_buf.trim().is_empty();
        let text_col = p.text; let panel = p.panel; let border = p.border;
        let mut col: Vec<Element<Message>> = vec![
            section_heading(p, "Variable Pinboard"),
            muted_txt(p, "Pinned values auto-fill into $param fields with matching names."),
            row![
                text_input("Pin name…", &self.pin_label_buf)
                    .on_input(Message::PinLabelChanged)
                    .width(Length::Fill),
                action_btn(p, "Add Pin", can_add)
                    .on_press_maybe(if can_add { Some(Message::AddPin) } else { None }),
            ].spacing(8).align_y(Alignment::Center).into(),
        ];
        for (i, pin) in self.pinboard.iter().enumerate() {
            col.push(
                container(row![
                    text(&pin.label).size(13).color(text_col).width(120),
                    text_input("value…", &pin.value)
                        .on_input(move |v| Message::PinValueChanged(i, v))
                        .width(Length::Fill),
                    ghost_btn(p, "Paste to param").on_press(Message::PastePinToParam(i)),
                    ghost_btn(p, "✕").on_press(Message::DeletePin(i)),
                ].spacing(8).align_y(Alignment::Center))
                .padding([8, 12])
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(panel)),
                    border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                    ..Default::default()
                })
                .into()
            );
        }
        scrollable(Column::with_children(col).spacing(8).padding([PAGE_PAD, PAGE_PAD])).into()
    }

    fn view_diff(&self, p: Pal) -> Element<'_, Message> {
        let names: Vec<String> = self.saved_results.iter().map(|s| s.name.clone()).collect();
        let left_name  = self.diff_left_idx.and_then(|i| names.get(i)).cloned();
        let right_name = self.diff_right_idx.and_then(|i| names.get(i)).cloned();
        let muted = p.muted; let text_col = p.text;

        let mut col: Vec<Element<Message>> = vec![
            section_heading(p, "Result Diff"),
            muted_txt(p, "Compare two saved snapshots row-by-row."),
            row![
                text("Left:").size(13),
                pick_list(names.clone(), left_name, { let names2 = names.clone(); move |name: String| { let i = names2.iter().position(|n| n == &name).unwrap_or(0); Message::DiffLeftSelected(i) } }).placeholder("-- select --").width(180),
                text("Right:").size(13),
                pick_list(names.clone(), right_name, { let names3 = names.clone(); move |name: String| { let i = names3.iter().position(|n| n == &name).unwrap_or(0); Message::DiffRightSelected(i) } }).placeholder("-- select --").width(180),
                button(text("Clear all").size(12)).padding([4,8]).on_press(Message::ClearSnapshots),
            ].spacing(10).align_y(Alignment::Center).into(),
            horizontal_rule(1).into(),
        ];

        let n  = self.saved_results.len();
        let li = self.diff_left_idx.filter(|&i| i < n);
        let ri = self.diff_right_idx.filter(|&i| i < n);
        if let (Some(li), Some(ri)) = (li, ri) {
            if li != ri {
                let left  = &self.saved_results[li];
                let right = &self.saved_results[ri];
                col.push(text(format!("{} ({} rows) vs {} ({} rows)", left.name, left.rows.len(), right.name, right.rows.len())).size(13).color(text_col).into());
                let columns = &left.columns;
                let max_rows = left.rows.len().max(right.rows.len()).min(self.max_display_rows);
                let mut added = 0; let mut removed = 0; let mut changed = 0;
                let mut hcells: Vec<Element<Message>> = vec![text("#").size(11).width(30).into()];
                for cn in columns { hcells.push(text(format!("L:{cn}")).size(11).color(p.success).into()); hcells.push(text(format!("R:{cn}")).size(11).color(p.warning).into()); }
                col.push(Row::with_children(hcells).spacing(6).into());
                for row_idx in 0..max_rows {
                    let lrow = left.rows.get(row_idx); let rrow = right.rows.get(row_idx);
                    match (lrow, rrow) { (Some(l), Some(r)) if l != r => changed += 1, (Some(_), None) => removed += 1, (None, Some(_)) => added += 1, _ => {} }
                    let mut cells: Vec<Element<Message>> = vec![text(format!("{}", row_idx+1)).size(11).color(muted).width(30).into()];
                    for ci in 0..columns.len() {
                        let lv = lrow.and_then(|r| r.get(ci)).map(String::as_str).unwrap_or("-");
                        let rv = rrow.and_then(|r| r.get(ci)).map(String::as_str).unwrap_or("-");
                        let diff = lv != rv;
                        cells.push(text(lv).size(11).font(iced::Font::MONOSPACE).color(if diff { p.error   } else { text_col }).into());
                        cells.push(text(rv).size(11).font(iced::Font::MONOSPACE).color(if diff { p.success } else { text_col }).into());
                    }
                    col.push(Row::with_children(cells).spacing(6).into());
                }
                col.push(row![text(format!("{removed} removed")).size(12).color(p.error), text(format!("{added} added")).size(12).color(p.success), text(format!("{changed} changed")).size(12).color(p.warning)].spacing(16).into());
            } else { col.push(muted_txt(p, "Select two different snapshots.")); }
        } else { col.push(muted_txt(p, "Select both snapshots above.")); }

        scrollable(Column::with_children(col).spacing(8).padding(24)).into()
    }

    fn view_automation(&self, p: Pal) -> Element<'_, Message> {
        const KINDS: &[&str] = &["Every N seconds","Hourly","Daily","Weekly","Monthly"];
        const DAYS:  &[&str] = &["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];

        let muted = p.muted; let text_col = p.text;

        let kind_pick = pick_list(
            KINDS.to_vec(),
            Some(KINDS[self.schedule_builder.kind]),
            |name: &str| {
                let i = KINDS.iter().position(|k| *k == name).unwrap_or(0);
                Message::SchedKindChanged(i)
            }
        ).width(160);

        let sched_fields: Element<Message> = match self.schedule_builder.kind {
            0 => row![
                lbl("Seconds:"),
                text_input("60", &self.schedule_builder.secs).on_input(Message::SchedSecsChanged).width(80),
            ].spacing(8).align_y(Alignment::Center).into(),
            1 => row![
                lbl("Minute:"),
                text_input("0", &self.schedule_builder.minute).on_input(Message::SchedMinuteChanged).width(50),
            ].spacing(8).align_y(Alignment::Center).into(),
            2 => row![
                lbl("HH:"),
                text_input("9", &self.schedule_builder.hour).on_input(Message::SchedHourChanged).width(40),
                lbl("MM:"),
                text_input("0", &self.schedule_builder.minute).on_input(Message::SchedMinuteChanged).width(40),
            ].spacing(8).align_y(Alignment::Center).into(),
            3 => row![
                pick_list(DAYS.to_vec(), Some(DAYS[self.schedule_builder.weekday]), |d: &str| {
                    let i = DAYS.iter().position(|x| *x == d).unwrap_or(0);
                    Message::SchedWeekdayChanged(i)
                }).width(80),
                lbl("HH:"),
                text_input("9", &self.schedule_builder.hour).on_input(Message::SchedHourChanged).width(40),
                lbl("MM:"),
                text_input("0", &self.schedule_builder.minute).on_input(Message::SchedMinuteChanged).width(40),
            ].spacing(8).align_y(Alignment::Center).into(),
            4 => row![
                lbl("Day 1-28:"),
                text_input("1", &self.schedule_builder.day_of_month).on_input(Message::SchedDomChanged).width(40),
                lbl("HH:"),
                text_input("9", &self.schedule_builder.hour).on_input(Message::SchedHourChanged).width(40),
                lbl("MM:"),
                text_input("0", &self.schedule_builder.minute).on_input(Message::SchedMinuteChanged).width(40),
            ].spacing(8).align_y(Alignment::Center).into(),
            _ => horizontal_space().width(0).into(),
        };

        let valid   = self.schedule_builder.build().is_some();
        let can_add = !self.auto_job_label_buf.trim().is_empty()
            && !self.auto_job_sql_buf.trim().is_empty()
            && valid;

        let add_form = column![
            section_heading(p, "New Automation Job"),
            row![lbl("Schedule:"), kind_pick].spacing(8).align_y(Alignment::Center),
            sched_fields,
            row![
                lbl("Label:"),
                text_input("", &self.auto_job_label_buf)
                    .on_input(Message::AutoLabelChanged)
                    .width(Length::Fill),
            ].spacing(8).align_y(Alignment::Center),
            row![
                lbl("SQL:"),
                text_input("", &self.auto_job_sql_buf)
                    .on_input(Message::AutoSqlChanged)
                    .width(Length::Fill),
            ].spacing(8).align_y(Alignment::Center),
            row![
                ghost_btn(p, "Add Job")
                    .on_press_maybe(if can_add { Some(Message::AddAutoJob) } else { None }),
                if !valid { err_lbl(p, "Invalid schedule values.") } else { horizontal_space().width(0).into() },
            ].spacing(8).align_y(Alignment::Center),
        ].spacing(12);

        let pin_section = column![
            horizontal_rule(1),
            section_heading(p, "Toolbar Queries"),
            row![
                text_input("Label", &self.toolbar_label_buf)
                    .on_input(Message::ToolbarLabelChanged)
                    .width(Length::Fill),
                ghost_btn(p, "Pin editor to toolbar")
                    .on_press_maybe(
                        if !self.query_tabs[self.active_tab].sql().trim().is_empty() {
                            Some(Message::PinToToolbar)
                        } else { None }
                    ),
            ].spacing(8).align_y(Alignment::Center),
        ].spacing(12);

        let mut jobs_col: Vec<Element<Message>> = vec![
            horizontal_rule(1).into(),
            section_heading(p, "Scheduled Jobs"),
        ];
        for (i, job) in self.auto_jobs.iter().enumerate() {
            jobs_col.push(
                container(row![
                    text(&job.label).size(13).color(text_col).width(150),
                    text(job.schedule.label()).size(12).color(text_col).width(150),
                    text(
                        job.last_run
                            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "never".into())
                    ).size(11).color(muted).width(160),
                    checkbox("", job.enabled).on_toggle(move |b| Message::ToggleAutoJob(i, b)),
                    ghost_btn(p, "Run now").on_press(Message::FireAutoJobNow(i)),
                    ghost_btn(p, "✕").on_press(Message::RemoveAutoJob(i)),
                ].spacing(8).align_y(Alignment::Center))
                .padding([8, 0])
                .into()
            );
        }

        scrollable(
            column![add_form, pin_section, Column::with_children(jobs_col).spacing(8)]
                .spacing(16)
                .padding([PAGE_PAD, PAGE_PAD])
        ).into()
    }

    fn view_shortcuts(&self, p: Pal) -> Element<'_, Message> {
        let shortcuts: &[(&str, &str)] = &[
            ("Ctrl+Enter",          "Execute query"),
            ("Click column header", "Sort by column"),
            ("Click result cell",   "Copy cell value"),
            ("Snapshot button",     "Save result for diff"),
            ("Pin to toolbar",      "One-click quick-run buttons"),
        ];
        let text_col = p.text; let panel = p.panel; let border = p.border; let muted = p.muted;
        let mut col: Vec<Element<Message>> = vec![section_heading(p, "Keyboard Shortcuts")];
        for (key, desc) in shortcuts {
            col.push(
                container(row![
                    container(text(*key).size(12).font(iced::Font::MONOSPACE).color(muted))
                        .width(200),
                    text(*desc).size(13).color(text_col),
                ].spacing(16).align_y(Alignment::Center))
                .padding([8, 14])
                .style(move |_| container::Style {
                    background: Some(iced::Background::Color(panel)),
                    border: iced::Border { color: border, width: 1.0, radius: 4.0.into() },
                    ..Default::default()
                })
                .into()
            );
        }
        scrollable(Column::with_children(col).spacing(8).padding([PAGE_PAD, PAGE_PAD])).into()
    }
}
