use crate::db::SchemaTable;

pub struct SchemaCache {
    pub tables: Vec<SchemaTable>,
    pub loaded: bool,
    pub error:  Option<String>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self { tables: vec![], loaded: false, error: None }
    }

    pub fn clear(&mut self) {
        self.tables.clear();
        self.loaded = false;
        self.error = None;
    }
}
