use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, NoTls};
use zeroize::Zeroize;

// ---------------------------------------------------------------------------
// ConnConfig — password is zeroized when dropped
// ---------------------------------------------------------------------------
#[derive(Clone)]
pub struct ConnConfig {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
    pub use_tls: bool,
}

impl ConnConfig {
    // builds the connection string; values are quoted to handle spaces/special chars.
    // kept private so callers can't accidentally log it.
    fn build_conn_string(&self) -> ZeroizingString {
        ZeroizingString(format!(
            "host='{}' port={} dbname='{}' user='{}' password='{}'",
            self.host.replace('\'', "\\'"),
            self.port,
            self.dbname.replace('\'', "\\'"),
            self.user.replace('\'', "\\'"),
            self.password.replace('\'', "\\'"),
        ))
    }
}

impl Drop for ConnConfig {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

// a String that zeroizes its contents on drop
struct ZeroizingString(String);

impl Drop for ZeroizingString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl std::ops::Deref for ZeroizingString {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// strip anything that looks like a connection string from an error message
// to prevent password leakage in displayed errors
// ---------------------------------------------------------------------------
pub fn sanitize_db_error(e: &str) -> String {
    // tokio-postgres errors sometimes echo the connection string; redact all
    // "password=..." or "password='...'" tokens regardless of how many appear.
    let mut s = e.to_owned();
    loop {
        let lower = s.to_lowercase();
        let Some(start) = lower.find("password=") else {
            break;
        };
        let after = start + 9; // skip "password="
                               // handle both quoted ('value') and unquoted (value ) forms
        let end = if s.get(after..after + 1) == Some("'") {
            s[after + 1..]
                .find('\'')
                .map(|i| after + 1 + i + 1)
                .unwrap_or(s.len())
        } else {
            s[after..]
                .find(|c: char| c.is_whitespace())
                .map(|i| after + i)
                .unwrap_or(s.len())
        };
        s.replace_range(start..end, "password=<redacted>");
    }
    s
}

// ---------------------------------------------------------------------------
// Query / schema results
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub duration_ms: u128,
}

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct SchemaTable {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub columns: Vec<SchemaColumn>,
}

// ---------------------------------------------------------------------------
// PgHandle
// ---------------------------------------------------------------------------
pub struct PgHandle {
    pub client: Client,
    rt: tokio::runtime::Runtime,
}

impl PgHandle {
    pub fn connect_sync(cfg: &ConnConfig) -> Result<Self, String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        let conn_str = cfg.build_conn_string(); // zeroized on drop

        let client = if cfg.use_tls {
            let tls_connector = TlsConnector::new().map_err(|e| format!("TLS init error: {e}"))?;
            let connector = MakeTlsConnector::new(tls_connector);
            rt.block_on(async {
                let (client, connection) = tokio_postgres::connect(&*conn_str, connector)
                    .await
                    .map_err(|e| sanitize_db_error(&e.to_string()))?;
                tokio::spawn(connection);
                Ok::<Client, String>(client)
            })?
        } else {
            rt.block_on(async {
                let (client, connection) = tokio_postgres::connect(&*conn_str, NoTls)
                    .await
                    .map_err(|e| sanitize_db_error(&e.to_string()))?;
                tokio::spawn(connection);
                Ok::<Client, String>(client)
            })?
        };
        // conn_str is dropped (zeroized) here before we return
        Ok(Self { client, rt })
    }

    pub fn begin_sync(&mut self) -> Result<(), String> {
        self.rt.block_on(async {
            self.client
                .execute("BEGIN", &[])
                .await
                .map(|_| ())
                .map_err(|e| sanitize_db_error(&e.to_string()))
        })
    }

    pub fn commit_sync(&mut self) -> Result<(), String> {
        self.rt.block_on(async {
            self.client
                .execute("COMMIT", &[])
                .await
                .map(|_| ())
                .map_err(|e| sanitize_db_error(&e.to_string()))
        })
    }

    pub fn rollback_sync(&mut self) -> Result<(), String> {
        self.rt.block_on(async {
            self.client
                .execute("ROLLBACK", &[])
                .await
                .map(|_| ())
                .map_err(|e| sanitize_db_error(&e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------
pub fn query_sync(handle: &PgHandle, sql: &str) -> Result<QueryResult, String> {
    handle.rt.block_on(async {
        let client = &handle.client;
        let start = std::time::Instant::now();
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| sanitize_db_error(&e.to_string()))?;
        let duration_ms = start.elapsed().as_millis();

        let columns: Vec<String> = if let Some(first) = rows.first() {
            first
                .columns()
                .iter()
                .map(|c| c.name().to_owned())
                .collect()
        } else {
            vec![]
        };

        let stringified: Vec<Vec<String>> = rows
            .iter()
            .map(|row| (0..row.len()).map(|i| cell_to_string(row, i)).collect())
            .collect();

        let row_count = stringified.len();
        Ok(QueryResult {
            columns,
            rows: stringified,
            row_count,
            duration_ms,
        })
    })
}

// ---------------------------------------------------------------------------
// Schema fetch
// ---------------------------------------------------------------------------
pub fn fetch_schema_sync(handle: &PgHandle) -> Result<Vec<SchemaTable>, String> {
    handle.rt.block_on(async {
        let client = &handle.client;
        let table_rows = client
            .query(
                "SELECT table_schema, table_name, table_type \
                 FROM information_schema.tables \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await
            .map_err(|e| sanitize_db_error(&e.to_string()))?;

        let mut tables: Vec<SchemaTable> = table_rows
            .iter()
            .map(|r| SchemaTable {
                schema: r.get::<_, String>(0),
                name: r.get::<_, String>(1),
                kind: r.get::<_, String>(2),
                columns: vec![],
            })
            .collect();

        let col_rows = client
            .query(
                "SELECT table_schema, table_name, column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name, ordinal_position",
                &[],
            )
            .await
            .map_err(|e| sanitize_db_error(&e.to_string()))?;

        for r in &col_rows {
            let tschema: String = r.get(0);
            let tname: String = r.get(1);
            let cname: String = r.get(2);
            let dtype: String = r.get(3);
            let nullable: String = r.get(4);

            if let Some(t) = tables
                .iter_mut()
                .find(|t| t.schema == tschema && t.name == tname)
            {
                t.columns.push(SchemaColumn {
                    name: cname,
                    data_type: dtype,
                    is_nullable: nullable == "YES",
                });
            }
        }

        Ok(tables)
    })
}

// ---------------------------------------------------------------------------
// Query plan (flamegraph data)
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub node_type:      String,
    pub relation:       Option<String>,
    pub actual_total_ms: f64,  // loops-adjusted total time (ms)
    pub actual_rows:    i64,
    #[allow(dead_code)]
    pub loops:          i64,
    pub depth:          usize,
    pub exclusive_ms:   f64,   // time not attributed to children
}

pub fn explain_plan_sync(handle: &PgHandle, sql: &str) -> Result<Vec<PlanNode>, String> {
    let explain_sql = format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}");
    handle.rt.block_on(async {
        let client = &handle.client;
        let rows = client
            .query(&explain_sql, &[])
            .await
            .map_err(|e| sanitize_db_error(&e.to_string()))?;
        let row = rows.first().ok_or_else(|| "No plan data returned".to_string())?;
        let json_val: serde_json::Value = row
            .try_get(0)
            .map_err(|e| format!("Could not read plan JSON: {e}"))?;
        let plan = &json_val[0]["Plan"];
        if plan.is_null() {
            return Err("Unexpected plan structure".to_string());
        }
        let mut nodes = Vec::new();
        flatten_plan(plan, 0, &mut nodes);
        Ok(nodes)
    })
}

fn flatten_plan(node: &serde_json::Value, depth: usize, out: &mut Vec<PlanNode>) {
    let node_type    = node["Node Type"].as_str().unwrap_or("Unknown").to_owned();
    let relation     = node["Relation Name"].as_str().map(|s| s.to_owned());
    let actual_ms    = node["Actual Total Time"].as_f64().unwrap_or(0.0);
    let actual_rows  = node["Actual Rows"].as_i64().unwrap_or(0);
    let loops        = node["Loops"].as_i64().unwrap_or(1);
    let total_ms     = actual_ms * loops as f64;

    let children_ms: f64 = node["Plans"]
        .as_array()
        .map(|cs| {
            cs.iter()
                .map(|c| {
                    c["Actual Total Time"].as_f64().unwrap_or(0.0)
                        * c["Loops"].as_i64().unwrap_or(1) as f64
                })
                .sum()
        })
        .unwrap_or(0.0);

    let exclusive_ms = (total_ms - children_ms).max(0.0);

    out.push(PlanNode { node_type, relation, actual_total_ms: total_ms, actual_rows, loops, depth, exclusive_ms });

    if let Some(children) = node["Plans"].as_array() {
        for child in children {
            flatten_plan(child, depth + 1, out);
        }
    }
}

// ---------------------------------------------------------------------------
// Cell serialisation
// ---------------------------------------------------------------------------
fn cell_to_string(row: &tokio_postgres::Row, idx: usize) -> String {
    use tokio_postgres::types::Type;
    let col_type = row.columns()[idx].type_();

    macro_rules! try_get {
        ($T:ty) => {
            if let Ok(v) = row.try_get::<_, Option<$T>>(idx) {
                return v
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_owned());
            }
        };
    }

    match col_type {
        &Type::BOOL => {
            try_get!(bool);
        }
        &Type::INT2 => {
            try_get!(i16);
        }
        &Type::INT4 => {
            try_get!(i32);
        }
        &Type::INT8 => {
            try_get!(i64);
        }
        &Type::FLOAT4 => {
            try_get!(f32);
        }
        &Type::FLOAT8 => {
            try_get!(f64);
        }
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME => {
            try_get!(String);
        }
        _ => {
            if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
                return v.unwrap_or_else(|| "NULL".to_owned());
            }
        }
    }
    format!("<{}>", col_type.name())
}
