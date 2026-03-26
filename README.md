# PostgreSQL Query Launcher

## Overview

PostgreSQL Query Launcher is a desktop application written in Rust that provides a graphical interface for interacting with PostgreSQL databases. The application is designed to streamline query execution, connection management, and query organization within a single environment.

It integrates database connectivity, query editing, history tracking, and result handling into a cohesive user interface built using the `iced` GUI framework.

---

## Features

- Graphical user interface for executing PostgreSQL queries  
- Persistent connection profiles with secure credential storage  
- Query history tracking using a local SQLite database  
- SQL syntax highlighting within the editor  
- Schema inspection and caching  
- Snippet management for reusable SQL fragments  
- Export functionality for query results (e.g., spreadsheet formats)  
- Query formatting support  
- Detection of slow queries based on execution time thresholds  

---

## Architecture

The application is organized into modular components, each responsible for a specific aspect of functionality:
src/
├── main.rs # Application entry point and window configuration
├── app.rs # Core application state, update logic, and UI rendering
├── db.rs # PostgreSQL connection handling and query execution
├── schema.rs # Schema fetching and caching
├── history.rs # Query history persistence (SQLite via rusqlite)
├── profiles.rs # Connection profile management and credential storage
├── recent.rs # Recently used connections
├── snippets.rs # SQL snippet storage and retrieval
├── highlighter.rs # SQL syntax highlighting
└── utils.rs # Utility functions

---

## Dependencies

Key dependencies include:

- `iced` for the graphical user interface  
- `tokio` and `tokio-postgres` for asynchronous database communication  
- `rusqlite` for local persistence of query history  
- `native-tls` and `postgres-native-tls` for secure connections  
- `serde` and `serde_json` for serialization  
- `rust_xlsxwriter` for exporting query results  
- `sqlformat` for SQL formatting  
- `keyring` for secure password storage  

---

## Installation

### Prerequisites

- Rust (edition 2021 or later)  
- Cargo (Rust package manager)  

### Build

```bash
git clone https://github.com/rasmsnall/postgresQuery.git
cd postgresQuery
cargo build --release
cargo run
```
