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


## Architecture

The application is organized into modular components, each responsible for a specific aspect of functionality:

```text
src/
├── main.rs         # Application entry point and window configuration
├── app.rs          # Core application state, update logic, and UI rendering
├── db.rs           # PostgreSQL connection handling and query execution
├── schema.rs       # Schema fetching and caching
├── history.rs      # Query history persistence (SQLite via rusqlite)
├── profiles.rs     # Connection profile management and credential storage
├── recent.rs       # Recently used connections
├── snippets.rs     # SQL snippet storage and retrieval
├── highlighter.rs  # SQL syntax highlighting
└── utils.rs        # Utility functions
```
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

## Usage
1. Launch the application
2. Create or select a connection profile
3. Enter SQL queries in the editor
4. Execute queries and view results
5. Save frequently used queries as snippets
6. Review previous queries via the history panel

## Data Storage
- Query history is stored locally using SQLite (query_history.db)
- Connection profiles are persisted and may use system keyring services for secure password storage
- Schema information may be cached to improve performance

## Security Considerations
- Credentials are handled using the system keyring when available
- Sensitive data structures utilize memory-zeroing techniques where applicable
- TLS is supported for secure database connections

## Performance Notes
- Queries exceeding a predefined threshold (e.g., 1000 ms) are flagged as slow
- Schema caching reduces repeated metadata queries
- Asynchronous execution ensures responsive UI behavior

## Contributing

```
-> Fork the repository
-> Create a feature branch
-> Commit your changes
-> Submit a pull request
```
## License

This project is licensed under the MIT License.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Remarks

The project combines systems programming in Rust with modern GUI development to provide a self-contained PostgreSQL client. Its modular structure supports extensibility while maintaining separation of concerns across database interaction, UI rendering, and local persistence.

