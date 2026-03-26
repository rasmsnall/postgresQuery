# postgresQuery

## Overview

`postgresQuery` is a lightweight utility for constructing and executing PostgreSQL queries in a structured and safe manner. The project aims to improve code readability and maintainability while preserving the flexibility of raw SQL.

The library emphasizes parameterized query construction to reduce the risk of SQL injection and to align with best practices in database interaction.

---

## Features

- Structured query construction using template literals  
- Automatic parameterization of query inputs  
- Minimal abstraction over raw SQL  
- Lightweight design with minimal dependencies  
- Compatibility with standard PostgreSQL clients  

---

## Installation

Clone the repository:

```bash
git clone https://github.com/rasmsnall/postgresQuery.git
cd postgresQuery
```

Alternatively, integrate the source directly into your project depending on your build system and requirements.

---

## Usage

### Basic Query

```javascript
const query = postgresQuery`
  SELECT * FROM users
  WHERE id = ${userId}
`
```

### Insert Example

```javascript
const query = postgresQuery`
  INSERT INTO users (name, email)
  VALUES (${name}, ${email})
`
```

### Update Example

```javascript
const query = postgresQuery`
  UPDATE users
  SET name = ${name}
  WHERE id = ${id}
`
```

### Execution

```javascript
const result = await db.query(query)
```

---

## Parameterization

The library transforms template literal interpolations into parameterized queries. Instead of embedding values directly into SQL strings, placeholders are generated and values are passed separately to the database driver.

This approach improves security by mitigating SQL injection risks and ensures consistent query behavior.

---

## Project Structure

```
postgresQuery/
├── src/
│   └── index.js
├── tests/
├── package.json
└── README.md
```

---

## Testing

Run the test suite using:

```bash
npm test
```

---

## Contributing

Contributions are accepted through standard Git workflows:

1. Fork the repository  
2. Create a new branch for your changes  
3. Commit your modifications  
4. Submit a pull request for review  

---

## License

This project is distributed under the MIT License.

---

## Notes

The design of `postgresQuery` is informed by established practices in PostgreSQL client libraries, particularly the use of parameterized queries and template-based query construction.
