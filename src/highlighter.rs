use iced::advanced::text::highlighter::{self, Highlighter as HighlighterTrait};
use iced::Color;
use std::ops::Range;

// sql keywords grouped by category
const KW_STATEMENT: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "EXPLAIN",
    "ANALYZE", "VACUUM", "REINDEX", "GRANT", "REVOKE", "WITH", "AS",
];
const KW_CLAUSE: &[&str] = &[
    "FROM",
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CROSS",
    "ON",
    "USING",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "INTERSECT",
    "EXCEPT",
    "INTO",
    "VALUES",
    "SET",
    "RETURNING",
    "DISTINCT",
    "LATERAL",
    "PARTITION",
    "OVER",
    "WINDOW",
    "ROWS",
    "RANGE",
    "BETWEEN",
    "AND",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "ROW",
];
const KW_CONTROL: &[&str] = &[
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IF",
    "DO",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "FOR",
    "LOOP",
    "IN",
    "FOREACH",
    "EXIT",
    "CONTINUE",
    "RETURN",
    "RAISE",
    "NOTICE",
    "EXCEPTION",
    "DECLARE",
    "LANGUAGE",
    "FUNCTION",
    "PROCEDURE",
    "TRIGGER",
    "OR",
    "NOT",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",
    "LIKE",
    "ILIKE",
    "SIMILAR",
    "EXISTS",
    "ANY",
    "SOME",
    "CAST",
    "COALESCE",
    "NULLIF",
    "GREATEST",
    "LEAST",
];
const KW_TYPE: &[&str] = &[
    "INTEGER",
    "INT",
    "BIGINT",
    "SMALLINT",
    "SERIAL",
    "BIGSERIAL",
    "TEXT",
    "VARCHAR",
    "CHAR",
    "BOOLEAN",
    "BOOL",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "PRECISION",
    "NUMERIC",
    "DECIMAL",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "INTERVAL",
    "UUID",
    "JSON",
    "JSONB",
    "BYTEA",
    "OID",
    "VOID",
    "TABLE",
    "INDEX",
    "VIEW",
    "SCHEMA",
    "DATABASE",
    "SEQUENCE",
    "EXTENSION",
    "UNIQUE",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "DEFAULT",
    "CONSTRAINT",
    "CHECK",
    "NOT NULL",
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TokenKind {
    Statement,   // SELECT, INSERT … — bright accent
    Clause,      // FROM, WHERE, JOIN … — blue-ish
    Control,     // CASE, WHEN, FOR, LOOP … — purple-ish
    Type,        // data types, DDL keywords — teal
    Number,      // numeric literals
    String,      // string literals
    Comment,     // -- comments
    Operator,    // = <> >= <= + - * / :: etc
    Punctuation, // ( ) , ; .
    Plain,
}

/// Returned per-span by the highlighter
#[derive(Debug, Clone, Copy)]
pub struct SqlHighlight(pub TokenKind);

impl SqlHighlight {
    pub fn color_for_dark(&self) -> Color {
        match self.0 {
            TokenKind::Statement => Color {
                r: 0.612,
                g: 0.769,
                b: 1.000,
                a: 1.0,
            }, // sky blue
            TokenKind::Clause => Color {
                r: 0.498,
                g: 0.863,
                b: 0.996,
                a: 1.0,
            }, // cyan
            TokenKind::Control => Color {
                r: 0.816,
                g: 0.635,
                b: 1.000,
                a: 1.0,
            }, // lavender
            TokenKind::Type => Color {
                r: 0.447,
                g: 0.878,
                b: 0.761,
                a: 1.0,
            }, // teal
            TokenKind::Number => Color {
                r: 0.996,
                g: 0.800,
                b: 0.510,
                a: 1.0,
            }, // amber
            TokenKind::String => Color {
                r: 0.671,
                g: 0.878,
                b: 0.557,
                a: 1.0,
            }, // sage green
            TokenKind::Comment => Color {
                r: 0.502,
                g: 0.502,
                b: 0.502,
                a: 1.0,
            }, // gray
            TokenKind::Operator => Color {
                r: 0.949,
                g: 0.600,
                b: 0.541,
                a: 1.0,
            }, // salmon
            TokenKind::Punctuation => Color {
                r: 0.650,
                g: 0.650,
                b: 0.650,
                a: 1.0,
            },
            TokenKind::Plain => Color {
                r: 0.878,
                g: 0.878,
                b: 0.878,
                a: 1.0,
            },
        }
    }

    pub fn color_for_light(&self) -> Color {
        match self.0 {
            TokenKind::Statement => Color {
                r: 0.059,
                g: 0.373,
                b: 0.800,
                a: 1.0,
            }, // deep blue
            TokenKind::Clause => Color {
                r: 0.000,
                g: 0.549,
                b: 0.749,
                a: 1.0,
            }, // teal-blue
            TokenKind::Control => Color {
                r: 0.490,
                g: 0.196,
                b: 0.800,
                a: 1.0,
            }, // purple
            TokenKind::Type => Color {
                r: 0.063,
                g: 0.549,
                b: 0.447,
                a: 1.0,
            }, // green-teal
            TokenKind::Number => Color {
                r: 0.694,
                g: 0.400,
                b: 0.000,
                a: 1.0,
            }, // amber
            TokenKind::String => Color {
                r: 0.200,
                g: 0.533,
                b: 0.200,
                a: 1.0,
            }, // green
            TokenKind::Comment => Color {
                r: 0.467,
                g: 0.467,
                b: 0.467,
                a: 1.0,
            }, // gray
            TokenKind::Operator => Color {
                r: 0.733,
                g: 0.133,
                b: 0.133,
                a: 1.0,
            }, // red
            TokenKind::Punctuation => Color {
                r: 0.400,
                g: 0.400,
                b: 0.400,
                a: 1.0,
            },
            TokenKind::Plain => Color {
                r: 0.098,
                g: 0.098,
                b: 0.102,
                a: 1.0,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------
fn tokenize_line(line: &str) -> Vec<(Range<usize>, TokenKind)> {
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut spans: Vec<(Range<usize>, TokenKind)> = Vec::new();
    let mut i = 0;

    while i < len {
        // -- comment: rest of line
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            spans.push((i..len, TokenKind::Comment));
            break;
        }

        // string literal: single-quoted
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        i += 1;
                    }
                    // escaped ''
                    else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            spans.push((start..i, TokenKind::String));
            continue;
        }

        // dollar-quoted string: $$…$$
        if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'$' {
            let start = i;
            i += 2;
            while i + 1 < len {
                if bytes[i] == b'$' && bytes[i + 1] == b'$' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            spans.push((start..i, TokenKind::String));
            continue;
        }

        // number (integer or float)
        if bytes[i].is_ascii_digit()
            || (bytes[i] == b'.' && i + 1 < len && bytes[i + 1].is_ascii_digit())
        {
            let start = i;
            while i < len
                && (bytes[i].is_ascii_digit()
                    || bytes[i] == b'.'
                    || bytes[i] == b'e'
                    || bytes[i] == b'E'
                    || bytes[i] == b'_')
            {
                i += 1;
            }
            spans.push((start..i, TokenKind::Number));
            continue;
        }

        // identifier / keyword
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word: &str = &line[start..i];
            let upper = word.to_uppercase();
            let kind = if KW_STATEMENT.contains(&upper.as_str()) {
                TokenKind::Statement
            } else if KW_CLAUSE.contains(&upper.as_str()) {
                TokenKind::Clause
            } else if KW_CONTROL.contains(&upper.as_str()) {
                TokenKind::Control
            } else if KW_TYPE.contains(&upper.as_str()) {
                TokenKind::Type
            } else {
                TokenKind::Plain
            };
            spans.push((start..i, kind));
            continue;
        }

        // operators: = <> >= <= != :: -> ->> + - * / % ^ | & ~
        if matches!(
            bytes[i],
            b'=' | b'<'
                | b'>'
                | b'!'
                | b':'
                | b'+'
                | b'-'
                | b'*'
                | b'/'
                | b'%'
                | b'^'
                | b'|'
                | b'&'
                | b'~'
        ) {
            let start = i;
            i += 1;
            // multi-char operators
            if i < len
                && matches!(
                    (bytes[start], bytes[i]),
                    (b'<', b'>')
                        | (b'>', b'=')
                        | (b'<', b'=')
                        | (b'!', b'=')
                        | (b':', b':')
                        | (b'-', b'>')
                        | (b'|', b'|')
                )
            {
                i += 1;
                // ->>
                if start + 1 < len
                    && bytes[start] == b'-'
                    && bytes[start + 1] == b'>'
                    && i < len
                    && bytes[i] == b'>'
                {
                    i += 1;
                }
            }
            spans.push((start..i, TokenKind::Operator));
            continue;
        }

        // punctuation: ( ) , ; . [ ]
        if matches!(bytes[i], b'(' | b')' | b',' | b';' | b'.' | b'[' | b']') {
            spans.push((i..i + 1, TokenKind::Punctuation));
            i += 1;
            continue;
        }

        // whitespace and anything else — skip without emitting a span
        i += 1;
    }

    spans
}

// ---------------------------------------------------------------------------
// Highlighter impl
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq)]
pub struct SqlSettings {
    pub dark_theme: bool,
}

pub struct SqlHighlighter {
    settings: SqlSettings,
    current_line: usize,
}

pub struct LineIter(Vec<(Range<usize>, SqlHighlight)>, usize);

impl Iterator for LineIter {
    type Item = (Range<usize>, SqlHighlight);
    fn next(&mut self) -> Option<Self::Item> {
        if self.1 < self.0.len() {
            let item = self.0[self.1].clone();
            self.1 += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl HighlighterTrait for SqlHighlighter {
    type Settings = SqlSettings;
    type Highlight = SqlHighlight;
    type Iterator<'a> = LineIter;

    fn new(settings: &Self::Settings) -> Self {
        Self {
            settings: settings.clone(),
            current_line: 0,
        }
    }

    fn update(&mut self, new_settings: &Self::Settings) {
        if *new_settings != self.settings {
            self.settings = new_settings.clone();
            self.current_line = 0;
        }
    }

    fn change_line(&mut self, line: usize) {
        self.current_line = line;
    }

    fn highlight_line(&mut self, line: &str) -> Self::Iterator<'_> {
        let tokens = tokenize_line(line);
        let spans = tokens
            .into_iter()
            .map(|(r, k)| (r, SqlHighlight(k)))
            .collect();
        self.current_line += 1;
        LineIter(spans, 0)
    }

    fn current_line(&self) -> usize {
        self.current_line
    }
}

/// Convert a SqlHighlight to an iced Format for use in highlight_with.
pub fn to_format(h: &SqlHighlight, theme: &iced::Theme) -> highlighter::Format<iced::Font> {
    let dark = matches!(
        theme,
        iced::Theme::Dark
            | iced::Theme::KanagawaDragon
            | iced::Theme::Dracula
            | iced::Theme::Nord
            | iced::Theme::SolarizedDark
            | iced::Theme::GruvboxDark
            | iced::Theme::Oxocarbon
            | iced::Theme::Ferra
            | iced::Theme::Moonfly
            | iced::Theme::Nightfly
            | iced::Theme::TokyoNight
            | iced::Theme::TokyoNightStorm
            | iced::Theme::CatppuccinMocha
            | iced::Theme::CatppuccinMacchiato
            | iced::Theme::CatppuccinFrappe
    );
    let color = if dark {
        h.color_for_dark()
    } else {
        h.color_for_light()
    };
    highlighter::Format {
        color: Some(color),
        font: Some(iced::Font::MONOSPACE),
    }
}
