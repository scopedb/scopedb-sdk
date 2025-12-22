// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::ops::Range;

use exn::Result;
use logos::Lexer;
use logos::Logos;

pub use self::TokenKind::*;
use crate::error::Error;

#[derive(Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub source: &'a str,
    pub kind: TokenKind,
    pub span: Range<usize>,
}

impl<'a> Token<'a> {
    pub fn new_eoi(source: &'a str) -> Self {
        Token {
            source,
            kind: EOI,
            span: source.len()..source.len(),
        }
    }
}

impl fmt::Debug for Token<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}({:?})", self.kind, self.span)
    }
}

pub struct Tokenizer<'a> {
    source: &'a str,
    lexer: Lexer<'a, TokenKind>,
    eoi: bool,
}

impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            source,
            lexer: TokenKind::lexer(source),
            eoi: false,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Result<Token<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lexer.next() {
            Some(Err(..)) => {
                let err = Error("failed to recognize the rest tokens".to_string());
                Some(Err(err.into()))
            }
            Some(Ok(kind)) => Some(Ok(Token {
                source: self.source,
                kind,
                span: self.lexer.span(),
            })),
            None if !self.eoi => {
                self.eoi = true;
                Some(Ok(Token::new_eoi(self.source)))
            }
            None => None,
        }
    }
}

pub fn run_tokenizer(source: &'_ str) -> Result<Vec<Token<'_>>, Error> {
    Tokenizer::new(source).collect::<Result<_, _>>()
}

#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Logos, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenKind {
    EOI,

    #[regex(r"[ \t\r\n\f]+", logos::skip)]
    Whitespace,

    #[regex(r"--[^\r\n\f]*", logos::skip)]
    Comment,

    #[regex(r"/\*([^\*]|(\*[^/]))*\*/", logos::skip)]
    CommentBlock,

    #[regex(r#"[_a-zA-Z][_a-zA-Z0-9]*"#)]
    Ident,

    #[regex(r#"'([^'\\]|\\.|'')*'"#)]
    #[regex(r#""([^"\\]|\\.|"")*""#)]
    #[regex(r#"`([^`\\]|\\.|``)*`"#)]
    LiteralString,
    #[regex(r"[xX]'[a-fA-F0-9]*'")]
    LiteralHexBinaryString,

    #[regex(r"[0-9]+(_|[0-9])*")]
    LiteralInteger,
    #[regex(r"0[xX][a-fA-F0-9]+")]
    LiteralHexInteger,

    #[regex(r"[0-9]+[eE][+-]?[0-9]+")]
    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?")]
    LiteralFloat,

    // Symbols
    #[token("=")]
    Eq,
    #[token("<>")]
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("<=")]
    Lte,
    #[token(">=")]
    Gte,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Multiply,
    #[token("/")]
    Divide,
    #[token("%")]
    Modulo,
    #[token("||")]
    Concat,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token(":")]
    Colon,
    #[token("::")]
    DoubleColon,
    #[token(";")]
    SemiColon,
    #[token("$")]
    Dollar,
    #[token("=>")]
    Arrow,

    // Keywords
    #[token("ADD", ignore(case))]
    ADD,
    #[token("AGGREGATE", ignore(case))]
    AGGREGATE,
    #[token("ALL", ignore(case))]
    ALL,
    #[token("ALTER", ignore(case))]
    ALTER,
    #[token("AND", ignore(case))]
    AND,
    #[token("ANALYZE", ignore(case))]
    ANALYZE,
    #[token("ANY", ignore(case))]
    ANY,
    #[token("ARRAY", ignore(case))]
    ARRAY,
    #[token("AS", ignore(case))]
    AS,
    #[token("ASC", ignore(case))]
    ASC,
    #[token("BEGIN", ignore(case))]
    BEGIN,
    #[token("BETWEEN", ignore(case))]
    BETWEEN,
    #[token("BOOLEAN", ignore(case))]
    BOOLEAN,
    #[token("BY", ignore(case))]
    BY,
    #[token("CASE", ignore(case))]
    CASE,
    #[token("CAST", ignore(case))]
    CAST,
    #[token("CLUSTER", ignore(case))]
    CLUSTER,
    #[token("COLUMN", ignore(case))]
    COLUMN,
    #[token("COMMENT", ignore(case))]
    COMMENT,
    #[token("CREATE", ignore(case))]
    CREATE,
    #[token("DATABASE", ignore(case))]
    DATABASE,
    #[token("DATABASES", ignore(case))]
    DATABASES,
    #[token("DELETE", ignore(case))]
    DELETE,
    #[token("DESC", ignore(case))]
    DESC,
    #[token("DESCRIBE", ignore(case))]
    DESCRIBE,
    #[token("DISTINCT", ignore(case))]
    DISTINCT,
    #[token("DROP", ignore(case))]
    DROP,
    #[token("ELSE", ignore(case))]
    ELSE,
    #[token("END", ignore(case))]
    END,
    #[token("EQUALITY", ignore(case))]
    EQUALITY,
    #[token("EXCLUDE", ignore(case))]
    EXCLUDE,
    #[token("EXEC", ignore(case))]
    EXEC,
    #[token("EXISTS", ignore(case))]
    EXISTS,
    #[token("EXPLAIN", ignore(case))]
    EXPLAIN,
    #[token("FALSE", ignore(case))]
    FALSE,
    #[token("FIRST", ignore(case))]
    FIRST,
    #[token("FLOAT", ignore(case))]
    FLOAT,
    #[token("FROM", ignore(case))]
    FROM,
    #[token("FULL", ignore(case))]
    FULL,
    #[token("GROUP", ignore(case))]
    GROUP,
    #[token("IF", ignore(case))]
    IF,
    #[token("IN", ignore(case))]
    IN,
    #[token("INDEX", ignore(case))]
    INDEX,
    #[token("INNER", ignore(case))]
    INNER,
    #[token("INSERT", ignore(case))]
    INSERT,
    #[token("INT", ignore(case))]
    INT,
    #[token("INTERVAL", ignore(case))]
    INTERVAL,
    #[token("INTO", ignore(case))]
    INTO,
    #[token("IS", ignore(case))]
    IS,
    #[token("JOB", ignore(case))]
    JOB,
    #[token("JOBS", ignore(case))]
    JOBS,
    #[token("JOIN", ignore(case))]
    JOIN,
    #[token("KEY", ignore(case))]
    KEY,
    #[token("LAST", ignore(case))]
    LAST,
    #[token("LEFT", ignore(case))]
    LEFT,
    #[token("LIMIT", ignore(case))]
    LIMIT,
    #[token("MATERIALIZED", ignore(case))]
    MATERIALIZED,
    #[token("NODEGROUP", ignore(case))]
    NODEGROUP,
    #[token("NOT", ignore(case))]
    NOT,
    #[token("NULL", ignore(case))]
    NULL,
    #[token("NULLS", ignore(case))]
    NULLS,
    #[token("OBJECT", ignore(case))]
    OBJECT,
    #[token("OFFSET", ignore(case))]
    OFFSET,
    #[token("ON", ignore(case))]
    ON,
    #[token("OPTIMIZE", ignore(case))]
    OPTIMIZE,
    #[token("OR", ignore(case))]
    OR,
    #[token("ORDER", ignore(case))]
    ORDER,
    #[token("OUTER", ignore(case))]
    OUTER,
    #[token("PERCENT", ignore(case))]
    PERCENT,
    #[token("PLAN", ignore(case))]
    PLAN,
    #[token("RANGE", ignore(case))]
    RANGE,
    #[token("RENAME", ignore(case))]
    RENAME,
    #[token("REPLACE", ignore(case))]
    REPLACE,
    #[token("RESUME", ignore(case))]
    RESUME,
    #[token("RIGHT", ignore(case))]
    RIGHT,
    #[token("SAMPLE", ignore(case))]
    SAMPLE,
    #[token("SCHEDULE", ignore(case))]
    SCHEDULE,
    #[token("SCHEMA", ignore(case))]
    SCHEMA,
    #[token("SCHEMAS", ignore(case))]
    SCHEMAS,
    #[token("SEARCH", ignore(case))]
    SEARCH,
    #[token("SELECT", ignore(case))]
    SELECT,
    #[token("SET", ignore(case))]
    SET,
    #[token("SHOW", ignore(case))]
    SHOW,
    #[token("STATEMENTS", ignore(case))]
    STATEMENTS,
    #[token("STRING", ignore(case))]
    STRING,
    #[token("SUSPEND", ignore(case))]
    SUSPEND,
    #[token("TABLE", ignore(case))]
    TABLE,
    #[token("TABLES", ignore(case))]
    TABLES,
    #[token("THEN", ignore(case))]
    THEN,
    #[token("TIMESTAMP", ignore(case))]
    TIMESTAMP,
    #[token("TO", ignore(case))]
    TO,
    #[token("TRUE", ignore(case))]
    TRUE,
    #[token("UINT", ignore(case))]
    UINT,
    #[token("UNION", ignore(case))]
    UNION,
    #[token("UPDATE", ignore(case))]
    UPDATE,
    #[token("VACUUM", ignore(case))]
    VACUUM,
    #[token("VALUES", ignore(case))]
    VALUES,
    #[token("VIEW", ignore(case))]
    VIEW,
    #[token("VIEWS", ignore(case))]
    VIEWS,
    #[token("WHEN", ignore(case))]
    WHEN,
    #[token("WHERE", ignore(case))]
    WHERE,
    #[token("WINDOW", ignore(case))]
    WINDOW,
    #[token("WITH", ignore(case))]
    WITH,
    #[token("WITHIN", ignore(case))]
    WITHIN,
    #[token("XOR", ignore(case))]
    XOR,

    // ScopeQL
    #[token("\\")]
    Backslash,
    #[token("CANCEL", ignore(case))]
    CANCEL,
}

impl TokenKind {
    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            LiteralFloat
                | LiteralInteger
                | LiteralString
                | LiteralHexBinaryString
                | LiteralHexInteger
        )
    }

    pub fn is_symbol(&self) -> bool {
        matches!(
            self,
            Eq | NotEq
                | Lt
                | Gt
                | Lte
                | Gte
                | Plus
                | Minus
                | Multiply
                | Divide
                | Modulo
                | Concat
                | LParen
                | RParen
                | LBracket
                | RBracket
                | LBrace
                | RBrace
                | Comma
                | Dot
                | Colon
                | DoubleColon
                | SemiColon
                | Dollar
                | Arrow
                | Backslash
        )
    }

    pub fn is_keyword(&self) -> bool {
        !self.is_literal() && !self.is_symbol() && !matches!(self, Ident | EOI)
    }
}
