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
    #[token("ADD", ignore(ascii_case))]
    ADD,
    #[token("AGGREGATE", ignore(ascii_case))]
    AGGREGATE,
    #[token("ALL", ignore(ascii_case))]
    ALL,
    #[token("ALTER", ignore(ascii_case))]
    ALTER,
    #[token("AND", ignore(ascii_case))]
    AND,
    #[token("ANY", ignore(ascii_case))]
    ANY,
    #[token("ARRAY", ignore(ascii_case))]
    ARRAY,
    #[token("AS", ignore(ascii_case))]
    AS,
    #[token("ASC", ignore(ascii_case))]
    ASC,
    #[token("BEGIN", ignore(ascii_case))]
    BEGIN,
    #[token("BETWEEN", ignore(ascii_case))]
    BETWEEN,
    #[token("BOOLEAN", ignore(ascii_case))]
    BOOLEAN,
    #[token("BY", ignore(ascii_case))]
    BY,
    #[token("CASE", ignore(ascii_case))]
    CASE,
    #[token("CAST", ignore(ascii_case))]
    CAST,
    #[token("CLUSTER", ignore(ascii_case))]
    CLUSTER,
    #[token("COLUMN", ignore(ascii_case))]
    COLUMN,
    #[token("COMMENT", ignore(ascii_case))]
    COMMENT,
    #[token("CREATE", ignore(ascii_case))]
    CREATE,
    #[token("DATABASE", ignore(ascii_case))]
    DATABASE,
    #[token("DELETE", ignore(ascii_case))]
    DELETE,
    #[token("DESC", ignore(ascii_case))]
    DESC,
    #[token("DESCRIBE", ignore(ascii_case))]
    DESCRIBE,
    #[token("DISTINCT", ignore(ascii_case))]
    DISTINCT,
    #[token("DROP", ignore(ascii_case))]
    DROP,
    #[token("ELSE", ignore(ascii_case))]
    ELSE,
    #[token("END", ignore(ascii_case))]
    END,
    #[token("EQUALITY", ignore(ascii_case))]
    EQUALITY,
    #[token("EXCLUDE", ignore(ascii_case))]
    EXCLUDE,
    #[token("EXEC", ignore(ascii_case))]
    EXEC,
    #[token("EXISTS", ignore(ascii_case))]
    EXISTS,
    #[token("EXPLAIN", ignore(ascii_case))]
    EXPLAIN,
    #[token("FALSE", ignore(ascii_case))]
    FALSE,
    #[token("FIRST", ignore(ascii_case))]
    FIRST,
    #[token("FLOAT", ignore(ascii_case))]
    FLOAT,
    #[token("FROM", ignore(ascii_case))]
    FROM,
    #[token("FULL", ignore(ascii_case))]
    FULL,
    #[token("GROUP", ignore(ascii_case))]
    GROUP,
    #[token("IF", ignore(ascii_case))]
    IF,
    #[token("IN", ignore(ascii_case))]
    IN,
    #[token("INDEX", ignore(ascii_case))]
    INDEX,
    #[token("INNER", ignore(ascii_case))]
    INNER,
    #[token("INSERT", ignore(ascii_case))]
    INSERT,
    #[token("INT", ignore(ascii_case))]
    INT,
    #[token("INTERVAL", ignore(ascii_case))]
    INTERVAL,
    #[token("INTO", ignore(ascii_case))]
    INTO,
    #[token("IS", ignore(ascii_case))]
    IS,
    #[token("JOB", ignore(ascii_case))]
    JOB,
    #[token("JOBS", ignore(ascii_case))]
    JOBS,
    #[token("JOIN", ignore(ascii_case))]
    JOIN,
    #[token("KEY", ignore(ascii_case))]
    KEY,
    #[token("LAST", ignore(ascii_case))]
    LAST,
    #[token("LEFT", ignore(ascii_case))]
    LEFT,
    #[token("LIMIT", ignore(ascii_case))]
    LIMIT,
    #[token("MATERIALIZED", ignore(ascii_case))]
    MATERIALIZED,
    #[token("NODEGROUP", ignore(ascii_case))]
    NODEGROUP,
    #[token("NOT", ignore(ascii_case))]
    NOT,
    #[token("NULL", ignore(ascii_case))]
    NULL,
    #[token("NULLS", ignore(ascii_case))]
    NULLS,
    #[token("OBJECT", ignore(ascii_case))]
    OBJECT,
    #[token("OFFSET", ignore(ascii_case))]
    OFFSET,
    #[token("ON", ignore(ascii_case))]
    ON,
    #[token("OPTIMIZE", ignore(ascii_case))]
    OPTIMIZE,
    #[token("OR", ignore(ascii_case))]
    OR,
    #[token("ORDER", ignore(ascii_case))]
    ORDER,
    #[token("OUTER", ignore(ascii_case))]
    OUTER,
    #[token("PERCENT", ignore(ascii_case))]
    PERCENT,
    #[token("PLAN", ignore(ascii_case))]
    PLAN,
    #[token("RANGE", ignore(ascii_case))]
    RANGE,
    #[token("RENAME", ignore(ascii_case))]
    RENAME,
    #[token("REPLACE", ignore(ascii_case))]
    REPLACE,
    #[token("RESUME", ignore(ascii_case))]
    RESUME,
    #[token("RIGHT", ignore(ascii_case))]
    RIGHT,
    #[token("SAMPLE", ignore(ascii_case))]
    SAMPLE,
    #[token("SCHEDULE", ignore(ascii_case))]
    SCHEDULE,
    #[token("SCHEMA", ignore(ascii_case))]
    SCHEMA,
    #[token("SEARCH", ignore(ascii_case))]
    SEARCH,
    #[token("SELECT", ignore(ascii_case))]
    SELECT,
    #[token("SET", ignore(ascii_case))]
    SET,
    #[token("SHOW", ignore(ascii_case))]
    SHOW,
    #[token("STATEMENTS", ignore(ascii_case))]
    STATEMENTS,
    #[token("STRING", ignore(ascii_case))]
    STRING,
    #[token("SUSPEND", ignore(ascii_case))]
    SUSPEND,
    #[token("TABLE", ignore(ascii_case))]
    TABLE,
    #[token("TABLES", ignore(ascii_case))]
    TABLES,
    #[token("THEN", ignore(ascii_case))]
    THEN,
    #[token("TIMESTAMP", ignore(ascii_case))]
    TIMESTAMP,
    #[token("TO", ignore(ascii_case))]
    TO,
    #[token("TRUE", ignore(ascii_case))]
    TRUE,
    #[token("UINT", ignore(ascii_case))]
    UINT,
    #[token("UNION", ignore(ascii_case))]
    UNION,
    #[token("UPDATE", ignore(ascii_case))]
    UPDATE,
    #[token("VALUES", ignore(ascii_case))]
    VALUES,
    #[token("VIEW", ignore(ascii_case))]
    VIEW,
    #[token("VIEWS", ignore(ascii_case))]
    VIEWS,
    #[token("WHEN", ignore(ascii_case))]
    WHEN,
    #[token("WHERE", ignore(ascii_case))]
    WHERE,
    #[token("WINDOW", ignore(ascii_case))]
    WINDOW,
    #[token("WITH", ignore(ascii_case))]
    WITH,
    #[token("WITHIN", ignore(ascii_case))]
    WITHIN,
    #[token("XOR", ignore(ascii_case))]
    XOR,

    // ScopeQL
    #[token("\\")]
    Backslash,
    #[token("CANCEL", ignore(ascii_case))]
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
