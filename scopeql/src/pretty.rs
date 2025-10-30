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

// This file is derived from https://github.com/gamache/jsonxf/blob/ab914dc7/src/jsonxf.rs

use std::io::BufReader;
use std::io::BufWriter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;

const BUF_SIZE: usize = 1024 * 16;

struct Formatter {
    /// Used for beginning-of-line indentation in arrays and objects.
    pub indent: String,

    /// Used inside arrays and objects.
    pub line_separator: String,

    /// Used between root-level arrays and objects.
    pub record_separator: String,

    /// Used after a colon inside objects.
    pub after_colon: String,

    /// Used at very end of output.
    pub trailing_output: String,

    /// Add a record_separator as soon as a record ends, before seeing a
    /// subsequent record. Useful when there's a long time between records.
    pub eager_record_separators: bool,

    // private mutable state
    depth: usize,       // current nesting depth
    in_string: bool,    // is the next byte part of a string?
    in_backslash: bool, // does the next byte follow a backslash in a string?
    empty: bool,        // is the next byte in an empty object or array?
    first: bool,        // is this the first byte of input?
}

impl Formatter {
    /// Formats a string of JSON-encoded data.
    ///
    /// Input must be valid JSON data in UTF-8 encoding.
    fn format(&mut self, json_string: &str) -> Result<String, String> {
        let mut input = json_string.as_bytes();
        let mut output: Vec<u8> = vec![];
        match self.format_stream(&mut input, &mut output) {
            Ok(_) => {}
            Err(f) => {
                return Err(f.to_string());
            }
        };
        let output_string = match String::from_utf8(output) {
            Ok(s) => s,
            Err(f) => {
                return Err(f.to_string());
            }
        };
        Ok(output_string)
    }

    /// Formats a stream of JSON-encoded data.
    ///
    /// Input must be valid JSON data in UTF-8 encoding.
    fn format_stream(&mut self, input: &mut dyn Read, output: &mut dyn Write) -> Result<(), Error> {
        let mut reader = BufReader::new(input);
        let mut writer = BufWriter::new(output);
        self.format_stream_unbuffered(&mut reader, &mut writer)
    }

    /// Formats a stream of JSON-encoded data without buffering.
    ///
    /// This will perform many small writes, so it's advisable to use an output that does its own
    /// buffering. In simple cases, use [`Formatter::format_stream`] instead.
    fn format_stream_unbuffered(
        &mut self,
        input: &mut impl Read,
        output: &mut impl Write,
    ) -> Result<(), Error> {
        let mut buf = [0_u8; BUF_SIZE];
        loop {
            match input.read(&mut buf) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    self.format_buf(&buf[0..n], output)?;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        output.write_all(self.trailing_output.as_bytes())?;
        Ok(())
    }

    /// Format directly from a buffer into a writer.
    ///
    /// This may be called on chunks of a JSON document to format it bit by bit.
    ///
    /// As such, it does not add the `trailing_output` at the end.
    fn format_buf(&mut self, buf: &[u8], writer: &mut impl Write) -> Result<(), Error> {
        let mut n = 0;
        while n < buf.len() {
            let b = buf[n];

            if self.in_string {
                if self.in_backslash {
                    writer.write_all(&buf[n..n + 1])?;
                    self.in_backslash = false;
                } else {
                    match memchr::memchr2(b'"', b'\\', &buf[n..]) {
                        None => {
                            // The whole rest of buf is part of the string
                            writer.write_all(&buf[n..])?;
                            break;
                        }
                        Some(index) => {
                            let length = index + 1;
                            writer.write_all(&buf[n..n + length])?;
                            if buf[n + index] == b'"' {
                                // End of string
                                self.in_string = false;
                            } else {
                                // Backslash
                                self.in_backslash = true;
                            }
                            n += length;
                            continue;
                        }
                    }
                }
            } else {
                match b {
                    b' ' | b'\n' | b'\r' | b'\t' => {} // skip whitespace
                    b'[' | b'{' => {
                        if self.first {
                            self.first = false;
                            writer.write_all(&buf[n..n + 1])?;
                        } else if self.empty {
                            writer.write_all(self.line_separator.as_bytes())?;
                            for _ in 0..self.depth {
                                writer.write_all(self.indent.as_bytes())?;
                            }
                            writer.write_all(&buf[n..n + 1])?;
                        } else if !self.eager_record_separators && self.depth == 0 {
                            writer.write_all(self.record_separator.as_bytes())?;
                            writer.write_all(&buf[n..n + 1])?;
                        } else {
                            writer.write_all(&buf[n..n + 1])?;
                        }
                        self.depth += 1;
                        self.empty = true;
                    }
                    b']' | b'}' => {
                        self.depth = self.depth.saturating_sub(1);
                        if self.empty {
                            self.empty = false;
                            writer.write_all(&buf[n..n + 1])?;
                        } else {
                            writer.write_all(self.line_separator.as_bytes())?;
                            for _ in 0..self.depth {
                                writer.write_all(self.indent.as_bytes())?;
                            }
                            writer.write_all(&buf[n..n + 1])?;
                        }
                        if self.eager_record_separators && self.depth == 0 {
                            writer.write_all(self.record_separator.as_bytes())?;
                        }
                    }
                    b',' => {
                        writer.write_all(&buf[n..n + 1])?;
                        writer.write_all(self.line_separator.as_bytes())?;
                        for _ in 0..self.depth {
                            writer.write_all(self.indent.as_bytes())?;
                        }
                    }
                    b':' => {
                        writer.write_all(&buf[n..n + 1])?;
                        writer.write_all(self.after_colon.as_bytes())?;
                    }
                    _ => {
                        if self.empty {
                            writer.write_all(self.line_separator.as_bytes())?;
                            for _ in 0..self.depth {
                                writer.write_all(self.indent.as_bytes())?;
                            }
                            self.empty = false;
                        }
                        if b == b'"' {
                            self.in_string = true;
                        }
                        writer.write_all(&buf[n..n + 1])?;
                    }
                };
            };
            n += 1;
        }

        Ok(())
    }
}

/// Pretty-prints a string of JSON-encoded data.
///
/// Input must be valid JSON data in UTF-8 encoding.
pub fn pretty_print(json_string: &str) -> Result<String, String> {
    Formatter {
        indent: String::from("  "),
        line_separator: String::from("\n"),
        record_separator: String::from("\n"),
        after_colon: String::from(" "),
        trailing_output: String::from(""),
        eager_record_separators: false,
        depth: 0,
        in_string: false,
        in_backslash: false,
        empty: false,
        first: true,
    }
    .format(json_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_print() {
        assert_eq!(
            pretty_print("{\"a\":1,\"b\":2}").unwrap(),
            "{\n  \"a\": 1,\n  \"b\": 2\n}"
        );
        assert_eq!(
            pretty_print("{\"empty\":{},\n\n\n\n\n\"one\":[1]}").unwrap(),
            "{\n  \"empty\": {},\n  \"one\": [\n    1\n  ]\n}"
        );
    }
}
