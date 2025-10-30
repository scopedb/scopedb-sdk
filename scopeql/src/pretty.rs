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
    fn format(&mut self, json_string: &str) -> String {
        let mut output: Vec<u8> = vec![];

        self.format_buf(json_string.as_bytes(), &mut output);
        output.extend_from_slice(self.trailing_output.as_bytes());

        String::from_utf8_lossy_owned(output)
    }

    /// Format directly from a buffer into a writer.
    ///
    /// This may be called on chunks of a JSON document to format it bit by bit.
    ///
    /// As such, it does not add the `trailing_output` at the end.
    fn format_buf(&mut self, buf: &[u8], writer: &mut Vec<u8>) {
        let mut n = 0;
        while n < buf.len() {
            let b = buf[n];

            if self.in_string {
                if self.in_backslash {
                    writer.push(buf[n]);
                    self.in_backslash = false;
                } else {
                    match memchr::memchr2(b'"', b'\\', &buf[n..]) {
                        None => {
                            // The whole rest of buf is part of the string
                            writer.extend_from_slice(&buf[n..]);
                            break;
                        }
                        Some(index) => {
                            let length = index + 1;
                            writer.extend_from_slice(&buf[n..n + length]);
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
                            writer.push(buf[n]);
                        } else if self.empty {
                            writer.extend_from_slice(self.line_separator.as_bytes());
                            for _ in 0..self.depth {
                                writer.extend_from_slice(self.indent.as_bytes());
                            }
                            writer.push(buf[n]);
                        } else if !self.eager_record_separators && self.depth == 0 {
                            writer.extend_from_slice(self.record_separator.as_bytes());
                            writer.push(buf[n]);
                        } else {
                            writer.push(buf[n]);
                        }
                        self.depth += 1;
                        self.empty = true;
                    }
                    b']' | b'}' => {
                        self.depth = self.depth.saturating_sub(1);
                        if self.empty {
                            self.empty = false;
                            writer.push(buf[n]);
                        } else {
                            writer.extend_from_slice(self.line_separator.as_bytes());
                            for _ in 0..self.depth {
                                writer.extend_from_slice(self.indent.as_bytes());
                            }
                            writer.push(buf[n]);
                        }
                        if self.eager_record_separators && self.depth == 0 {
                            writer.extend_from_slice(self.record_separator.as_bytes());
                        }
                    }
                    b',' => {
                        writer.push(buf[n]);
                        writer.extend_from_slice(self.line_separator.as_bytes());
                        for _ in 0..self.depth {
                            writer.extend_from_slice(self.indent.as_bytes());
                        }
                    }
                    b':' => {
                        writer.push(buf[n]);
                        writer.extend_from_slice(self.after_colon.as_bytes());
                    }
                    _ => {
                        if self.empty {
                            writer.extend_from_slice(self.line_separator.as_bytes());
                            for _ in 0..self.depth {
                                writer.extend_from_slice(self.indent.as_bytes());
                            }
                            self.empty = false;
                        }
                        if b == b'"' {
                            self.in_string = true;
                        }
                        writer.push(buf[n]);
                    }
                };
            };
            n += 1;
        }
    }
}

/// Pretty-prints a string of JSON-encoded data.
///
/// Input must be valid JSON data in UTF-8 encoding.
pub fn pretty_print(json_string: &str) -> String {
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
            pretty_print("{\"a\":1,\"b\":2}"),
            "{\n  \"a\": 1,\n  \"b\": 2\n}"
        );
        assert_eq!(
            pretty_print("{\"empty\":{},\n\n\n\n\n\"one\":[1]}"),
            "{\n  \"empty\": {},\n  \"one\": [\n    1\n  ]\n}"
        );
    }
}
