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

/// Pretty-prints a string of JSON-encoded data.
///
/// This method assumes `input` to be a valid JSON string in UTF-8 encoding.
pub fn pretty_print(input: &str) -> String {
    let input = input.as_bytes();
    let mut output: Vec<u8> = vec![];

    // formatting states
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut in_backslash = false;
    let mut empty = false;
    let mut first = true;

    let mut n = 0;
    while n < input.len() {
        let b = input[n];

        if in_string {
            if in_backslash {
                output.push(input[n]);
                in_backslash = false;
            } else {
                match memchr::memchr2(b'"', b'\\', &input[n..]) {
                    None => {
                        // The whole rest of buf is part of the string
                        output.extend_from_slice(&input[n..]);
                        break;
                    }
                    Some(index) => {
                        let length = index + 1;
                        output.extend_from_slice(&input[n..n + length]);
                        if input[n + index] == b'"' {
                            // End of string
                            in_string = false;
                        } else {
                            // Backslash
                            in_backslash = true;
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
                    if first {
                        first = false;
                        output.push(input[n]);
                    } else if empty {
                        output.push(b'\n');
                        for _ in 0..depth {
                            output.push(b' ');
                            output.push(b' ');
                        }
                        output.push(input[n]);
                    } else if depth == 0 {
                        output.push(b'\n');
                        output.push(input[n]);
                    } else {
                        output.push(input[n]);
                    }
                    depth += 1;
                    empty = true;
                }
                b']' | b'}' => {
                    depth = depth.saturating_sub(1);
                    if empty {
                        empty = false;
                        output.push(input[n]);
                    } else {
                        output.push(b'\n');
                        for _ in 0..depth {
                            output.push(b' ');
                            output.push(b' ');
                        }
                        output.push(input[n]);
                    }
                }
                b',' => {
                    output.push(input[n]);
                    output.push(b'\n');
                    for _ in 0..depth {
                        output.push(b' ');
                        output.push(b' ');
                    }
                }
                b':' => {
                    output.push(input[n]);
                    output.push(b' ');
                }
                _ => {
                    if empty {
                        output.push(b'\n');
                        for _ in 0..depth {
                            output.push(b' ');
                            output.push(b' ');
                        }
                        empty = false;
                    }
                    if b == b'"' {
                        in_string = true;
                    }
                    output.push(input[n]);
                }
            };
        };
        n += 1;
    }

    String::from_utf8_lossy_owned(output)
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
