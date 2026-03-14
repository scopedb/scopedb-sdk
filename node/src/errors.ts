/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export type ErrorKind =
  | "Unexpected"
  | "ConfigInvalid"
  /** The statement execution was rejected by the server (failed or cancelled in-band). */
  | "StatementFailed";
export type ErrorStatus = "permanent" | "temporary" | "persistent";

export class ScopeDBError extends Error {
  readonly kind: ErrorKind;
  private errorStatus: ErrorStatus;
  private readonly errorContext: Map<string, string>;

  constructor(
    kind: ErrorKind,
    message: string,
    options?: {
      cause?: unknown;
      status?: ErrorStatus;
    },
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "ScopeDBError";
    this.kind = kind;
    this.errorStatus = options?.status ?? "permanent";
    this.errorContext = new Map();
  }

  withContext(key: string, value: string | number | boolean): this {
    this.errorContext.set(key, String(value));
    return this;
  }

  context(): ReadonlyMap<string, string> {
    return this.errorContext;
  }

  status(): ErrorStatus {
    return this.errorStatus;
  }

  setPermanent(): this {
    this.errorStatus = "permanent";
    return this;
  }

  setTemporary(): this {
    this.errorStatus = "temporary";
    return this;
  }

  setPersistent(): this {
    this.errorStatus = "persistent";
    return this;
  }

  isPermanent(): boolean {
    return this.errorStatus === "permanent";
  }

  isTemporary(): boolean {
    return this.errorStatus === "temporary";
  }

  isPersistent(): boolean {
    return this.errorStatus === "persistent";
  }

  override toString(): string {
    let s = `ScopeDBError [${this.kind}/${this.errorStatus}]: ${this.message}`;
    if (this.errorContext.size > 0) {
      const ctx = [...this.errorContext.entries()]
        .map(([k, v]) => `${k}=${v}`)
        .join(", ");
      s += ` { ${ctx} }`;
    }
    return s;
  }
}

export function asScopeDBError(
  kind: ErrorKind,
  message: string,
  cause: unknown,
): ScopeDBError {
  if (cause instanceof ScopeDBError) {
    return cause;
  }
  return new ScopeDBError(kind, message, { cause });
}
