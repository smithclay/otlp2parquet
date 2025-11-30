/* tslint:disable */
/* eslint-disable */
/**
 * Convert OTLP JSON logs to Arrow IPC bytes
 *
 * Input: JSON string in OTLP ExportLogsServiceRequest format
 * Output: Arrow IPC stream bytes (can be loaded by DuckDB)
 */
export function logs_json_to_arrow_ipc(otlp_json: string): Uint8Array;
/**
 * Convert OTLP JSON traces to Arrow IPC bytes
 */
export function traces_json_to_arrow_ipc(otlp_json: string): Uint8Array;
/**
 * Convert OTLP JSON metrics to Arrow IPC bytes (returns gauge data only for demo)
 */
export function metrics_json_to_arrow_ipc(otlp_json: string): Uint8Array;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly logs_json_to_arrow_ipc: (a: number, b: number) => [number, number, number, number];
  readonly traces_json_to_arrow_ipc: (a: number, b: number) => [number, number, number, number];
  readonly metrics_json_to_arrow_ipc: (a: number, b: number) => [number, number, number, number];
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
