// console.js is generated from this TypeScript source. Do not edit its output.
// Only the Rust SDK's validated/redacted envelope reaches this sink. Never
// forward exception objects, stacks, URLs or response bodies here.
export function emitConsole(line: string): boolean {
  try {
    const value: unknown = JSON.parse(line);
    if (typeof value !== "object" || value === null || !("severity" in value)) return false;
    const method =
      value.severity === "error" ? "error" : value.severity === "warn" ? "warn" : "log";
    console[method](line.trimEnd());
    return true;
  } catch {
    return false;
  }
}
