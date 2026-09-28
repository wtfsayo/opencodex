// The [1m] context-variant marker Claude clients may leave on a model id. Import-free.
export const ONE_M_MARKER_RE = /\[1m\]$/i;

export function stripOneMillionMarker(value: string): string {
  return value.replace(ONE_M_MARKER_RE, "");
}
