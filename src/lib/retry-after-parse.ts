// Retry-After as a delay in milliseconds, apart from combos/failover.ts's cooldown state so a
// response formatter (and the Cloudflare Worker) can read one without the combo registry.

export const MAX_COOLDOWN_MS = 10 * 60_000;
const MAX_SERVER_DELAY_MS = 24 * 60 * 60_000;
const IMF_FIXDATE_RE = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), (\d{2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$/i;
const RFC850_DATE_RE = /^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), (\d{2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2}) (\d{2}):(\d{2}):(\d{2}) GMT$/i;
const ASCTIME_DATE_RE = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ( \d|\d{2}) (\d{2}):(\d{2}):(\d{2}) (\d{4})$/i;
const HTTP_MONTH_INDEX: Record<string, number> = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};


function parseUtcDateParts(
  year: number,
  monthName: string,
  day: number,
  hour: number,
  minute: number,
  second: number,
): number | undefined {
  const month = HTTP_MONTH_INDEX[monthName.toLowerCase()];
  if (month === undefined) return undefined;
  const timestamp = Date.UTC(year, month, day, hour, minute, second);
  const parsed = new Date(timestamp);
  return parsed.getUTCFullYear() === year
    && parsed.getUTCMonth() === month
    && parsed.getUTCDate() === day
    && parsed.getUTCHours() === hour
    && parsed.getUTCMinutes() === minute
    && parsed.getUTCSeconds() === second
    ? timestamp
    : undefined;
}

function parseHttpDate(value: string, now: number): number | undefined {
  const imf = IMF_FIXDATE_RE.exec(value);
  if (imf) {
    return parseUtcDateParts(
      Number(imf[3]), imf[2]!, Number(imf[1]),
      Number(imf[4]), Number(imf[5]), Number(imf[6]),
    );
  }
  const rfc850 = RFC850_DATE_RE.exec(value);
  if (rfc850) {
    const current = new Date(now);
    const currentYear = current.getUTCFullYear();
    const month = HTTP_MONTH_INDEX[rfc850[2]!.toLowerCase()];
    if (month === undefined) return undefined;
    let year = Math.floor(currentYear / 100) * 100 + Number(rfc850[3]);
    const yearDelta = year - currentYear;
    const candidateTimeOfYear = Date.UTC(
      2000, month, Number(rfc850[1]),
      Number(rfc850[4]), Number(rfc850[5]), Number(rfc850[6]),
    );
    const currentTimeOfYear = Date.UTC(
      2000, current.getUTCMonth(), current.getUTCDate(),
      current.getUTCHours(), current.getUTCMinutes(), current.getUTCSeconds(),
      current.getUTCMilliseconds(),
    );
    if (yearDelta < -50 || (yearDelta === -50 && candidateTimeOfYear < currentTimeOfYear)) {
      year += 100;
    } else if (yearDelta > 50 || (yearDelta === 50 && candidateTimeOfYear > currentTimeOfYear)) {
      year -= 100;
    }
    return parseUtcDateParts(
      year, rfc850[2]!, Number(rfc850[1]),
      Number(rfc850[4]), Number(rfc850[5]), Number(rfc850[6]),
    );
  }
  const asctime = ASCTIME_DATE_RE.exec(value);
  if (!asctime) return undefined;
  return parseUtcDateParts(
    Number(asctime[6]), asctime[1]!, Number(asctime[2]),
    Number(asctime[3]), Number(asctime[4]), Number(asctime[5]),
  );
}

/** Parse a Retry-After delay, optionally retaining an upstream delay up to one day. */
export function parseRetryAfterMs(
  value: string | null | undefined,
  now = Date.now(),
  options?: { preserveImmediate?: boolean; preserveServerDelay?: boolean },
): number | undefined {
  const text = value?.trim();
  if (!text) return undefined;
  // Keep legacy bounded parsing for other callers. Combo cooldowns preserve
  // multi-hour upstream delays, but never quarantine a target beyond one day.
  const maximum = options?.preserveServerDelay === true
    ? MAX_SERVER_DELAY_MS
    : MAX_COOLDOWN_MS;
  if (/^\d+(?:\.\d+)?$/.test(text)) {
    const seconds = Number(text);
    if (
      Number.isFinite(seconds)
      && (seconds > 0 || (options?.preserveImmediate && seconds === 0))
    ) {
      return Math.min(Math.max(Math.ceil(seconds * 1000), 1), maximum);
    }
  }
  const timestamp = parseHttpDate(text, now);
  if (timestamp === undefined) return undefined;
  const delay = timestamp - now;
  if (delay > 0) return Math.min(delay, maximum);
  return options?.preserveImmediate ? 1 : undefined;
}
