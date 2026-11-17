// UTC timestamps in the group-wide form YYYY-MM-DDTHH:mm:ss.sssZ.
export function isoTimestamp(ms = Date.now()) {
  return new Date(ms).toISOString();
}

const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;

// True for a UTC ISO-8601 timestamp that parses to a real date.
export function isIsoTimestamp(value) {
  return typeof value === 'string' && ISO_RE.test(value) && !Number.isNaN(Date.parse(value));
}
