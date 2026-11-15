export const AUTH_RULE = {
  ruleId: 'RULE-AUTH-001',
  ruleName: 'Repeated failed logins from one IP',
  ruleVersion: 1,
  threshold: 5,
  windowMs: 60000
};

export function isFailedAuthentication(event) {
  return event.category === 'authentication' &&
    event.outcome === 'failure';
}
