import Ajv from 'ajv';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(currentDir, '../schemas');

const readSchema = (name) => {
  const file = path.join(schemaDir, name);
  return JSON.parse(fs.readFileSync(file, 'utf8'));
};

const ajv = new Ajv({
  allErrors: true,
  strict: false
});

const commonSchema = readSchema('common.v1.json');
const rawSchema = readSchema('raw-events.v1.json');
const normalizedSchema = readSchema('normalized-events.v1.json');

ajv.addSchema(commonSchema);

const validateRaw = ajv.compile(rawSchema);
const validateNormalized = ajv.compile(normalizedSchema);

export function validateRawEvent(event) {
  const valid = validateRaw(event);

  return {
    valid,
    errors: valid ? [] : validateRaw.errors
  };
}

export function validateNormalizedEvent(event) {
  const valid = validateNormalized(event);

  return {
    valid,
    errors: valid ? [] : validateNormalized.errors
  };
}
