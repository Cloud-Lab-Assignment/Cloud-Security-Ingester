import { z } from 'zod';

const timestamp = z.string().refine((value) => {
  const pattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

  if (!pattern.test(value)) return false;

  const date = new Date(value);

  return !Number.isNaN(date.getTime()) &&
    date.toISOString() === value;
}, {
  message: 'Invalid UTC ISO timestamp'
});

const sourceSchema = z.object({
  type: z.string().regex(/^[a-z][a-z0-9_]*$/),
  name: z.string().min(1),
  host: z.string().nullable().optional(),
  region: z.string().nullable().optional(),
  accountId: z.string().nullable().optional()
});

const commonFields = {
  schemaVersion: z.literal('1.0'),
  eventId: z.uuid({ version: 'v4' }),
  receivedAt: timestamp,
  eventTime: timestamp.nullable(),
  source: sourceSchema,
  ingestedBy: z.object({
    name: z.string().min(1),
    version: z.string().min(1)
  })
};

export const eventSchema = z.discriminatedUnion('format', [
  z.object({
    ...commonFields,
    format: z.literal('json'),
    payload: z.record(z.string(), z.unknown())
  }),

  z.object({
    ...commonFields,
    format: z.literal('text'),
    payload: z.string()
  })
]);

export const eventRequestSchema = z.discriminatedUnion('format', [
  z.object({
    source: sourceSchema,
    format: z.literal('json'),
    payload: z.record(z.string(), z.unknown()),
    eventTime: timestamp.nullable().optional()
  }),

  z.object({
    source: sourceSchema,
    format: z.literal('text'),
    payload: z.string(),
    eventTime: timestamp.nullable().optional()
  })
]);
