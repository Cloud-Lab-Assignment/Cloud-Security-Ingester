import fs from 'node:fs';
import pg from 'pg';

// TLS keeps certificate verification on: the CA bundle (AWS RDS global bundle
// in the image) is passed explicitly. Never set rejectUnauthorized:false.
export function createPool(config, extra = {}) {
  const options = { max: 10, ...extra };
  if (config.databaseUrl) options.connectionString = config.databaseUrl;
  if (config.databaseSsl) {
    options.ssl = { ca: fs.readFileSync(config.databaseSslCa, 'utf8') };
  }
  return new pg.Pool(options);
}

// Run fn inside a transaction on a dedicated client.
export async function withTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
