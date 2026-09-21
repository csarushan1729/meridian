#!/usr/bin/env node
/**
 * Create the database named in DATABASE_URL if it does not exist yet.
 * Used by docker-compose (dashboard-migrate) so an existing Postgres volume also gets the
 * "auth" database, not only a fresh one. Safe to run many times.
 */
import pg from "pg";

const url = process.env.DATABASE_URL;
if (!url) {
  console.log("[ensure-db] DATABASE_URL not set, skipping.");
  process.exit(0);
}

const target = new URL(url);
const dbName = decodeURIComponent(target.pathname.slice(1));
if (!/^[A-Za-z0-9_]+$/.test(dbName)) {
  console.error(`[ensure-db] refusing unusual database name: ${dbName}`);
  process.exit(1);
}
const admin = new URL(url);
admin.pathname = "/postgres"; // connect to the default database to create the new one

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  for (let attempt = 1; ; attempt++) {
    const client = new pg.Client({ connectionString: admin.toString() });
    try {
      await client.connect();
      const found = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [dbName]);
      if (found.rowCount) {
        console.log(`[ensure-db] database "${dbName}" already exists.`);
      } else {
        await client.query(`CREATE DATABASE "${dbName}"`);
        console.log(`[ensure-db] created database "${dbName}".`);
      }
      await client.end();
      return;
    } catch (err) {
      await client.end().catch(() => {});
      if (attempt >= 30) throw err;
      console.log(`[ensure-db] waiting for postgres (${attempt}): ${err.message}`);
      await sleep(1000);
    }
  }
}

main().catch((err) => {
  console.error("[ensure-db] failed:", err?.message || err);
  process.exit(1);
});
