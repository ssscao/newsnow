import { Client } from "pg";  // 引入 pg 驱动
import type { NewsItem } from "@shared/types";
import type { CacheInfo, CacheRow } from "../types";

export class Cache {
  private db: Client;

  constructor(db: Client) {
    this.db = db;
  }

  async init() {
    await this.db.query(`
      CREATE TABLE IF NOT EXISTS cache (
        id TEXT PRIMARY KEY,
        updated INTEGER,
        data TEXT
      );
    `);
    console.log(`init cache table`);
  }

  async set(key: string, value: NewsItem[]) {
    const now = Date.now();
    await this.db.query(
      `INSERT INTO cache (id, data, updated) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET data = $2, updated = $3`,
      [key, JSON.stringify(value), now]
    );
    console.log(`set ${key} cache`);
  }

  async get(key: string): Promise<CacheInfo | undefined> {
    const result = await this.db.query(
      `SELECT id, data, updated FROM cache WHERE id = $1`,
      [key]
    );
    if (result.rows.length > 0) {
      const row = result.rows[0];
      console.log(`get ${key} cache`);
      return {
        id: row.id,
        updated: row.updated,
        items: JSON.parse(row.data),
      };
    }
  }

  async getEntire(keys: string[]): Promise<CacheInfo[]> {
    const keysStr = keys.map((_, idx) => `$${idx + 1}`).join(", ");
    const result = await this.db.query(
      `SELECT id, data, updated FROM cache WHERE id IN (${keysStr})`,
      keys
    );
    return result.rows.map((row) => ({
      id: row.id,
      updated: row.updated,
      items: JSON.parse(row.data),
    }));
  }

  async delete(key: string) {
    await this.db.query(`DELETE FROM cache WHERE id = $1`, [key]);
  }
}

export async function getCacheTable() {
  try {
    const db = new Client({
      connectionString: process.env.DATABASE_URL,
    });
    await db.connect();
    const cacheTable = new Cache(db);
    if (process.env.ENABLE_CACHE !== "false") await cacheTable.init();
    return cacheTable;
  } catch (error) {
    console.error("failed to init database", error);
  }
}
