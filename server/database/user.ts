import { Client } from "pg";  // 引入 pg 驱动
import type { UserInfo } from "#/types";

export class UserTable {
  private db: Client;

  constructor(db: Client) {
    this.db = db;
  }

  async init() {
    await this.db.query(`
      CREATE TABLE IF NOT EXISTS user (
        id TEXT PRIMARY KEY,
        email TEXT,
        data TEXT,
        type TEXT,
        created INTEGER,
        updated INTEGER
      );
    `);
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS idx_user_id ON user(id);
    `);
    console.log(`init user table`);
  }

  async addUser(id: string, email: string, type: "github") {
    const u = await this.getUser(id);
    const now = Date.now();
    if (!u) {
      await this.db.query(
        `INSERT INTO user (id, email, data, type, created, updated) VALUES ($1, $2, $3, $4, $5, $6)`,
        [id, email, "", type, now, now]
      );
      console.log(`add user ${id}`);
    } else if (u.email !== email || u.type !== type) {
      await this.db.query(
        `UPDATE user SET email = $1, updated = $2 WHERE id = $3`,
        [email, now, id]
      );
      console.log(`update user ${id} email`);
    } else {
      console.log(`user ${id} already exists`);
    }
  }

  async getUser(id: string) {
    const result = await this.db.query(
      `SELECT id, email, data, created, updated FROM user WHERE id = $1`,
      [id]
    );
    if (result.rows.length > 0) {
      return result.rows[0] as UserInfo;
    }
  }

  async setData(key: string, value: string, updatedTime = Date.now()) {
    const result = await this.db.query(
      `UPDATE user SET data = $1, updated = $2 WHERE id = $3`,
      [value, updatedTime, key]
    );
    if (result.rowCount === 0) {
      throw new Error(`set user ${key} data failed`);
    }
    console.log(`set ${key} data`);
  }

  async getData(id: string) {
    const result = await this.db.query(
      `SELECT data, updated FROM user WHERE id = $1`,
      [id]
    );
    if (result.rows.length === 0) {
      throw new Error(`user ${id} not found`);
    }
    console.log(`get ${id} data`);
    return result.rows[0] as {
      data: string;
      updated: number;
    };
  }

  async deleteUser(key: string) {
    const result = await this.db.query(
      `DELETE FROM user WHERE id = $1`,
      [key]
    );
    if (result.rowCount === 0) {
      throw new Error(`delete user ${key} failed`);
    }
    console.log(`delete user ${key}`);
  }
}
