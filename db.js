// db.js – lớp tương thích better-sqlite3 nhưng chạy được Postgres (Neon/pg)
const path = require('path');
const fs = require('fs');

const isPg = !!process.env.DATABASE_URL;
let client, pg;

/**
 * Chuyển dấu ? thành $1, $2 ... để dùng với pg.
 */
function toPgParams(sql, args = []) {
  let i = 0;
  const text = sql.replace(/\?/g, () => {
    i += 1;
    return `$${i}`;
  });
  return { text, values: args };
}

/* ------------------------------------------------------------------ */
/*                           Postgres (pg)                             */
/* ------------------------------------------------------------------ */
async function initPg() {
  pg = require('pg');
  client = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
    // Trên Render/Neon phần lớn dùng SSL; chấp nhận cert mặc định
    ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false },
    max: 5,
  });

  // Lưu ý QUAN TRỌNG:
  // - Trong Postgres, mọi tên cột KHÔNG ĐƯỢC đặt trong "..." sẽ thành chữ thường.
  // - Ta viết hoTen, chiBo, createdAt... trong DDL bên dưới NHƯNG KHÔNG đặt " ",
  //   nên bảng thật sẽ có cột: hoten, chibo, createdat...
  // - Các câu lệnh trong server.js cũng không đặt " ", nên Postgres sẽ tự map đúng.

  const ddl = `
  /* ---------------------- FIX LEGACY (idempotent) ---------------------- */
  DO $$
  BEGIN
    -- Nếu lỡ có cột "chiBo" (từng viết có dấu "), đổi về chibo
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_name = 'members' AND column_name = 'chiBo'
    ) THEN
      EXECUTE 'ALTER TABLE members RENAME COLUMN "chiBo" TO chibo';
    END IF;

    -- Nếu lỡ có cột "createdAt" do ai đó tạo với dấu ", đổi về createdat
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_name = 'members' AND column_name = 'createdAt'
    ) THEN
      EXECUTE 'ALTER TABLE members RENAME COLUMN "createdAt" TO createdat';
    END IF;

    -- Nếu lỡ có cột "ownerEmail"/"uploadedDate"/... dùng dấu ", bạn có thể
    -- thêm tương tự ở đây. (không bắt buộc, chỉ phòng khi bị tạo sai trước đó)
  END $$;

  -- Xoá các index cũ có thể sai tên cột để tạo lại cho sạch
  DROP INDEX IF EXISTS idx_members_chibo;
  DROP INDEX IF EXISTS idx_members_chiBo;

  /* --------------------------- CREATE TABLES --------------------------- */
  CREATE TABLE IF NOT EXISTS docs (
    id            TEXT PRIMARY KEY,
    name          TEXT,
    soHieu        TEXT,
    loai          TEXT,
    mucDo         TEXT,
    donVi         TEXT,
    hanXuLy       TEXT,
    nguoiGui      TEXT,
    nguoiPhuTrach TEXT,
    nhan          TEXT,
    trichYeu      TEXT,
    uploadedDate  TEXT,
    webViewLink   TEXT,
    flow          TEXT,
    ownerEmail    TEXT,
    createdAt     TIMESTAMP DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS shares (
    id SERIAL PRIMARY KEY,
    fileId    TEXT,
    email     TEXT,
    role      TEXT,
    notified  INTEGER,
    message   TEXT,
    createdAt TIMESTAMP DEFAULT NOW()
  );
  CREATE UNIQUE INDEX IF NOT EXISTS uq_shares_file_email_role ON shares(fileId,email,role);

  CREATE TABLE IF NOT EXISTS members (
    id SERIAL PRIMARY KEY,
    hoTen TEXT, tenGoiKhac TEXT, ngaySinh TEXT, gioiTinh TEXT, dienThoai TEXT,
    danToc TEXT, tonGiao TEXT, ksTinh TEXT, ksHuyen TEXT, ksXa TEXT,
    ngayVaoDang TEXT, ngayChinhThuc TEXT, soTheDang TEXT UNIQUE,
    soCCCD TEXT UNIQUE, ngayCapCCCD TEXT, chiBo TEXT, dangUyCapUy TEXT,
    donViBoPhan TEXT, email TEXT, ngayBatDauSH TEXT, ngayKetThucSH TEXT,
    trangThai TEXT, ghiChu TEXT,
    createdAt TIMESTAMP DEFAULT NOW()
  );
  CREATE UNIQUE INDEX IF NOT EXISTS uq_members_email ON members(email);

  CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE,
    fullName TEXT,
    partyId  TEXT,
    role     TEXT DEFAULT 'user',
    scopes   TEXT,
    manageAll INTEGER DEFAULT 0,
    manageUnits TEXT,
    manageChiBo TEXT,
    userUnit TEXT,
    userChiBo TEXT,
    chibo TEXT,              -- đồng bộ với server.js (dùng trong lọc/seed)
    hash TEXT,
    active INTEGER DEFAULT 1,
    googleEmail TEXT,
    createdAt TIMESTAMP DEFAULT NOW()
  );

  /* ------------------------------ INDEXES ------------------------------ */
  -- Lưu ý: vì KHÔNG đặt trong " ", Postgres sẽ hiểu các tên cột là chữ thường:
  --   createdAt -> createdat, chiBo -> chibo, ownerEmail -> owneremail ...
  CREATE INDEX IF NOT EXISTS idx_docs_createdAt  ON docs(createdat);
  CREATE INDEX IF NOT EXISTS idx_docs_flow       ON docs(flow);
  CREATE INDEX IF NOT EXISTS idx_docs_donVi      ON docs(donvi);
  CREATE INDEX IF NOT EXISTS idx_docs_mucDo      ON docs(mucdo);
  CREATE INDEX IF NOT EXISTS idx_docs_loai       ON docs(loai);
  CREATE INDEX IF NOT EXISTS idx_docs_nguoiGui   ON docs(nguoi gui);
  CREATE INDEX IF NOT EXISTS idx_docs_hanXuLy    ON docs(hanxuly);
  CREATE INDEX IF NOT EXISTS idx_docs_ownerEmail ON docs(owneremail);

  CREATE INDEX IF NOT EXISTS idx_shares_file     ON shares(fileid);
  CREATE INDEX IF NOT EXISTS idx_shares_email    ON shares(email);

  CREATE INDEX IF NOT EXISTS idx_members_email   ON members(email);
  CREATE INDEX IF NOT EXISTS idx_members_chibo   ON members(chibo);
  CREATE INDEX IF NOT EXISTS idx_members_created ON members(createdat);

  CREATE INDEX IF NOT EXISTS idx_users_role         ON users(role);
  CREATE INDEX IF NOT EXISTS idx_users_googleEmail  ON users(googleemail);
  `;

  await exec(ddl);

  // (Không cần gì thêm: mọi thứ idempotent, chạy lại không lỗi)
}

/* ------------------------------ API chung ------------------------------ */
async function query(sql, ...args) {
  const { text, values } = toPgParams(sql, args);
  return client.query(text, values);
}
async function get(sql, ...args) {
  const r = await query(sql, ...args);
  return r.rows[0] || undefined;
}
async function all(sql, ...args) {
  const r = await query(sql, ...args);
  return r.rows;
}
async function run(sql, ...args) {
  await query(sql, ...args);
  return { changes: 1 };
}
async function exec(multiSql) {
  const conn = await client.connect();
  try {
    await conn.query('BEGIN');
    await conn.query(multiSql);
    await conn.query('COMMIT');
  } catch (e) {
    await conn.query('ROLLBACK');
    throw e;
  } finally {
    conn.release();
  }
}
function prepare(sql) {
  return {
    get: (...args) => get(sql, ...args),
    all: (...args) => all(sql, ...args),
    run: (...args) => run(sql, ...args),
  };
}
function transaction(fn) {
  return async function () {
    const conn = await client.connect();
    try {
      await conn.query('BEGIN');

      // API tạm dùng cùng connection
      const txApi = {
        get: async (s, ...a) => (await conn.query(toPgParams(s, a))).rows[0],
        all: async (s, ...a) => (await conn.query(toPgParams(s, a))).rows,
        run: async (s, ...a) => {
          await conn.query(toPgParams(s, a));
          return { changes: 1 };
        },
        prepare: (s) => ({
          get: (...a) => txApi.get(s, ...a),
          all: (...a) => txApi.all(s, ...a),
          run: (...a) => txApi.run(s, ...a),
        }),
      };

      await fn(txApi);
      await conn.query('COMMIT');
    } catch (e) {
      await conn.query('ROLLBACK');
      throw e;
    } finally {
      conn.release();
    }
  };
}

/* ------------------------------------------------------------------ */
/*                         SQLite (dev/offline)                        */
/* ------------------------------------------------------------------ */
let sqliteDb;

function initSqlite() {
  const Database = require('better-sqlite3');
  const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data', 'party.sqlite');
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  sqliteDb = new Database(DB_PATH);
  sqliteDb.pragma('busy_timeout = 3000');
  sqliteDb.pragma('synchronous = NORMAL');

  // DDL giống server.js (giữ camelCase để dễ đọc; SQLite không phân biệt hoa/thường)
  sqliteDb.exec(`
    PRAGMA journal_mode = WAL;

    CREATE TABLE IF NOT EXISTS docs (
      id TEXT PRIMARY KEY,
      name TEXT, soHieu TEXT, loai TEXT, mucDo TEXT, donVi TEXT,
      hanXuLy TEXT, nguoiGui TEXT, nguoiPhuTrach TEXT, nhan TEXT, trichYeu TEXT,
      uploadedDate TEXT, webViewLink TEXT, flow TEXT, ownerEmail TEXT,
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS shares (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fileId TEXT, email TEXT, role TEXT, notified INTEGER, message TEXT,
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS members (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hoTen TEXT, tenGoiKhac TEXT, ngaySinh TEXT, gioiTinh TEXT, dienThoai TEXT,
      danToc TEXT, tonGiao TEXT, ksTinh TEXT, ksHuyen TEXT, ksXa TEXT,
      ngayVaoDang TEXT, ngayChinhThuc TEXT, soTheDang TEXT UNIQUE,
      soCCCD TEXT UNIQUE, ngayCapCCCD TEXT, chiBo TEXT, dangUyCapUy TEXT,
      donViBoPhan TEXT, email TEXT, ngayBatDauSH TEXT, ngayKetThucSH TEXT,
      trangThai TEXT, ghiChu TEXT,
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE, fullName TEXT, partyId TEXT, role TEXT DEFAULT 'user',
      scopes TEXT, manageAll INTEGER DEFAULT 0, manageUnits TEXT, manageChiBo TEXT,
      userUnit TEXT, userChiBo TEXT, chibo TEXT,
      hash TEXT, active INTEGER DEFAULT 1,
      googleEmail TEXT, createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE UNIQUE INDEX IF NOT EXISTS uq_members_email ON members(email);
    CREATE UNIQUE INDEX IF NOT EXISTS uq_shares_file_email_role ON shares(fileId,email,role);

    CREATE INDEX IF NOT EXISTS idx_docs_createdAt  ON docs(createdAt);
    CREATE INDEX IF NOT EXISTS idx_docs_flow       ON docs(flow);
    CREATE INDEX IF NOT EXISTS idx_docs_donVi      ON docs(donVi);
    CREATE INDEX IF NOT EXISTS idx_docs_mucDo      ON docs(mucDo);
    CREATE INDEX IF NOT EXISTS idx_docs_loai       ON docs(loai);
    CREATE INDEX IF NOT EXISTS idx_docs_nguoiGui   ON docs(nguoiGui);
    CREATE INDEX IF NOT EXISTS idx_docs_hanXuLy    ON docs(hanXuLy);
    CREATE INDEX IF NOT EXISTS idx_docs_ownerEmail ON docs(ownerEmail);

    CREATE INDEX IF NOT EXISTS idx_shares_file     ON shares(fileId);
    CREATE INDEX IF NOT EXISTS idx_shares_email    ON shares(email);

    CREATE INDEX IF NOT EXISTS idx_members_email   ON members(email);
    CREATE INDEX IF NOT EXISTS idx_members_chiBo   ON members(chiBo);
    CREATE INDEX IF NOT EXISTS idx_members_created ON members(createdAt);

    CREATE INDEX IF NOT EXISTS idx_users_role      ON users(role);
    CREATE INDEX IF NOT EXISTS idx_users_googleEmail ON users(googleEmail);
  `);
}

function wrapSqlite() {
  return {
    get: (s, ...a) => sqliteDb.prepare(s).get(...a),
    all: (s, ...a) => sqliteDb.prepare(s).all(...a),
    run: (s, ...a) => sqliteDb.prepare(s).run(...a),
    exec: (s) => sqliteDb.exec(s),
    prepare: (s) => sqliteDb.prepare(s),
    transaction: (fn) => () => {
      const tx = sqliteDb.transaction(() => fn(module.exports));
      tx();
    },
  };
}

/* ------------------------------------------------------------------ */
/*                           Khởi tạo & export                         */
/* ------------------------------------------------------------------ */
let api = null;

async function init() {
  if (isPg) {
    await initPg();
    api = { get, all, run, exec, prepare, transaction };
  } else {
    initSqlite();
    api = wrapSqlite();
  }
}
const ready = init();

module.exports = new Proxy(
  {},
  {
    get(_, prop) {
      if (prop === 'isPg') return isPg;
      if (prop === 'ready') return ready;
      return (...args) => api[prop](...args);
    },
  }
);
