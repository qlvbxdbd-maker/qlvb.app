// admin-reset.js - Tạo/cập nhật tài khoản admin
const path = require('path');
const Database = require('better-sqlite3');
const bcrypt = require('bcryptjs');
require('dotenv').config();

const email = process.env.ADMIN_EMAIL || 'datnt.bd@gmail.com'; // chỉnh theo bạn
const pass  = process.env.ADMIN_PASSWORD || '123456';           // chỉnh theo bạn

const db = new Database(path.join(__dirname, 'data.sqlite'));

// Bảo đảm có bảng users (khớp server.js)
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email     TEXT UNIQUE,
    fullName  TEXT,
    partyId   TEXT,
    role      TEXT DEFAULT 'user',
    scopes    TEXT,
    hash      TEXT,
    active    INTEGER DEFAULT 1,
    createdAt TEXT DEFAULT (datetime('now'))
  )
`);

const row = db.prepare('SELECT id FROM users WHERE email=?').get(email);

if (row) {
  // dùng bind tham số cho role để tránh lỗi "no such column"
  db.prepare('UPDATE users SET hash=?, role=?, active=1 WHERE id=?')
    .run(bcrypt.hashSync(pass, 10), 'admin', row.id);
  console.log('ĐÃ CẬP NHẬT admin:', email);
} else {
  db.prepare('INSERT INTO users(email, fullName, role, hash, active) VALUES(?,?,?,?,1)')
    .run(email, 'Administrator', 'admin', bcrypt.hashSync(pass, 10));
  console.log('ĐÃ TẠO MỚI admin:', email);
}

console.log('> Đăng nhập bằng:', email, '/', pass);
