const Database = require('better-sqlite3');
const db = new Database('data.sqlite');
try {
  const rows = db.prepare('SELECT id,email,role,active,createdAt FROM users ORDER BY createdAt').all();
  console.table(rows);
} catch (e) { console.error(e.message); }
