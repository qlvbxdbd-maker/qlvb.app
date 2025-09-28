const bcrypt = require('bcryptjs');
const Database = require('better-sqlite3');
const db = new Database('data.sqlite');

const email = 'datnt.bd@gmail.com';  // email cần reset
const newPass = 'Abc#2025!';         // MẬT KHẨU MỚI bạn sẽ dùng để đăng nhập

const u = db.prepare('SELECT id FROM users WHERE LOWER(email)=LOWER(?)').get(email);
if (!u) { console.log('Không tìm thấy user:', email); process.exit(1); }

db.prepare('UPDATE users SET hash=?, active=1 WHERE id=?')
  .run(bcrypt.hashSync(newPass, 10), u.id);

console.log('Đã đặt lại mật khẩu cho', email);
