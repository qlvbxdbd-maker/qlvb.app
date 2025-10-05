// PartyDocs – Trang chủ + Văn bản + Đảng viên + Báo cáo + Quản trị + Nhập Excel
// DB: Postgres (Neon) qua db.js; dev không có DATABASE_URL thì db.js sẽ dùng SQLite
// Auth: express-session + bcryptjs

require('dotenv').config();
const path = require('path');
const fs = require('fs');

const express = require('express');
const multer = require('multer');
const compression = require('compression');
const cors = require('cors');


const session = require('express-session');
const bcrypt = require('bcryptjs');

const { google } = require('googleapis');
const XLSX = require('xlsx');

// ===== DB adapter (bắt buộc tạo file db.js như đã hướng dẫn) =====
const db = require('./db'); // API: await db.ready; db.get/all/run/exec; db.transaction(fn)

// ===== Redis session store (scale ngang) =====
const IORedis = require('ioredis');

// --- safe JSON helpers (chấp nhận cả mảng JSON hoặc chuỗi CSV) ---
function safeParseJSON(s, fallback) {
  try { if (s == null || s === '') return fallback; return JSON.parse(s); }
  catch { return fallback; }
}
function parseArrayOrCSV(s) {
  const v = safeParseJSON(s, null);
  if (Array.isArray(v)) return v;
  if (typeof s === 'string' && s.trim()) {
    return s.split(/[,;|]/).map(x => x.trim()).filter(Boolean);
  }
  return [];
}

/* ===================== HTTP & STATIC ===================== */
const app = express();
const isProd = process.env.NODE_ENV === 'production';
app.set('trust proxy', 1);

app.use(cors({
  origin: process.env.CORS_ORIGIN || true, // cùng domain thì true là đủ
  credentials: true                        // nếu dùng cookie session
}));

app.use(compression());
app.use(express.json({ limit: process.env.JSON_LIMIT || '10mb' }));
app.use(express.urlencoded({ extended: true }));

// NÊN đặt ngay sau app.use(express.urlencoded(...))
// const cors = require('cors');
const FRONTEND = process.env.FRONTEND_ORIGIN; // ví dụ: https://docs.example.vn
if (FRONTEND) {
  app.use(cors({ origin: FRONTEND, credentials: true }));
}

// Health check cho Render (trả 200 nhanh gọn)
app.get('/healthz', (req, res) => res.status(200).send('OK'));

app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: isProd ? '7d' : '0',
  etag: true,
  setHeaders(res, filePath) {
    const base = path.basename(filePath);
    if (['sw.js','manifest.webmanifest','offline.html'].includes(base)) {
      res.setHeader('Cache-Control', 'no-cache');
    }
    // mọi file .html đều no-cache
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
    if (/\.[0-9a-f]{8,}\./i.test(base)) {
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    }
  }
}));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/* ===== Session Store (Redis nếu có, dev fallback MemoryStore) ===== */
let sessionStore;
try {
  const connectRedis = require('connect-redis');
  const RedisExport = connectRedis.default || connectRedis;
  const hasRedisUrl = !!process.env.REDIS_URL && String(process.env.REDIS_URL).trim() !== '';
  if (hasRedisUrl) {
    const redisClient = new IORedis(process.env.REDIS_URL);
    let storeInstance;
    if (typeof RedisExport === 'function' && RedisExport.length === 1) {
      const RedisStoreV6 = RedisExport(session);
      storeInstance = new RedisStoreV6({ client: redisClient, prefix: process.env.REDIS_PREFIX || 'partydocs:sess:' });
    } else {
      const RedisStoreV7 = RedisExport;
      storeInstance = new RedisStoreV7({ client: redisClient, prefix: process.env.REDIS_PREFIX || 'partydocs:sess:' });
    }
    sessionStore = storeInstance;
    console.log('[session] Using Redis store at', process.env.REDIS_URL);
  } else {
    console.warn('[session] REDIS_URL not set → using MemoryStore (dev only).');
    sessionStore = new session.MemoryStore();
  }
} catch (err) {
  console.warn('[session] Redis unavailable → fallback MemoryStore. Error:', err?.message || err);
  sessionStore = new session.MemoryStore();
}

app.use(session({
  store: sessionStore,
  name: process.env.SESSION_NAME || 'pd.sid',
  secret: process.env.SESSION_SECRET || 'partydocs_dev_secret',
  resave: false,
  saveUninitialized: false,
  rolling: true,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: isProd,
    maxAge: Number(process.env.SESSION_TTL_MS || 1000 * 60 * 60 * 8),
  }
}));

// Upload temp
const uploadsDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
const upload = multer({ dest: uploadsDir });

// Giới hạn file
function checkUploadRules(req, res, next) {
  const maxMb = Number(process.env.MAX_FILE_MB || 0);
  if (maxMb && req.file && req.file.size > maxMb * 1024 * 1024) {
    return res.status(400).json({ ok: false, error: `Tệp vượt quá ${maxMb}MB` });
  }
  const allow = (process.env.ALLOWED_MIME || "").split(",").map(s => s.trim()).filter(Boolean);
  if (allow.length && req.file && !allow.includes(req.file.mimetype)) {
    return res.status(400).json({ ok: false, error: "Định dạng tệp không được phép" });
  }
  next();
}

/* ===================== CATALOGS (có cache RAM) ===================== */
const catalogsPath = path.join(__dirname, "catalogs.json");
let catalogsCache = null;
let catalogsMtime = 0;

function loadCatalogsFromDisk() {
  try {
    const stat = fs.existsSync(catalogsPath) ? fs.statSync(catalogsPath) : null;
    if (stat && stat.mtimeMs === catalogsMtime && catalogsCache) return catalogsCache;
    if (stat) {
      catalogsCache = JSON.parse(fs.readFileSync(catalogsPath, "utf8") || "{}");
      catalogsMtime = stat.mtimeMs;
      return catalogsCache;
    }
  } catch {}
  catalogsCache = {
    loaiVanBan: [
      { id: "congvan", label: "Công văn" },
      { id: "baocao", label: "Báo cáo" },
      { id: "quyetdinh", label: "Quyết định" }
    ],
    mucDo: [
      { id: "thuong", label: "Thường" },
      { id: "khan",   label: "Khẩn" },
      { id: "mat",    label: "Mật" }
    ],
    donVi: [
      { id: "danguy", label: "Đảng ủy" },
      { id: "chibo",  label: "Chi bộ" },
      { id: "chung",  label: "Chung" }
    ],
    nhan: ["Đảng vụ", "Khẩn", "Nội bộ"]
  };
  catalogsMtime = Date.now();
  return catalogsCache;
}

function ensureAdmin(req, res, next) {
  if (req.session?.user?.role === "admin") return next();
  return res.status(403).json({ ok:false, error:"Chỉ dành cho admin" });
}
app.get("/catalogs", (req, res) => {
  try { res.json(loadCatalogsFromDisk()); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get("/admin/catalogs", ensureAdmin, (req,res)=>{
  try{ res.json({ ok:true, data: loadCatalogsFromDisk() }); }
  catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});
app.post("/admin/catalogs", ensureAdmin, (req,res)=>{
  try{
    fs.writeFileSync(catalogsPath, JSON.stringify(req.body||{}, null, 2), "utf8");
    catalogsCache = req.body || {};
    catalogsMtime = Date.now();
    res.json({ ok:true });
  }catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

/* ===================== GOOGLE DRIVE ===================== */
function isInvalidGrant(err) {
  const s = String(
    err?.response?.data?.error ||
    err?.errors?.[0]?.message ||
    err?.message || ''
  ).toLowerCase();
  return s.includes('invalid_grant');
}
function buildOAuth() {
  return new google.auth.OAuth2(
    process.env.GOOGLE_OAUTH_CLIENT_ID,
    process.env.GOOGLE_OAUTH_CLIENT_SECRET,
    process.env.GOOGLE_OAUTH_REDIRECT_URI
  );
}
const GOOGLE_TOKEN_PATH = process.env.GOOGLE_TOKEN_PATH || path.join(__dirname, 'data', 'token.json');
fs.mkdirSync(path.dirname(GOOGLE_TOKEN_PATH), { recursive: true });
const getTokens = () => fs.existsSync(GOOGLE_TOKEN_PATH) ? JSON.parse(fs.readFileSync(GOOGLE_TOKEN_PATH, "utf8") || "{}") : null;
const saveTokens = (t) => fs.writeFileSync(GOOGLE_TOKEN_PATH, JSON.stringify(t, null, 2), "utf8");

async function authAsCentral() {
  const oauth2 = buildOAuth();
  const saved = getTokens();
  if (!saved || !saved.refresh_token) {
    throw new Error("Chưa kết nối Drive trung tâm. Vào /auth/admin/drive để ủy quyền.");
  }
  oauth2.setCredentials(saved);
  oauth2.on('tokens', (tokens) => {
    const merged = { ...saved, ...tokens };
    try { saveTokens(merged); } catch {}
  });
  return oauth2;
}
async function ensureRootFolder(drive, name) {
  const q = `name='${name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false`;
  const list = await drive.files.list({ q, fields: "files(id,name)" });
  if (list.data.files?.length) return list.data.files[0].id;
  const created = await drive.files.create({
    requestBody: { name, mimeType: "application/vnd.google-apps.folder", parents: ["root"] },
    fields: "id"
  });
  return created.data.id;
}
async function findOrCreateFolder(drive, parentId, name) {
  const q = `'${parentId}' in parents and name='${name}' and mimeType='application/vnd.google-apps.folder' and trashed=false`;
  const r = await drive.files.list({ q, fields:"files(id)" });
  if (r.data.files?.length) return r.data.files[0].id;
  const c = await drive.files.create({
    requestBody: { name, parents:[parentId], mimeType:'application/vnd.google-apps.folder' },
    fields: 'id'
  });
  return c.data.id;
}
async function ensureYearMonthFolder(drive, rootId, year, month) {
  const yearId = await findOrCreateFolder(drive, rootId, String(year));
  const mm = String(month).padStart(2, "0");
  const monthId = await findOrCreateFolder(drive, yearId, mm);
  return monthId;
}
async function ensurePersonalFolder(drive, email) {
  const rootId = await ensureRootFolder(drive, process.env.DRIVE_ROOT_FOLDER_NAME || "PartyDocsRoot");
  const usersId = await findOrCreateFolder(drive, rootId, "Users");
  const safe = (email||"unknown").replace(/[\/\\]/g,"_");
  return await findOrCreateFolder(drive, usersId, safe);
}

// OAuth routes
app.get("/auth/admin/drive", (req, res) => {
  const oauth2 = buildOAuth();
  const envScopes = (process.env.GOOGLE_OAUTH_SCOPES || "").split(/\s+/).filter(Boolean);
  const scopes = envScopes.length ? envScopes : ["https://www.googleapis.com/auth/drive"];
  const url = oauth2.generateAuthUrl({ access_type: "offline", scope: scopes, prompt: "consent" });
  res.redirect(url);
});
app.get("/oauth2/callback", async (req, res) => {
  try {
    const oauth2 = buildOAuth();
    const { tokens } = await oauth2.getToken(req.query.code);
    if (tokens.refresh_token) saveTokens({ ...getTokens(), ...tokens });
    oauth2.setCredentials(tokens);
    const drive = google.drive({ version: "v3", auth: oauth2 });
    await ensureRootFolder(drive, process.env.DRIVE_ROOT_FOLDER_NAME || "PartyDocsRoot");
    res.redirect("/?authok=1");
  } catch (e) { res.status(500).send("Lỗi callback OAuth: " + e.message); }
});
app.get("/auth/status", async (req, res) => {
  await db.ready;
  const t = getTokens();
  const sessUser = req.session.user || null;
  let fullUser = null;
  if (sessUser?.id) {
    const u = await db.get(`
      SELECT id,email,fullName,partyId,role,active,scopes,
             manageAll,manageUnits,manageChiBo,userUnit,userChiBo
      FROM users WHERE id=?`, sessUser.id);
    if (u) {
      fullUser = {
        id: u.id,
        email: u.email,
        fullName: u.fullName,
        role: u.role,
        scopes:    safeParseJSON(u.scopes, {}),
        manageAll: !!u.manageAll,
        manageUnits: parseArrayOrCSV(u.manageUnits),
        manageChiBo: parseArrayOrCSV(u.manageChiBo),
        userUnit:  u.userUnit  || null,
        userChiBo: u.userChiBo || null
      };
    }
  }
  res.json({ ok: true, authorized: !!(t && t.refresh_token), user: fullUser });
});
app.get("/drive/ping", async (req, res) => {
  try { const auth = await authAsCentral(); const drive = google.drive({ version: "v3", auth });
    const about = await drive.about.get({ fields: "user,storageQuota" }); res.json(about.data);
  } catch (e) { res.status(500).send(e.message); }
});
app.post("/auth/reset-token", ensureAdmin, (req,res)=>{
  try {
    if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH);
    res.json({ ok:true, message:"Đã xoá token.json, vào /auth/admin/drive để cấp quyền lại." });
  } catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

/* ===================== AUTH ===================== */
app.post("/auth/login", async (req,res)=>{
  await db.ready;
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ ok:false, error:"Thiếu email/password" });
  const u = await db.get("SELECT * FROM users WHERE email=? AND active=1", String(email).trim().toLowerCase());
  if (!u || !bcrypt.compareSync(password, u.hash||"x")) return res.status(401).json({ ok:false, error:"Sai thông tin đăng nhập" });
  req.session.user = { id:u.id, email:u.email, fullName:u.fullName, role:u.role };
  res.json({ ok:true, user:{ id:u.id, email:u.email, fullName:u.fullName, role:u.role } });
});
app.post("/auth/logout", (req,res)=>{ req.session.destroy(()=>res.json({ok:true})); });

// API đổi mật khẩu
app.post("/me/change-password", async (req,res)=>{
  await db.ready;
  const me = req.session?.user;
  const { oldPassword, newPassword } = req.body||{};
  if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });
  if (!oldPassword || !newPassword) return res.status(400).json({ ok:false, error:"Thiếu mật khẩu" });
  const u = await db.get("SELECT id,hash FROM users WHERE id=?", me.id);
  if (!u || !bcrypt.compareSync(oldPassword, u.hash||"x")) return res.status(400).json({ ok:false, error:"Mật khẩu cũ không đúng" });
  await db.run("UPDATE users SET hash=? WHERE id=?", bcrypt.hashSync(newPassword,10), me.id);
  res.json({ ok:true });
});

// seed admin
(async function seedAdmin(){
  await db.ready;
  const email = process.env.ADMIN_EMAIL, pass = process.env.ADMIN_PASSWORD;
  if (!email || !pass) return;
  const has = await db.get("SELECT 1 FROM users WHERE email=?", email.trim().toLowerCase());
  if (!has) {
    await db.run(
      "INSERT INTO users(email, fullName, role, hash, scopes, manageAll, manageUnits, manageChiBo, active) VALUES(?,?,?,?,?,?,?, ?,1)",
      email.trim().toLowerCase(), "Administrator", "admin", bcrypt.hashSync(pass, 10), "{}", 1, "[]", "[]"
    );
    console.log("[seed] created admin:", email);
  }
})();

/* ===================== HELPERS & ACL ===================== */
function currentUser(req){ return req.session?.user || null; }
function isAdmin(u){ return u && u.role === 'admin'; }
function isManager(u){
  return u && (u.role === 'manager_unit' || u.role === 'manager_chibo' || u.role === 'manager_all' || u.role === 'admin');
}
async function docACL(req){
  await db.ready;
  const me = currentUser(req);
  if (!me) return { clause:"1=0", params:[] };

  const u = await db.get(`
    SELECT role, manageAll, manageUnits, manageChiBo, userUnit, userChiBo
    FROM users WHERE id=?`, me.id);

  const role = u?.role || 'user';
  const manageAllFlag = !!u?.manageAll || role === 'manager_all';
  const units = parseArrayOrCSV(u?.manageUnits);
  const userUnit = u?.userUnit || null;

  if (role === 'admin') return { clause:"", params:[] };

  if (manageAllFlag) {
    return {
      clause: `(flow IN ('den','di')) OR (flow='personal' AND ownerEmail=?) OR id IN (SELECT fileId FROM shares WHERE email=?)`,
      params: [me.email, me.email]
    };
  }

  if (role === 'user'){
    return {
      clause: `((flow='personal' AND ownerEmail=?) OR id IN (SELECT fileId FROM shares WHERE email=?))`,
      params: [me.email, me.email]
    };
  }

  const unitsAllowed = (units && units.length ? units : (userUnit ? [userUnit] : []));
  if (!unitsAllowed.length){
    return {
      clause: `((flow='personal' AND ownerEmail=?) OR id IN (SELECT fileId FROM shares WHERE email=?))`,
      params: [me.email, me.email]
    };
  }
  const placeholders = unitsAllowed.map(()=>'?').join(',');
  return {
    clause:
      `((flow IN ('den','di') AND COALESCE(donVi,'') IN (${placeholders}))` +
      ` OR (flow='personal' AND ownerEmail=?)` +
      ` OR id IN (SELECT fileId FROM shares WHERE email=?))`,
    params: [...unitsAllowed, me.email, me.email]
  };
}
async function memberACL(req){
  await db.ready;
  const me = currentUser(req);
  if (!me) return { memberClause:"1=0", memberParams:[] };

  const u = await db.get(`
    SELECT role, manageAll, manageUnits, manageChiBo, userUnit, userChiBo
    FROM users WHERE id=?`, me.id);

  const role = u?.role || 'user';
  const isSuper = (role === 'admin' || role === 'manager_all' || !!u?.manageAll);
  if (isSuper) return { memberClause:"", memberParams:[] };

  const theEmail = String(me.email||'').toLowerCase();
  const conds = ["LOWER(COALESCE(email,''))=?"];
  const params = [ theEmail ];

  if (role === 'user') return { memberClause: conds.join(' OR '), memberParams: params };

  const units = parseArrayOrCSV(u?.manageUnits);
  const chis  = parseArrayOrCSV(u?.manageChiBo);
  const userUnit = u?.userUnit ? [u.userUnit] : [];
  const allowedUnits = (units.length ? units : userUnit);

  if (allowedUnits.length){
    conds.push(`COALESCE(donViBoPhan,'') IN (${allowedUnits.map(()=>'?').join(',')})`);
    params.push(...allowedUnits);
  }
  if (chis.length){
    conds.push(`COALESCE(chiBo,'') IN (${chis.map(()=>'?').join(',')})`);
    params.push(...chis);
  }

  return { memberClause: conds.join(' OR '), memberParams: params };
}
async function canAccessDocById(req, fileId){
  await db.ready;
  const me = currentUser(req);
  if (!me) return { ok:false, code:401, error:"Cần đăng nhập" };

  const d = await db.get("SELECT id,flow,ownerEmail,donVi FROM docs WHERE id=?", fileId);
  if (!d) return { ok:false, code:404, error:"Không tìm thấy tài liệu" };

  if (isAdmin(me)) return { ok:true };

  const u = await db.get(`
    SELECT role, manageAll, manageUnits, manageChiBo, userUnit, userChiBo
    FROM users WHERE id=?`, me.id);

  const role = u?.role || 'user';
  const manageAll = !!u?.manageAll || role === 'manager_all';
  const units = parseArrayOrCSV(u?.manageUnits);
  const userUnit = u?.userUnit || null;

  const allowedUnits = Array.from(new Set([ ...(units||[]), ...(userUnit ? [userUnit] : []) ]));

  if (d.flow === 'personal'){
    if (d.ownerEmail === me.email) return { ok:true };
    return { ok:false, code:403, error:"Không có quyền truy cập tài liệu cá nhân" };
  }

  if (role === 'user'){
    const shared = await db.get("SELECT 1 FROM shares WHERE fileId=? AND email=?", fileId, me.email);
    return shared ? { ok:true } : { ok:false, code:403, error:"Không có quyền tải tệp này" };
  }

  if (manageAll) return { ok:true };
  if (allowedUnits.length && allowedUnits.map(String).includes(String(d.donVi||''))) return { ok:true };

  const shared = await db.get("SELECT 1 FROM shares WHERE fileId=? AND email=?", fileId, me.email);
  return shared ? { ok:true } : { ok:false, code:403, error:"Không có quyền tải tệp này" };
}

// ===== Cache TTL & helpers =====
const grantCache = new Map();
function shouldGrantNow(key, ttlMs = 10 * 60 * 1000) {
  const now = Date.now();
  const t = grantCache.get(key) || 0;
  if (now - t < ttlMs) return false;
  grantCache.set(key, now);
  return true;
}
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
async function withRetry(fn, times=3, base=200){
  let lastErr;
  for (let i=0;i<times;i++){
    try { return await fn(); } catch(err){
      const code = err?.code || err?.response?.status;
      const msg  = String(err?.message||'').toLowerCase();
      if (code === 429 || msg.includes('rate') || msg.includes('user rate')) {
        await sleep(base * (1<<i) + Math.floor(Math.random()*100));
        lastErr = err; continue;
      }
      throw err;
    }
  }
  throw lastErr;
}
async function pMapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let index = 0;
  async function runner() {
    while (true) {
      const i = index++;
      if (i >= items.length) return;
      try { results[i] = await worker(items[i], i); }
      catch (err) { results[i] = { ok:false, error: err?.message || String(err) }; }
    }
  }
  const runners = Array.from({ length: Math.min(limit, items.length) }, () => runner());
  await Promise.all(runners);
  return results;
}
async function resolveViewEmailsForUser(userIdOrEmail, fallbackEmail) {
  await db.ready;
  let row = null;
  if (typeof userIdOrEmail === 'number' || /^\d+$/.test(String(userIdOrEmail))) {
    row = await db.get("SELECT email, googleEmail FROM users WHERE id=?", Number(userIdOrEmail));
  } else {
    row = await db.get("SELECT email, googleEmail FROM users WHERE LOWER(email)=LOWER(?)", String(userIdOrEmail||''));
  }
  const emails = new Set();
  const appEmail = String(row?.email || fallbackEmail || '').trim().toLowerCase();
  const gEmail  = String(row?.googleEmail || '').trim().toLowerCase();
  if (gEmail)  emails.add(gEmail);
  if (appEmail) emails.add(appEmail);
  return Array.from(emails).filter(Boolean);
}
async function canEditMember(req, row){
  await db.ready;
  const me = currentUser(req);
  if (!me) return { ok:false, code:401, error:'Cần đăng nhập' };
  if (!row) return { ok:false, code:404, error:'Không tìm thấy hồ sơ' };
  if (isAdmin(me)) return { ok:true };
  if ((row.email||'').toLowerCase() === String(me.email||'').toLowerCase()){
    return { ok:true };
  }
  const u = await db.get(`
    SELECT role, manageAll, manageUnits, manageChiBo, userUnit, userChiBo
    FROM users WHERE id=?`, me.id);

  const role = u?.role || 'user';
  const isSuper = (role === 'manager_all') || !!u?.manageAll;

  if (role === 'user'){
    if ((row.email||'').toLowerCase() === (me.email||'').toLowerCase()) return { ok:true };
    return { ok:false, code:403, error:'Chỉ được sửa hồ sơ cá nhân của bạn' };
  }
  if (isSuper) return { ok:true };

  const units = parseArrayOrCSV(u?.manageUnits);
  const chis  = parseArrayOrCSV(u?.manageChiBo);
  const userUnit = u?.userUnit ? [u.userUnit] : [];
  const allowedUnits = (units.length ? units : userUnit);

  const inUnit = allowedUnits.length ? allowedUnits.map(String).includes(String(row.donViBoPhan||'')) : false;
  const inChi  = chis.length ? chis.includes(row.chiBo||'') : false;

  if (inUnit || inChi) return { ok:true };
  return { ok:false, code:403, error:'Hồ sơ nằm ngoài phạm vi quản lý' };
}

/* ===================== VĂN BẢN – UPLOAD TỔ CHỨC ===================== */
app.post("/documents/upload", upload.single("file"), checkUploadRules, async (req, res) => {
  await db.ready;
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Chưa chọn tệp" });
    if (!currentUser(req)) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });

    const auth = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });
    const rootId = await ensureRootFolder(drive, process.env.DRIVE_ROOT_FOLDER_NAME || "PartyDocsRoot");

    const d = req.query.date ? new Date(req.query.date) : new Date();
    const folderId = await ensureYearMonthFolder(drive, rootId, d.getFullYear(), d.getMonth() + 1);
    const decodeLatin1 = (s) => Buffer.from(s, "latin1").toString("utf8");
    const originalName = decodeLatin1(req.file.originalname).replace(/[\/\\]/g, "／");

    const {
      soHieu="", tenTep="", loai="", mucDo="", donVi="",
      hanXuLy="", nguoiGui="", nguoiPhuTrach="", nhan="", trichYeu="", flow=""
    } = req.body || {};

    const flowFixed = flow ? String(flow) : "den"; // mặc định 'den'

    const meta = {
      name: originalName,
      parents: [folderId],
      description: trichYeu || undefined,
      appProperties: {
        soHieu, loai, mucDo, donVi, hanXuLy, nguoiGui, nguoiPhuTrach, nhan,
        tenTepHienThi: tenTep || "", uploadedDate: (req.query.date || "").toString(), flow: flowFixed
      }
    };
    const media = { mimeType: req.file.mimetype, body: fs.createReadStream(req.file.path) };
    const r = await drive.files.create({ requestBody: meta, media, fields: "id,name,webViewLink,webContentLink,createdTime" });
    fs.unlink(req.file.path, ()=>{});

    await db.run(`
      INSERT INTO docs (id,name,soHieu,loai,mucDo,donVi,hanXuLy,nguoiGui,nguoiPhuTrach,nhan,trichYeu,uploadedDate,webViewLink,flow,createdAt)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT (id) DO UPDATE SET
        name=EXCLUDED.name,soHieu=EXCLUDED.soHieu,loai=EXCLUDED.loai,mucDo=EXCLUDED.mucDo,donVi=EXCLUDED.donVi,
        hanXuLy=EXCLUDED.hanXuLy,nguoiGui=EXCLUDED.nguoiGui,nguoiPhuTrach=EXCLUDED.nguoiPhuTrach,nhan=EXCLUDED.nhan,
        trichYeu=EXCLUDED.trichYeu,uploadedDate=EXCLUDED.uploadedDate,webViewLink=EXCLUDED.webViewLink,flow=EXCLUDED.flow,createdAt=EXCLUDED.createdAt
    `,
      r.data.id, r.data.name, soHieu, loai, mucDo, donVi, hanXuLy, nguoiGui, nguoiPhuTrach,
      nhan, trichYeu, (req.query.date || "").toString(), r.data.webViewLink || null, flowFixed,
      (r.data.createdTime || new Date().toISOString()).replace("T"," ").replace("Z","")
    );

    res.json({ ok:true, message:"Tải lên thành công.", fileId:r.data.id, name:r.data.name, webViewLink:r.data.webViewLink, webContentLink:r.data.webContentLink });
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).json({ ok:false, error:"invalid_grant – Vào /auth/admin/drive để cấp quyền lại." });
    }
    res.status(500).json({ ok:false, error:e.message });
  }
});

/* ===================== UPLOAD / QUẢN LÝ LƯU TRỮ CÁ NHÂN ===================== */
app.post("/personal/upload", upload.single("file"), checkUploadRules, async (req,res)=>{
  await db.ready;
  try{
    const me = currentUser(req);
    if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });
    if (!req.file) return res.status(400).json({ ok:false, error:"Chưa chọn tệp" });

    const ownerEmail = me.email;
    const auth = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });
    const perRoot = await ensurePersonalFolder(drive, ownerEmail);
    const d = req.query.date ? new Date(req.query.date) : new Date();
    const folderId = await ensureYearMonthFolder(drive, perRoot, d.getFullYear(), d.getMonth()+1);

    const decodeLatin1 = (s) => Buffer.from(s, "latin1").toString("utf8");
    const originalName = decodeLatin1(req.file.originalname).replace(/[\/\\]/g, "／");

    const { nhan="", trichYeu="" } = req.body||{};
    const meta = {
      name: originalName, parents:[folderId], description: trichYeu||undefined,
      appProperties:{ flow:"personal", ownerEmail, tenTepHienThi: originalName, nhan, trichYeu, uploadedDate:(req.query.date||"").toString() }
    };
    const media = { mimeType: req.file.mimetype, body: fs.createReadStream(req.file.path) };
    const r = await drive.files.create({ requestBody: meta, media, fields:"id,name,webViewLink,createdTime" });
    fs.unlink(req.file.path, ()=>{});

    // cấp quyền reader cho email Google của user
    try {
      const emails = await resolveViewEmailsForUser(me.id, ownerEmail);
      for (const em of emails) {
        try {
          await withRetry(() => drive.permissions.create({
            fileId: r.data.id,
            requestBody: { type: 'user', role: 'reader', emailAddress: em },
            sendNotificationEmail: false,
            supportsAllDrives: true
          }));
          await db.run("INSERT INTO shares(fileId,email,role,notified,message) VALUES (?,?,?,?,?)",
            r.data.id, em, "reader", 0, null);
        } catch (e) {
          const msg = String(e?.message||'').toLowerCase();
          if (msg.includes("not a valid google account")) {
            console.warn("[personal/upload] skip grant (non-google account):", em);
          } else {
            console.warn("[personal/upload] grant fail", { fileId: r.data.id, em, err: e?.message });
          }
        }
      }
    } catch {}

    await db.run(`
      INSERT INTO docs (id,name,nhan,trichYeu,uploadedDate,webViewLink,flow,ownerEmail,createdAt)
      VALUES (?,?,?,?,?,?,?,?,?)
      ON CONFLICT (id) DO UPDATE SET
        name=EXCLUDED.name, nhan=EXCLUDED.nhan, trichYeu=EXCLUDED.trichYeu,
        uploadedDate=EXCLUDED.uploadedDate, webViewLink=EXCLUDED.webViewLink,
        flow=EXCLUDED.flow, ownerEmail=EXCLUDED.ownerEmail, createdAt=EXCLUDED.createdAt
    `,
      r.data.id, r.data.name, nhan, trichYeu, (req.query.date||"").toString(), r.data.webViewLink||null, "personal", ownerEmail,
      (r.data.createdTime || new Date().toISOString()).replace("T"," ").replace("Z","")
    );

    res.json({ ok:true, link: `/documents/${encodeURIComponent(r.data.id)}/open`, id: r.data.id, name: r.data.name });
  }catch(e){
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).json({ ok:false, error:"invalid_grant – Vào /auth/admin/drive để cấp quyền lại." });
    }
    res.status(500).json({ ok:false, error:e.message });
  }
});

// Danh sách file cá nhân của chính user đang đăng nhập
app.get("/personal/search", async (req, res) => {
  await db.ready;
  try {
    const me = currentUser(req);
    if (!me) return res.status(401).json({ ok: false, error: "Cần đăng nhập" });

    const text = String(req.query?.text || "").trim();
    const wh = ["flow='personal'", "ownerEmail=?"];
    const params = [me.email];

    if (text) {
      text.split(/\s+/).slice(0, 5).forEach(() => {
        wh.push("name ILIKE ?");
        params.push(`%${text}%`);
      });
    }

    const rows = await db.all(`
      SELECT id, name, createdAt
      FROM docs
      WHERE ${wh.join(" AND ")}
      ORDER BY createdAt DESC
      LIMIT 300
    `, ...params);

    const items = rows.map(r => ({
      id: r.id,
      name: r.name,
      createdAt: r.createdAt,
      webViewLink: `/documents/${encodeURIComponent(r.id)}/open?proxy=1`,
      openUrl: `/documents/${encodeURIComponent(r.id)}/open?proxy=1`
    }));

    return res.json({ ok: true, items });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.delete("/personal/:id", async (req,res)=>{
  await db.ready;
  try{
    const me = currentUser(req);
    if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });
    const d = await db.get("SELECT id,ownerEmail FROM docs WHERE id=? AND flow='personal'", req.params.id);
    if (!d || d.ownerEmail !== me.email) return res.status(403).json({ ok:false, error:"Không có quyền xóa" });

    const auth = await authAsCentral();
    const drive = google.drive({ version:"v3", auth });
    try { await drive.files.delete({ fileId: d.id, supportsAllDrives: true }); } catch {}

    await db.run("DELETE FROM docs WHERE id=?", d.id);
    await db.run("DELETE FROM shares WHERE fileId=?", d.id);
    res.json({ ok:true });
  }catch(e){
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).json({ ok:false, error:"invalid_grant – Vào /auth/admin/drive để cấp quyền lại." });
    }
    res.status(500).json({ ok:false, error:e.message });
  }
});

/* ===================== CHIA SẺ, TÌM KIẾM, LATEST ===================== */
app.post("/documents/share", async (req, res) => {
  await db.ready;
  try {
    const me = currentUser(req);
    if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });

    const { fileId, role, notify, message, mode, target, email } = req.body || {};
    if (!fileId) return res.status(400).json({ ok:false, error:"Thiếu fileId" });

    let recipients = [];
    const rRole = (role === 'writer') ? 'writer' : 'reader';

    const canShare = (u, m) => {
      if (!u) return false;
      if (u.role === 'admin' || u.role === 'manager_all') return true;
      if (u.role === 'manager_chibo') return (m === 'chibo' || m === 'user');
      if (u.role === 'manager_unit')  return (m === 'donvi' || m === 'user');
      return (m === 'user') ? true : false;
    };

    if (!mode || mode === 'user') {
      if (!email) return res.status(400).json({ ok:false, error:"Thiếu email người nhận" });
      if (!canShare(me,'user')) return res.status(403).json({ ok:false, error:"Không đủ quyền chia sẻ cá nhân" });
      recipients = [email.trim().toLowerCase()];
    } else if (mode === 'all') {
      if (!canShare(me,'all')) return res.status(403).json({ ok:false, error:"Không đủ quyền chia sẻ toàn bộ" });
      const rows = await db.all("SELECT email FROM users WHERE active=1");
      recipients = rows.map(r=>r.email).filter(Boolean);
    } else if (mode === 'chibo') {
      if (!target) return res.status(400).json({ ok:false, error:"Thiếu tên Chi bộ" });
      if (!canShare(me,'chibo')) return res.status(403).json({ ok:false, error:"Không đủ quyền chia sẻ theo Chi bộ" });
      const rows = await db.all(
        "SELECT DISTINCT u.email FROM users u JOIN members m ON m.email=u.email WHERE u.active=1 AND COALESCE(m.chiBo,'')=?",
        target
      );
      recipients = rows.map(r=>r.email);
    } else if (mode === 'donvi') {
      if (!target) return res.status(400).json({ ok:false, error:"Thiếu Đơn vị/Bộ phận" });
      if (!canShare(me,'donvi')) return res.status(403).json({ ok:false, error:"Không đủ quyền chia sẻ theo Đơn vị/Bộ phận" });
      const rows = await db.all(
        "SELECT DISTINCT u.email FROM users u JOIN members m ON m.email=u.email WHERE u.active=1 AND COALESCE(m.donViBoPhan,'')=?",
        target
      );
      recipients = rows.map(r=>r.email);
    } else {
      return res.status(400).json({ ok:false, error:"mode không hợp lệ" });
    }

    const seen = new Set(); const list = [];
    for (const e of recipients) {
      const em = String(e||"").trim().toLowerCase();
      if (!em) continue;
      if (seen.has(em)) continue;
      seen.add(em); list.push(em);
    }
    if (!list.length) return res.status(400).json({ ok:false, error:"Không có người nhận phù hợp" });

    const auth = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });

    const results = await pMapLimit(list, 5, async (em) => {
      try {
        await withRetry(() => drive.permissions.create({
          fileId,
          requestBody: { type:"user", role: rRole, emailAddress: em },
          sendNotificationEmail: notify !== false,
          emailMessage: message || undefined,
          supportsAllDrives: true
        }));
        await db.run("INSERT INTO shares (fileId,email,role,notified,message) VALUES (?,?,?,?,?)",
          fileId, em, rRole, notify!==false?1:0, message||null);
        return { ok: true };
      } catch (err) {
        return { ok: false, error: err?.message || String(err) };
      }
    });

    let okCount = 0, failCount = 0;
    for (const r of results) {
      if (r && r.ok !== false) okCount++; else failCount++;
    }

    const file = await drive.files.get({
      fileId,
      fields: "id,name,webViewLink",
      supportsAllDrives: true
    });

    res.json({ ok:true, message:`Chia sẻ: thành công ${okCount}, lỗi ${failCount}.`, link:file.data.webViewLink, total:list.length, okCount, failCount });
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).json({ ok:false, error:"invalid_grant – Vào /auth/admin/drive để cấp quyền lại." });
    }
    res.status(500).json({ ok:false, error:e.message });
  }
});

app.get("/documents/search", async (req, res) => {
  await db.ready;
  try {
    const { from, to, loai, mucDo, donVi, text, flow, sender, receiver, dueFrom, dueTo } = req.query || {};
    const wh = []; const args = [];

    if (flow === 'den' || flow === 'di') {
      wh.push("flow=?"); args.push(flow);
    } else {
      wh.push("flow IN ('den','di')");
    }

    if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
    if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
    if (loai){ wh.push("loai=?"); args.push(loai); }
    if (mucDo){ wh.push("mucDo=?"); args.push(mucDo); }
    if (donVi){ wh.push("donVi=?"); args.push(donVi); }
    if (sender){   wh.push("nguoiGui ILIKE ?"); args.push(`%${sender}%`); }
    if (receiver){ wh.push("nguoiGui ILIKE ?"); args.push(`%${receiver}%`); } // giữ tương thích
    if (dueFrom){ wh.push("hanXuLy>=?"); args.push(dueFrom); }
    if (dueTo){   wh.push("hanXuLy<=?"); args.push(dueTo); }
    if (text && text.trim()){
      text.trim().split(/\s+/).slice(0,5).forEach(t=>{ wh.push("name ILIKE ?"); args.push(`%${t}%`); });
    }

    const acl = await docACL(req);
    const whereParts = [];
    if (wh.length) whereParts.push(wh.join(' AND '));
    if (acl.clause) whereParts.push(`(${acl.clause})`);
    const whereSql = whereParts.length ? ('WHERE ' + whereParts.join(' AND ')) : '';

    const rows = await db.all(`
      SELECT id,name,soHieu,loai,mucDo,donVi,nguoiGui,hanXuLy,trichYeu,webViewLink,createdAt
      FROM docs
      ${whereSql}
      ORDER BY createdAt DESC LIMIT 300
    `, ...args, ...acl.params);

    res.json({ ok:true, count:rows.length, files: rows.map(r=>({
      id:r.id,
      name:r.name,
      webViewLink: `/documents/${encodeURIComponent(r.id)}/open`,
      gdocLink: r.webViewLink,
      openUrl: `/documents/${encodeURIComponent(r.id)}/open`,
      createdAt:r.createdAt,
      modifiedTime:r.createdAt,
      appProperties:{
        soHieu:r.soHieu, loai:r.loai, mucDo:r.mucDo, donVi:r.donVi,
        nguoiGui:r.nguoiGui, hanXuLy:r.hanXuLy, trichYeu:r.trichYeu
      }
    }))});
  } catch (e) { res.status(500).json({ ok:false, error:e.message }); }
});

app.get("/documents/latest", async (req, res) => {
  await db.ready;
  try {
    const limit = Math.min(Number(req.query.limit || 8), 50);
    const acl = await docACL(req);
    const wh = ["flow IN ('den','di')"];
    if (acl.clause) wh.push(`(${acl.clause})`);
    const whereSql = `WHERE ${wh.join(" AND ")}`;
    const rows = await db.all(`
      SELECT id,name,soHieu,mucDo,nguoiGui,hanXuLy,trichYeu,webViewLink,createdAt
      FROM docs ${whereSql}
      ORDER BY createdAt DESC LIMIT ?
    `, ...acl.params, limit);

    const items = rows.map(r => ({ ...r, webViewLink: `/documents/${encodeURIComponent(r.id)}/open` }));
    res.json({ ok:true, items });
  } catch (e) { res.status(500).json({ ok:false, error:e.message }); }
});

/* ===================== DOWNLOAD & OPEN (Google Viewer) ===================== */
app.get("/documents/:id/download", async (req, res) => {
  try {
    const chk = await canAccessDocById(req, req.params.id);
    if (!chk.ok) return res.status(chk.code).send(chk.error || "Forbidden");

    const auth = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });

    const meta = await drive.files.get({
      fileId: req.params.id,
      fields: "name, mimeType",
      supportsAllDrives: true
    });

    const filename = meta.data.name || "file";
    const mime = meta.data.mimeType || "application/octet-stream";
    const inline = String(req.query.inline || "") === "1";

    const isGoogleApp = mime.startsWith("application/vnd.google-apps");
    if (isGoogleApp) {
      const exportMime = "application/pdf";
      res.setHeader(
        "Content-Disposition",
        `${inline ? "inline" : "attachment"}; filename*=UTF-8''${encodeURIComponent(
          filename.replace(/\.[^.]+$/, "") + ".pdf"
        )}`
      );
      res.type(exportMime);

      const stream = await drive.files.export(
        { fileId: req.params.id, mimeType: exportMime },
        { responseType: "stream" }
      );
      stream.data.on("error", () => res.end());
      return stream.data.pipe(res);
    }

    res.setHeader(
      "Content-Disposition",
      `${inline ? "inline" : "attachment"}; filename*=UTF-8''${encodeURIComponent(filename)}`
    );
    res.type(mime);

    const stream = await drive.files.get(
      { fileId: req.params.id, alt: "media", supportsAllDrives: true },
      { responseType: "stream" }
    );
    stream.data.on("error", () => res.end());
    stream.data.pipe(res);
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).send("Tải tệp lỗi: invalid_grant – Vào /auth/admin/drive để cấp quyền lại.");
    }
    res.status(500).send("Tải tệp lỗi: " + e.message);
  }
});

app.get("/documents/:id/open", async (req, res) => {
  try {
    const me  = currentUser(req);
    const chk = await canAccessDocById(req, req.params.id);
    if (!chk.ok) return res.status(chk.code).send(chk.error || "Forbidden");

    const auth  = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });

    const f = await drive.files.get({
      fileId: req.params.id,
      fields: "id,name,mimeType,webViewLink,owners",
      supportsAllDrives: true
    });

    const row = await db.get("SELECT id, flow, ownerEmail FROM docs WHERE id=?", req.params.id) || {};

    let wantProxy = String(req.query.proxy || "") === "1";
    if (!wantProxy && row.flow === "personal") wantProxy = true;

    if (!wantProxy && me?.email && f.data?.id) {
      const emails = await resolveViewEmailsForUser(me.id || me.email, me.email);
      for (const em of emails) {
        const key = `${f.data.id}:${em}:reader`;
        if (!shouldGrantNow(key)) continue;
        try {
          await withRetry(() => drive.permissions.create({
            fileId: f.data.id,
            requestBody: { type: "user", role: "reader", emailAddress: em },
            sendNotificationEmail: false,
            supportsAllDrives: true
          }));
          try {
            await db.run("INSERT INTO shares(fileId,email,role,notified,message) VALUES (?,?,?,?,?)",
              f.data.id, em, "reader", 0, null);
          } catch {}
        } catch {}
      }
    }

    if (wantProxy || !f.data.webViewLink) {
      return res.redirect(`/documents/${encodeURIComponent(req.params.id)}/download?inline=1`);
    }
    return res.redirect(f.data.webViewLink);
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).send("Open lỗi: invalid_grant – Vào /auth/admin/drive để cấp quyền lại.");
    }
    res.status(500).send("Open lỗi: " + e.message);
  }
});

app.get("/documents/:id/preview", async (req, res) => {
  try {
    const chk = await canAccessDocById(req, req.params.id);
    if (!chk.ok) return res.status(chk.code).send(`<p style="font:14px system-ui">Lỗi: ${chk.error}</p>`);

    const auth = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });

    const meta = await drive.files.get({
      fileId: req.params.id,
      fields: "id, name, mimeType, webViewLink",
      supportsAllDrives: true
    });

    const name = meta.data.name || "file";
    const mime = (meta.data.mimeType || "").toLowerCase();

    const INLINEABLE = (
      mime.startsWith("image/") ||
      mime === "application/pdf" ||
      mime.startsWith("text/")
    );

    const shellTop = `<!doctype html><meta charset="utf-8">
<style>
  body{margin:0;background:#111}
  .bar{position:fixed;inset:0 0 auto 0;display:flex;gap:10px;align-items:center;padding:8px 12px;background:#111;color:#fff}
  .name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:60vw}
  .viewer{position:fixed;top:44px;left:0;right:0;bottom:0;background:#222}
  .btn{appearance:none;border:1px solid #444;background:#222;color:#fff;border-radius:8px;padding:6px 10px;text-decoration:none;display:inline-block}
</style>
<div class="bar">
  <div class="name">${name.replace(/</g,"&lt;")}</div>
  <div style="flex:1"></div>
  <a class="btn" href="/documents/${encodeURIComponent(req.params.id)}/download">Tải file</a>
</div>
<div class="viewer">`;
    const shellBottom = `</div>`;

    if (INLINEABLE) {
      const src = `/documents/${encodeURIComponent(req.params.id)}/download?inline=1`;
      return res
        .status(200)
        .type("text/html")
        .send(`${shellTop}<iframe src="${src}" style="width:100%;height:100%;border:0"></iframe>${shellBottom}`);
    }

    let canDirect = !!meta.data.webViewLink;
    if (canDirect) {
      let hadAnyGrant = false;
      let grantDeniedAll = false;
      try {
        const me = currentUser(req);
        if (me?.email && meta.data?.id) {
          const auth2  = await authAsCentral();
          const drive2 = google.drive({ version: "v3", auth: auth2 });
          const emails = await resolveViewEmailsForUser(me.id || me.email, me.email);

          for (const em of emails) {
            const key = `${meta.data.id}:${em}:reader`;
            if (!shouldGrantNow(key)) { hadAnyGrant = true; continue; }
            try {
              await withRetry(() => drive2.permissions.create({
                fileId: meta.data.id,
                requestBody: { type: "user", role: "reader", emailAddress: em },
                sendNotificationEmail: false,
                supportsAllDrives: true
              }));
              hadAnyGrant = true;
              try {
                await db.run("INSERT INTO shares(fileId,email,role,notified,message) VALUES (?,?,?,?,?)",
                  meta.data.id, em, "reader", 0, null);
              } catch {}
            } catch (e) {
              const msg = String(e?.message||"").toLowerCase();
              if (msg.includes("not a valid google account")) grantDeniedAll = true;
            }
          }
        }
        if (grantDeniedAll) canDirect = false;
        if (hadAnyGrant) { await sleep(300); }
      } catch {}
    }
    if (canDirect) return res.redirect(meta.data.webViewLink);

    return res
      .status(200)
      .type("text/html")
      .send(`${shellTop}
    <div style="color:#ddd;display:flex;align-items:center;justify-content:center;height:100%;text-align:center;padding:24px">
      <div>
        <div style="font:16px system-ui;margin-bottom:10px">
          Không xem trước bằng Google Viewer được. Hãy bấm <b>Tải file</b> để mở bằng ứng dụng tương ứng.
        </div>
      </div>
    </div>
  ${shellBottom}`);
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res.status(401).type("text/html").send(`<p style="font:14px system-ui">Lỗi xem trước: invalid_grant – Vào /auth/admin/drive để cấp quyền lại.</p>`);
    }
    res.status(500).type("text/html").send(`<p style="font:14px system-ui">Lỗi xem trước: ${e.message}</p>`);
  }
});

/* ===================== ĐẢNG VIÊN – API ===================== */
app.get("/members/distinct", async (req,res)=>{
  await db.ready;
  const field = req.query.field;
  const allow = {chiBo:1,dangUyCapUy:1,donViBoPhan:1};
  if (!allow[field]) return res.json({ ok:false, items:[] });
  const rows = await db.all(`SELECT DISTINCT ${field} AS v FROM members WHERE COALESCE(${field},'')<>'' ORDER BY v`);
  res.json({ ok:true, items:rows.map(r=>r.v) });
});

app.get("/me/member-exists", async (req,res)=>{
  await db.ready;
  const me = currentUser(req);
  if (!me) return res.json({ ok:true, exists:false });
  const row = await db.get("SELECT 1 FROM members WHERE LOWER(COALESCE(email,''))=? LIMIT 1",
    String(me.email||'').toLowerCase());
  res.json({ ok:true, exists: !!row });
});

app.get("/members", async (req, res) => {
  await db.ready;
  try {
    const { text, chiBo, from, to } = req.query || {};
    const acl = await memberACL(req);

    const wh = [];
    const args = [];

    if (chiBo) { wh.push("chiBo=?"); args.push(chiBo); }
    if (text && text.trim()) {
      const t = text.trim();
      wh.push("(hoTen ILIKE ? OR soTheDang ILIKE ? OR soCCCD ILIKE ? OR email ILIKE ?)");
      args.push(`%${t}%`,`%${t}%`,`%${t}%`,`%${t}%`);
    }
    if (from) { wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
    if (to)   { wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
    if (acl.memberClause) { wh.push(`(${acl.memberClause})`); args.push(...acl.memberParams); }

    const rows = await db.all(`
      SELECT *
      FROM members
      ${wh.length ? 'WHERE ' + wh.join(' AND ') : ''}
      ORDER BY chiBo, hoTen
      LIMIT 500
    `, ...args);

    res.json({ ok:true, items: rows });
  } catch (e) {
    res.status(500).json({ ok:false, error:e.message });
  }
});

app.get("/members/:id(\\d+)", async (req, res) => {
  await db.ready;
  try {
    const acl = await memberACL(req);
    let row;

    if (acl.memberClause) {
      row = await db.get(`
        SELECT * FROM members
        WHERE id=? AND (${acl.memberClause})
        LIMIT 1
      `, req.params.id, ...acl.memberParams);
    } else {
      row = await db.get(`SELECT * FROM members WHERE id=?`, req.params.id);
    }

    if (!row) return res.status(404).json({ ok:false, error:"Không tìm thấy hồ sơ hoặc không có quyền xem" });
    res.json({ ok:true, item: row });
  } catch (e) {
    res.status(500).json({ ok:false, error:e.message });
  }
});

app.post("/members", async (req,res)=>{
  await db.ready;
  const me = currentUser(req);
  if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });

  const m = req.body||{};

  if (req.session?.user?.role === 'user'){
    m.email = (me.email || '').trim().toLowerCase();
    if (!m.email) return res.status(400).json({ ok:false, error:"Không xác định được email người dùng" });
    const existed = await db.get("SELECT id FROM members WHERE LOWER(COALESCE(email,''))=? LIMIT 1", m.email);
    if (existed){
      return res.status(400).json({ ok:false, error:"Bạn đã có hồ sơ cá nhân. Mỗi người chỉ được tạo 1 lần." });
    }
  } else if (m.email){
    const existed = await db.get("SELECT id FROM members WHERE LOWER(COALESCE(email,''))=? LIMIT 1", String(m.email).toLowerCase());
    if (existed){
      return res.status(400).json({ ok:false, error:"Email này đã có hồ sơ trên hệ thống" });
    }
  }

  try{
    await db.run(`
      INSERT INTO members
      (hoTen,tenGoiKhac,ngaySinh,gioiTinh,dienThoai,danToc,tonGiao,ksTinh,ksHuyen,ksXa,ngayVaoDang,ngayChinhThuc,
       soTheDang,soCCCD,ngayCapCCCD,chiBo,dangUyCapUy,donViBoPhan,email,ngayBatDauSH,ngayKetThucSH,trangThai,ghiChu)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `,
      m.hoTen||"", m.tenGoiKhac||"", m.ngaySinh||"", m.gioiTinh||"", m.dienThoai||"", m.danToc||"", m.tonGiao||"",
      m.ksTinh||"", m.ksHuyen||"", m.ksXa||"", m.ngayVaoDang||"", m.ngayChinhThuc||"", m.soTheDang||"",
      m.soCCCD||"", m.ngayCapCCCD||"", m.chiBo||"", m.dangUyCapUy||"", m.donViBoPhan||"", (m.email||"").trim().toLowerCase(),
      m.ngayBatDauSH||"", m.ngayKetThucSH||"", m.trangThai||"", m.ghiChu||""
    );
    res.json({ ok:true });
  }catch(e){
    if (String(e.message||'').toLowerCase().includes('unique') && String(m.email||'').trim()){
      return res.status(400).json({ ok:false, error:"Email này đã có hồ sơ trên hệ thống" });
    }
    res.status(400).json({ ok:false, error:e.message });
  }
});

app.put("/members/:id(\\d+)", async (req,res)=>{
  await db.ready;
  const me = currentUser(req);
  if (!me) return res.status(401).json({ ok:false, error:"Cần đăng nhập" });

  const row = await db.get("SELECT * FROM members WHERE id=?", req.params.id);
  const chk = await canEditMember(req, row);
  if (!chk.ok) return res.status(chk.code||403).json({ ok:false, error:chk.error||'Forbidden' });

  const m=req.body||{};
  if (req.session?.user?.role === 'user'){
    m.email = (me.email || '').trim().toLowerCase();
  }else if (m.email){
    m.email = String(m.email).trim().toLowerCase();
  }

  await db.run(`UPDATE members SET
    hoTen=?,tenGoiKhac=?,ngaySinh=?,gioiTinh=?,dienThoai=?,danToc=?,tonGiao=?,
    ksTinh=?,ksHuyen=?,ksXa=?,ngayVaoDang=?,ngayChinhThuc=?,soTheDang=?,soCCCD=?,ngayCapCCCD=?,
    chiBo=?,dangUyCapUy=?,donViBoPhan=?,email=?,ngayBatDauSH=?,ngayKetThucSH=?,trangThai=?,ghiChu=?
    WHERE id=?`,
    m.hoTen||"", m.tenGoiKhac||"", m.ngaySinh||"", m.gioiTinh||"", m.dienThoai||"", m.danToc||"", m.tonGiao||"",
    m.ksTinh||"", m.kshuyen||m.ksHuyen||"", m.ksXa||"", m.ngayVaoDang||"", m.ngayChinhThuc||"", m.soTheDang||"", m.soCCCD||"", m.ngayCapCCCD||"",
    m.chiBo||"", m.dangUyCapUy||"", m.donViBoPhan||"", m.email||row.email||"", m.ngayBatDauSH||"", m.ngayKetThucSH||"", m.trangThai||"", m.ghiChu||"",
    req.params.id
  );
  res.json({ ok:true });
});

app.delete("/members/:id(\\d+)", ensureAdmin, async (req,res)=>{
  await db.ready;
  await db.run("DELETE FROM members WHERE id=?", req.params.id);
  res.json({ ok:true });
});

/* ======== Excel TEMPLATE & IMPORT – ĐẢNG VIÊN ======== */
const GENDER_LIST = ["Nam","Nữ","Khác"];
const STATUS_LIST = ["Đang sinh hoạt","Nghỉ sinh hoạt","Chuyển Đảng"];
function parseVNDate(s){
  if (!s) return "";
  if (s instanceof Date && !isNaN(+s)) return s.toISOString().slice(0,10);
  if (typeof s === "number") {
    const d = XLSX.SSF.parse_date_code(s);
    if (d) return new Date(Date.UTC(d.y, d.m-1, d.d)).toISOString().slice(0,10);
  }
  const m = String(s).trim().match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})$/);
  if (!m) return "";
  const dd = Number(m[1]), mm = Number(m[2]), yraw = Number(m[3]);
  const yy = yraw < 100 ? 2000 + yraw : yraw;
  const dt = new Date(yy, mm-1, dd);
  if (dt && dt.getMonth()===mm-1 && dt.getDate()===dd) return dt.toISOString().slice(0,10);
  return "";
}
function isCCCD12(x){ return !!String(x||"").trim().match(/^\d{12}$/); }

app.get("/members/template.xlsx", async (req,res)=>{
  await db.ready;
  try{
    const chiBos = (await db.all("SELECT DISTINCT chiBo FROM members WHERE COALESCE(chiBo,'')<>'' ORDER BY chiBo"))
      .map(r=>r.chiBo);
    let donVi = [];
    try {
      const cat = loadCatalogsFromDisk();
      donVi = (cat.donVi||[]).map(x=>x.label||x);
    } catch {}
    const capUy = ["Đảng ủy","Chi bộ","Chung"];

    const HEAD = [
      "STT","Họ và tên","Tên gọi khác","Ngày sinh","Giới tính","Điện thoại liên hệ",
      "Dân tộc","Tôn giáo","Tỉnh/TP nơi khai sinh","Quận/Huyện nơi khai sinh","Xã/Phường nơi khai sinh",
      "Ngày vào Đảng","Ngày chính thức","Số thẻ Đảng viên","Số CCCD","Ngày cấp CCCD",
      "Chi bộ","Đảng bộ/cấp ủy","Đơn vị/ Bộ phận","Gmail","Ngày bắt đầu sinh hoạt","Ngày kết thúc sinh hoạt","Trạng thái"
    ];

    const ws = XLSX.utils.aoa_to_sheet([HEAD]);
    ws["!cols"] = Array(HEAD.length).fill({ wch: 22 });

    const dv = [];
    const col = (idx)=>XLSX.utils.encode_col(idx);
    dv.push({ type:"list", sqref:`${col(4)}2:${col(4)}2000`, formulas:[`"${GENDER_LIST.join(",")}"`] });
    dv.push({ type:"list", sqref:`${col(16)}2:${col(16)}2000`, formulas:[`"${chiBos.join(",")}"`] });
    dv.push({ type:"list", sqref:`${col(17)}2:${col(17)}2000`, formulas:[`"${capUy.join(",")}"`] });
    dv.push({ type:"list", sqref:`${col(18)}2:${col(18)}2000`, formulas:[`"${donVi.join(",")}"`] });
    dv.push({ type:"list", sqref:`${col(22)}2:${col(22)}2000`, formulas:[`"${STATUS_LIST.join(",")}"`] });
    ws["!dataValidation"] = dv;

    const readme = XLSX.utils.aoa_to_sheet([
      ["README"], [""],
      ["• Sheet PartyMembers: điền dữ liệu theo mẫu. Không đổi thứ tự cột."],
      ["• Các cột ngày dùng định dạng dd/mm/yyyy. STT có thể bỏ trống."],
      ["• 'Giới tính', 'Chi bộ', 'Đảng bộ/cấp ủy', 'Đơn vị/ Bộ phận', 'Trạng thái' có danh sách chọn sẵn."],
      ["• Sau khi điền, vào chương trình: Đảng viên » Nhập từ Excel."]
    ]);

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "PartyMembers");
    XLSX.utils.book_append_sheet(wb, readme, "README");

    const buf = XLSX.write(wb, { type:"buffer", bookType:"xlsx" });
    res.setHeader("Content-Disposition", `attachment; filename="PartyMembers_Template.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){
    res.status(500).send("Tạo template lỗi: "+e.message);
  }
});

app.post("/members/import/preview", upload.single("file"), async (req,res)=>{
  try{
    if (!req.file) return res.status(400).json({ ok:false, error:"Chưa chọn tệp .xlsx" });
    const wb = XLSX.readFile(req.file.path);
    fs.unlink(req.file.path, ()=>{});
    const ws = wb.Sheets["PartyMembers"] || wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.status(400).json({ ok:false, error:"Không tìm thấy sheet dữ liệu" });
    const rows = XLSX.utils.sheet_to_json(ws, { defval:"", raw:true });
    if (!rows.length) return res.json({ ok:true, token:null, total:0, valid:0, invalid:0, errors:[] });

    function pick(o,k){ return (o[k]!==undefined?o[k]:o[k?.trim? k.trim():k]) || ""; }
    const valid=[], invalid=[];
    const seenCCCD=new Set(), seenThe=new Set();

    for (let i=0;i<rows.length;i++){
      const r=rows[i]; const line=i+2;
      const item = {
        hoTen: pick(r,"Họ và tên"),
        tenGoiKhac: pick(r,"Tên gọi khác"),
        ngaySinh: parseVNDate(pick(r,"Ngày sinh")),
        gioiTinh: pick(r,"Giới tính"),
        dienThoai: pick(r,"Điện thoại liên hệ"),
        danToc: pick(r,"Dân tộc"),
        tonGiao: pick(r,"Tôn giáo"),
        ksTinh: pick(r,"Tỉnh/TP nơi khai sinh"),
        ksHuyen: pick(r,"Quận/Huyện nơi khai sinh"),
        ksXa: pick(r,"Xã/Phường nơi khai sinh"),
        ngayVaoDang: parseVNDate(pick(r,"Ngày vào Đảng")),
        ngayChinhThuc: parseVNDate(pick(r,"Ngày chính thức")),
        soTheDang: String(pick(r,"Số thẻ Đảng viên")||"").trim(),
        soCCCD: String(pick(r,"Số CCCD")||"").trim(),
        ngayCapCCCD: parseVNDate(pick(r,"Ngày cấp CCCD")),
        chiBo: pick(r,"Chi bộ"),
        dangUyCapUy: pick(r,"Đảng bộ/cấp ủy"),
        donViBoPhan: pick(r,"Đơn vị/ Bộ phận"),
        email: pick(r,"Gmail"),
        ngayBatDauSH: parseVNDate(pick(r,"Ngày bắt đầu sinh hoạt")),
        ngayKetThucSH: parseVNDate(pick(r,"Ngày kết thúc sinh hoạt")),
        trangThai: pick(r,"Trạng thái")
      };

      const errs=[];
      if (!item.hoTen) errs.push("Thiếu Họ và tên");
      if (pick(r,"Ngày sinh") && !item.ngaySinh) errs.push("Ngày sinh sai định dạng");
      if (pick(r,"Ngày vào Đảng") && !item.ngayVaoDang) errs.push("Ngày vào Đảng sai định dạng");
      if (pick(r,"Ngày chính thức") && !item.ngayChinhThuc) errs.push("Ngày chính thức sai định dạng");
      if (item.ngayVaoDang && item.ngayChinhThuc && item.ngayChinhThuc < item.ngayVaoDang) errs.push("Chính thức < Vào Đảng");
      if (item.soCCCD && !isCCCD12(item.soCCCD)) errs.push("CCCD phải 12 số");

      if (item.soCCCD){ const key="S"+item.soCCCD; if (seenCCCD.has(key)) errs.push("Trùng CCCD trong file"); else seenCCCD.add(key); }
      if (item.soTheDang){ const key="T"+item.soTheDang; if (seenThe.has(key)) errs.push("Trùng Số thẻ trong file"); else seenThe.add(key); }

      if (errs.length) invalid.push({ line, error: errs.join("; "), ...item });
      else valid.push({ line, ...item });
    }

    const token = `imp_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const jsonPath = path.join(uploadsDir, token+".json");
    fs.writeFileSync(jsonPath, JSON.stringify({ valid, invalid, filename: req.file.originalname }, null, 2), "utf8");

    res.json({ ok:true, token, total: valid.length + invalid.length, valid: valid.length, invalid: invalid.length });
  }catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

app.get("/members/import/errors/:token", (req,res)=>{
  try{
    const fi = path.join(uploadsDir, req.params.token + ".json");
    if (!fs.existsSync(fi)) return res.status(404).send("Not found");
    const data = JSON.parse(fs.readFileSync(fi,"utf8") || "{}");
    const rows = (data.invalid||[]).map(x=>[
      x.line, x.hoTen,x.tenGoiKhac,x.ngaySinh,x.gioiTinh,x.dienThoai,x.danToc,x.tonGiao,x.ksTinh,x.ksHuyen,x.ksXa,
      x.ngayVaoDang,x.ngayChinhThuc,x.soTheDang,x.soCCCD,x.ngayCapCCCD,x.chiBo,x.dangUyCapUy,x.donViBoPhan,x.email,
      x.ngayBatDauSH,x.ngayKetThucSH,x.trangThai, x.error
    ]);
    const HEAD = ["Line","Họ và tên","Tên gọi khác","Ngày sinh","Giới tính","Điện thoại","Dân tộc","Tôn giáo","Tỉnh/TP","Quận/Huyện","Xã/Phường","Ngày vào Đảng","Ngày chính thức","Số thẻ","Số CCCD","Ngày cấp CCCD","Chi bộ","Đảng bộ/cấp ủy","Đơn vị/Bộ phận","Gmail","Ngày BĐ SH","Ngày KT SH","Trạng thái","Error"];
    const ws = XLSX.utils.aoa_to_sheet([HEAD, ...rows]);
    const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, "Errors");
    const buf = XLSX.write(wb,{type:"buffer",bookType:"xlsx"});
    res.setHeader("Content-Disposition", `attachment; filename="Import_Errors.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){ res.status(500).send(e.message); }
});

app.post("/members/import/confirm", async (req,res)=>{
  await db.ready;
  try{
    const { token } = req.body||{};
    if (!token) return res.status(400).json({ ok:false, error:"Thiếu token" });
    const fi = path.join(uploadsDir, token + ".json");
    if (!fs.existsSync(fi)) return res.status(400).json({ ok:false, error:"Token không hợp lệ/đã hết hạn" });
    const data = JSON.parse(fs.readFileSync(fi,"utf8") || "{}");
    const valid = data.valid || [];
    let inserted=0, updated=0;

    const tx = await db.transaction(async (sql)=>{
      for (const v of valid){
        const row = await sql.get("SELECT id FROM members WHERE (soCCCD<>'' AND soCCCD=?) OR (soTheDang<>'' AND soTheDang=?) LIMIT 1",
          v.soCCCD||"", v.soTheDang||"");
        if (row && row.id){
          await sql.run(`
            UPDATE members SET
              hoTen=?,tenGoiKhac=?,ngaySinh=?,gioiTinh=?,dienThoai=?,danToc=?,tonGiao=?,ksTinh=?,ksHuyen=?,ksXa=?,
              ngayVaoDang=?,ngayChinhThuc=?,soTheDang=?,soCCCD=?,ngayCapCCCD=?,chiBo=?,dangUyCapUy=?,donViBoPhan=?,
              email=?,ngayBatDauSH=?,ngayKetThucSH=?,trangThai=? WHERE id=?`,
            v.hoTen||"", v.tenGoiKhac||"", v.ngaySinh||"", v.gioiTinh||"", v.dienThoai||"", v.danToc||"", v.tonGiao||"",
            v.ksTinh||"", v.ksHuyen||"", v.ksXa||"", v.ngayVaoDang||"", v.ngayChinhThuc||"", v.soTheDang||"", v.soCCCD||"",
            v.ngayCapCCCD||"", v.chiBo||"", v.dangUyCapUy||"", v.donViBoPhan||"", v.email||"", v.ngayBatDauSH||"", v.ngayKetThucSH||"", v.trangThai||"", row.id
          );
          updated++;
        } else {
          await sql.run(`
            INSERT INTO members
            (hoTen,tenGoiKhac,ngaySinh,gioiTinh,dienThoai,danToc,tonGiao,ksTinh,ksHuyen,ksXa,ngayVaoDang,ngayChinhThuc,
             soTheDang,soCCCD,ngayCapCCCD,chiBo,dangUyCapUy,donViBoPhan,email,ngayBatDauSH,ngayKetThucSH,trangThai)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            v.hoTen||"", v.tenGoiKhac||"", v.ngaySinh||"", v.gioiTinh||"", v.dienThoai||"", v.danToc||"", v.tonGiao||"",
            v.ksTinh||"", v.ksHuyen||"", v.ksXa||"", v.ngayVaoDang||"", v.ngayChinhThuc||"", v.soTheDang||"", v.soCCCD||"",
            v.ngayCapCCCD||"", v.chiBo||"", v.dangUyCapUy||"", v.donViBoPhan||"", v.email||"", v.ngayBatDauSH||"", v.ngayKetThucSH||"", v.trangThai||""
          );
          inserted++;
        }
      }
    });
    await tx;

    try{ fs.unlinkSync(fi); }catch{}
    res.json({ ok:true, inserted, updated, total: valid.length });
  }catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

// --- helper: format YYYY-MM-DD -> dd/mm/yyyy
function fmtVNDate(s){
  if (!s) return "";
  try{
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(s))) {
      const [y,m,d] = String(s).split("-");
      return `${d}/${m}/${y}`;
    }
    const d = new Date(s);
    if (!isNaN(d)) {
      const dd = String(d.getDate()).padStart(2,'0');
      const mm = String(d.getMonth()+1).padStart(2,'0');
      const yy = String(d.getFullYear());
      return `${dd}/${mm}/${yy}`;
    }
    return String(s);
  }catch{
    return String(s||"");
  }
}

/* ===================== BÁO CÁO ===================== */
app.get("/reports/members/detail", async (req,res)=>{
  await db.ready;
  const { text, chiBo, from, to } = req.query||{};

  const acl = await memberACL(req);

  const wh=[]; const args=[];
  if (chiBo){ wh.push("chiBo=?"); args.push(chiBo); }
  if (text && text.trim()){
    const t=text.trim(); wh.push("(hoTen ILIKE ? OR soTheDang ILIKE ? OR soCCCD ILIKE ? OR email ILIKE ?)");
    args.push(`%${t}%`,`%${t}%`,`%${t}%`,`%${t}%`);
  }
  if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
  if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
  if (acl.memberClause){ wh.push(`(${acl.memberClause})`); args.push(...acl.memberParams); }

  const rows = await db.all(`SELECT * FROM members ${wh.length?'WHERE '+wh.join(' AND '):''} ORDER BY chiBo, hoTen`, ...args);
  res.json({ ok:true, items:rows });
});

app.get("/reports/members/detail.xlsx", async (req,res)=>{
  await db.ready;
  try{
    const { text, chiBo, from, to } = req.query||{};
    const acl = await memberACL(req);

    const wh=[]; const args=[];
    if (chiBo){ wh.push("chiBo=?"); args.push(chiBo); }
    if (text && text.trim()){
      const t=text.trim(); wh.push("(hoTen ILIKE ? OR soTheDang ILIKE ? OR soCCCD ILIKE ? OR email ILIKE ?)");
      args.push(`%${t}%`,`%${t}%`,`%${t}%`,`%${t}%`);
    }
    if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
    if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
    if (acl.memberClause){ wh.push(`(${acl.memberClause})`); args.push(...acl.memberParams); }

    const rows = await db.all(`
      SELECT * FROM members
      ${wh.length?'WHERE '+wh.join(' AND '):''}
      ORDER BY chiBo, hoTen
    `, ...args);

    if (!rows.length){ return res.status(200).send("Không có dữ liệu."); }

    const HEAD = [
      "STT","Họ và tên","Tên gọi khác","Ngày sinh","Giới tính",
      "Điện thoại liên hệ","Dân tộc","Tôn giáo",
      "Tỉnh/TP nơi khai sinh","Quận/Huyện nơi khai sinh","Xã/Phường nơi khai sinh",
      "Ngày vào Đảng","Ngày chính thức","Tuổi Đảng",
      "Số thẻ Đảng viên","Số CCCD","Ngày cấp CCCD",
      "Chi bộ","Đảng bộ/cấp ủy","Đơn vị/ Bộ phận","Gmail",
      "Ngày bắt đầu sinh hoạt","Ngày kết thúc sinh hoạt","Trạng thái"
    ];

    const AOA = [HEAD];
    rows.forEach((m,i)=>{
      const tuoiDang = m.ngayVaoDang
        ? Math.max(0, Math.floor((Date.now()-new Date(m.ngayVaoDang))/31557600000))
        : "";
      AOA.push([
        i+1, m.hoTen||"", m.tenGoiKhac||"", fmtVNDate(m.ngaySinh),
        m.gioiTinh||"", m.dienThoai||"", m.danToc||"", m.tonGiao||"",
        m.ksTinh||"", m.ksHuyen||"", m.ksXa||"",
        fmtVNDate(m.ngayVaoDang), fmtVNDate(m.ngayChinhThuc), tuoiDang,
        m.soTheDang||"", m.soCCCD||"", fmtVNDate(m.ngayCapCCCD),
        m.chiBo||"", m.dangUyCapUy||"", m.donViBoPhan||"",
        m.email||"", fmtVNDate(m.ngayBatDauSH), fmtVNDate(m.ngayKetThucSH),
        m.trangThai||""
      ]);
    });

    const ws = XLSX.utils.aoa_to_sheet(AOA);
    ws['!cols'] = HEAD.map(()=>({ wch: 22 }));
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "BaoCao_DangVien");

    const buf = XLSX.write(wb,{type:"buffer", bookType:"xlsx"});
    res.setHeader("Content-Disposition", `attachment; filename="BaoCao_ChiTiet_DangVien.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){
    res.status(500).send(e.message);
  }
});

app.get("/reports/members/summary", async (req,res)=>{
  await db.ready;
  const { from,to } = req.query||{};

  const acl = await memberACL(req);

  const wh=[]; const args=[];
  if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
  if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
  if (acl.memberClause){ wh.push(`(${acl.memberClause})`); args.push(...acl.memberParams); }

  const rows = await db.all(`
    SELECT chiBo, COUNT(*) as soLuong
    FROM members
    ${wh.length?'WHERE '+wh.join(' AND '):''}
    GROUP BY chiBo
    ORDER BY soLuong DESC
  `, ...args);

  res.json({ ok:true, items:rows });
});

app.get("/reports/docs/deployed", async (req,res)=>{
  await db.ready;
  const { from,to,level } = req.query||{};
  const wh=["flow IN ('den','di')"]; const args=[];
  if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
  if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
  if (level){ wh.push("donVi=?"); args.push(level); }

  const acl = await docACL(req);
  const whereParts = [wh.join(' AND ')];
  if (acl.clause) whereParts.push(`(${acl.clause})`);
  const whereSql = 'WHERE ' + whereParts.join(' AND ');

  const rows = await db.all(`
    SELECT id,name,soHieu,donVi,mucDo,nguoiGui,trichYeu,webViewLink,createdAt
    FROM docs ${whereSql} ORDER BY createdAt DESC LIMIT 500
  `, ...args, ...acl.params);
  res.json({ ok:true, items: rows.map(r => ({ ...r, openUrl: `/documents/${encodeURIComponent(r.id)}/open` })) });
});

app.get("/reports/docs/deployed.xlsx", async (req,res)=>{
  await db.ready;
  try{
    const { from,to,level } = req.query||{};
    const wh=["flow IN ('den','di')"]; const args=[];
    if (from){ wh.push("createdAt>=?"); args.push(`${from} 00:00:00`); }
    if (to){   wh.push("createdAt<=?"); args.push(`${to} 23:59:59`); }
    if (level){ wh.push("donVi=?"); args.push(level); }

    const acl = await docACL(req);
    const whereParts = [wh.join(' AND ')];
    if (acl.clause) whereParts.push(`(${acl.clause})`);
    const whereSql = 'WHERE ' + whereParts.join(' AND ');

    const rows = await db.all(`
      SELECT id,name,soHieu,donVi,mucDo,nguoiGui,trichYeu,createdAt
      FROM docs ${whereSql} ORDER BY createdAt DESC LIMIT 2000
    `, ...args, ...acl.params);

    const HEAD = ["Tên","Trích yếu","Số hiệu","Cấp","Mức độ","Người gửi","Ngày","ID"];
    const AOA  = [HEAD].concat(rows.map(r=>[
      r.name||"", r.trichYeu||"", r.soHieu||"", r.donVi||"", r.mucDo||"", r.nguoiGui||"", r.createdAt||"", r.id||""
    ]));

    const ws = XLSX.utils.aoa_to_sheet(AOA);
    ws['!cols'] = HEAD.map(()=>({wch:26}));
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "BaoCao_VanBan");
    const buf = XLSX.write(wb,{type:"buffer",bookType:"xlsx"});

    res.setHeader("Content-Disposition", `attachment; filename="BaoCao_VanBan_DaTrienKhai.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){
    res.status(500).send(e.message);
  }
});

/* ======== QUẢN TRỊ NGƯỜI DÙNG ======== */
const VALID_ROLES = ['user','manager_unit','manager_chibo','manager_all','admin'];
const toBool = v => (v===true || v===1 || v==='1');
function normEmail(e){ return String(e||'').trim().toLowerCase(); }
function normalizeUserBody(body = {}) {
  const email = normEmail(body.email);
  const roleRaw = body.role || 'user';
  const role = VALID_ROLES.includes(roleRaw) ? roleRaw : 'user';

  const manageUnits = Array.isArray(body.manageUnitsIds) && body.manageUnitsIds.length
    ? body.manageUnitsIds
    : (Array.isArray(body.manageUnits) ? body.manageUnits : []);

  const userUnit = body.userUnitId || body.userUnit || null;
  const newPassword = (body.newPassword || body.password || '').toString();
  const manageAll = (role === 'admin' || role === 'manager_all') ? 1 : (toBool(body.manageAll) ? 1 : 0);

  return {
    email,
    fullName: body.fullName || '',
    partyId:  body.partyId  || '',
    role,
    manageAll,
    manageUnits,
    manageChiBo: Array.isArray(body.manageChiBo) ? body.manageChiBo : [],
    userUnit,
    userChiBo: body.userChiBo || null,
    newPassword
  };
}

app.get("/admin/users", ensureAdmin, async (req,res)=>{
  await db.ready;
  const rows = await db.all(`SELECT id,email,fullName,partyId,role,active,createdAt,
                                  scopes,manageAll,manageUnits,manageChiBo,userUnit,userChiBo
                           FROM users ORDER BY createdAt DESC`);
  const items = rows.map(u => ({
    ...u,
    scopes: safeParseJSON(u.scopes, {}),
    manageAll: !!u.manageAll,
    manageUnits: parseArrayOrCSV(u.manageUnits),
    manageChiBo: parseArrayOrCSV(u.manageChiBo)
  }));

  res.json({ ok:true, items });
});

app.post("/admin/users", ensureAdmin, async (req,res)=>{
  await db.ready;
  const nb = normalizeUserBody(req.body);
  if (!nb.email) return res.status(400).json({ ok:false, error:"Thiếu email" });

  const hash = bcrypt.hashSync(
    nb.newPassword && nb.newPassword.trim()
      ? nb.newPassword.trim()
      : Math.random().toString(36).slice(2),
    10
  );

  try{
    await db.run(`INSERT INTO users(email,fullName,partyId,role,hash,scopes,manageAll,manageUnits,manageChiBo,userUnit,userChiBo,active)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,1)`,
      nb.email, nb.fullName, nb.partyId, nb.role, hash,
      "{}", nb.manageAll,
      JSON.stringify(nb.manageUnits||[]),
      JSON.stringify(nb.manageChiBo||[]),
      nb.userUnit || null,
      nb.userChiBo || null
    );
    res.json({ ok:true });
  }catch(e){ res.status(400).json({ ok:false, error:e.message }); }
});

async function updateUserByEmail(req, res){
  await db.ready;
  const emailKey = normEmail(req.params.email);
  if (!emailKey) return res.status(400).json({ ok:false, error:"Thiếu email trên URL" });

  const nb = normalizeUserBody({ ...req.body, email: req.body?.email || emailKey });

  const row = await db.get("SELECT * FROM users WHERE email=?", emailKey);
  if (!row) return res.status(404).json({ ok:false, error:"Không tìm thấy user" });

  const nextUserUnit  = (typeof nb.userUnit  !== 'undefined' && nb.userUnit  !== null) ? nb.userUnit  : row.userUnit;
  const nextUserChiBo = (typeof nb.userChiBo !== 'undefined' && nb.userChiBo !== null) ? nb.userChiBo : row.userChiBo;

  const params = {
    fullName: nb.fullName,
    partyId : nb.partyId,
    role    : nb.role,
    manageAll: nb.manageAll,
    manageUnits: JSON.stringify(nb.manageUnits||[]),
    manageChiBo: JSON.stringify(nb.manageChiBo||[]),
    userUnit: nextUserUnit || null,
    userChiBo: nextUserChiBo || null,
    id: row.id
  };

  const tx = await db.transaction(async (sql)=>{
    await sql.run(`UPDATE users SET
      fullName=@fullName, partyId=@partyId, role=@role, manageAll=@manageAll,
      manageUnits=@manageUnits, manageChiBo=@manageChiBo, userUnit=@userUnit, userChiBo=@userChiBo
      WHERE id=@id`, params);
    if (nb.newPassword && nb.newPassword.trim()){
      await sql.run("UPDATE users SET hash=? WHERE id=?", bcrypt.hashSync(nb.newPassword.trim(),10), row.id);
    }
  });
  await tx;

  try { await authAsCentral(); } catch(e) { /* ignore */ }

  res.json({ ok:true });
}
app.put("/admin/users/:email",  ensureAdmin, updateUserByEmail);
app.post("/admin/users/:email", ensureAdmin, updateUserByEmail);

app.post("/admin/users/update", ensureAdmin, async (req,res)=>{
  req.params.email = normEmail(req.body?.email);
  if (!req.params.email) return res.status(400).json({ ok:false, error:"Thiếu email" });
  await updateUserByEmail(req,res);
});

app.delete("/admin/users/:email", ensureAdmin, async (req,res)=>{
  await db.ready;
  const emailKey = normEmail(req.params.email);
  const row = await db.get("SELECT id FROM users WHERE email=?", emailKey);
  if (!row) return res.status(404).json({ ok:false, error:"Không tìm thấy user" });
  await db.run("DELETE FROM users WHERE id=?", row.id);
  res.json({ ok:true });
});

app.post("/admin/users/reset", ensureAdmin, async (req,res)=>{
  await db.ready;
  const email = normEmail(req.body?.email);
  const newPassword = (req.body?.newPassword || req.body?.password || '').trim();
  if (!email || !newPassword) return res.status(400).json({ ok:false, error:"Thiếu email/newPassword" });
  await db.run("UPDATE users SET hash=? WHERE email=?", bcrypt.hashSync(newPassword,10), email);
  res.json({ ok:true });
});

/* ---- Template & Import Users (Excel) ---- */
const USER_ROLES = ["user","manager_unit","manager_chibo","manager_all","admin"];
const ACTIVE_LIST = ["1","0"];

app.get("/users/template.xlsx", (req,res)=>{
  try{
    const HEAD = ["Email","Họ tên","Số thẻ","Vai trò (nhóm)","Mật khẩu (tùy chọn)","Active (1/0)","Chi bộ (tùy chọn)","Đơn vị/Bộ phận (tùy chọn)"];
    const ws = XLSX.utils.aoa_to_sheet([HEAD]);
    ws["!cols"] = Array(HEAD.length).fill({ wch: 26 });

    const dv=[]; const col = (i)=>XLSX.utils.encode_col(i);
    dv.push({ type:"list", sqref:`${col(3)}2:${col(3)}2000`, formulas:[`"${USER_ROLES.join(",")}"`] });
    dv.push({ type:"list", sqref:`${col(5)}2:${col(5)}2000`, formulas:[`"${ACTIVE_LIST.join(",")}"`] });
    ws["!dataValidation"] = dv;

    const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, "Users");
    const buf = XLSX.write(wb,{type:"buffer",bookType:"xlsx"});
    res.setHeader("Content-Disposition", `attachment; filename="Users_Template.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){ res.status(500).send(e.message); }
});

app.post("/users/import/preview", ensureAdmin, upload.single("file"), async (req,res)=>{
  try{
    if (!req.file) return res.status(400).json({ ok:false, error:"Chưa chọn tệp .xlsx" });
    const wb = XLSX.readFile(req.file.path); fs.unlink(req.file.path, ()=>{});
    const ws = wb.Sheets["Users"] || wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.status(400).json({ ok:false, error:"Không tìm thấy sheet Users" });
    const rows = XLSX.utils.sheet_to_json(ws, { defval:"" });

    const valid=[], invalid=[];
    for (let i=0;i<rows.length;i++){
      const r = rows[i], line=i+2;
      const item = {
        email: String(r["Email"]||"").trim().toLowerCase(),
        fullName: r["Họ tên"]||"",
        partyId: r["Số thẻ"]||"",
        role: String(r["Vai trò (nhóm)"]||"user").trim(),
        pass: r["Mật khẩu (tùy chọn)"]||"",
        active: String(r["Active (1/0)"]||"1").trim(),
        chiBo: r["Chi bộ (tùy chọn)"]||"",
        donViBoPhan: r["Đơn vị/Bộ phận (tùy chọn)"]||""
      };
      const errs=[];
      if (!item.email) errs.push("Thiếu Email");
      if (item.role && !USER_ROLES.includes(item.role)) errs.push("Vai trò không hợp lệ");
      if (item.active && !["1","0"].includes(item.active)) errs.push("Active phải 1/0");
      if (errs.length) invalid.push({ line, error: errs.join("; "), ...item });
      else valid.push({ line, ...item });
    }

    const token = `userimp_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    fs.writeFileSync(path.join(uploadsDir, token+".json"), JSON.stringify({ valid, invalid }, null, 2), "utf8");
    res.json({ ok:true, token, total: valid.length+invalid.length, valid: valid.length, invalid: invalid.length });
  }catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

app.get("/users/import/errors/:token", ensureAdmin, (req,res)=>{
  try{
    const fi = path.join(uploadsDir, req.params.token + ".json");
    if (!fs.existsSync(fi)) return res.status(404).send("Not found");
    const data = JSON.parse(fs.readFileSync(fi,"utf8") || "{}");
    const rows = (data.invalid||[]).map(x=>[
      x.line, x.email,x.fullName,x.partyId,x.role,x.active,x.error
    ]);
    const HEAD = ["Line","Email","Họ tên","Số thẻ","Vai trò","Active","Error"];
    const ws = XLSX.utils.aoa_to_sheet([HEAD, ...rows]);
    const wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, "Errors");
    const buf = XLSX.write(wb,{type:"buffer",bookType:"xlsx"});
    res.setHeader("Content-Disposition", `attachment; filename="Users_Import_Errors.xlsx"`);
    res.type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.send(buf);
  }catch(e){ res.status(500).send(e.message); }
});

app.post("/users/import/confirm", ensureAdmin, async (req,res)=>{
  await db.ready;
  try{
    const { token } = req.body||{};
    if (!token) return res.status(400).json({ ok:false, error:"Thiếu token" });
    const fi = path.join(uploadsDir, token + ".json");
    if (!fs.existsSync(fi)) return res.status(400).json({ ok:false, error:"Token không hợp lệ/đã hết hạn" });
    const data = JSON.parse(fs.readFileSync(fi,"utf8") || "{}");
    const valid = data.valid||[];
    let inserted=0, updated=0;

    const tx = await db.transaction(async (sql)=>{
      for (const v of valid){
        const row = await sql.get("SELECT id FROM users WHERE email=?", v.email);
        if (row){
          await sql.run("UPDATE users SET fullName=?, partyId=?, role=?, active=? WHERE id=?",
            v.fullName||"", v.partyId||"", (USER_ROLES.includes(v.role)?v.role:'user'), Number(v.active? v.active : 1), row.id);
          updated++;
        }else{
          const pass = v.pass && String(v.pass).trim() ? String(v.pass).trim() : Math.random().toString(36).slice(2);
          await sql.run("INSERT INTO users(email,fullName,partyId,role,hash,active) VALUES(?,?,?,?,?,?)",
            v.email, v.fullName||"", v.partyId||"", (USER_ROLES.includes(v.role)?v.role:'user'), bcrypt.hashSync(pass,10), Number(v.active? v.active : 1));
          inserted++;
        }
        if (v.chiBo || v.donViBoPhan){
          const m = await sql.get("SELECT id FROM members WHERE email=? ORDER BY id DESC LIMIT 1", v.email);
          if (m && m.id){
            await sql.run("UPDATE members SET chiBo=?, donViBoPhan=? WHERE id=?", v.chiBo||"", v.donViBoPhan||"", m.id);
          }else{
            await sql.run("INSERT INTO members(hoTen,email,chiBo,donViBoPhan) VALUES (?,?,?,?)",
              v.fullName||"", v.email, v.chiBo||"", v.donViBoPhan||"");
          }
        }
      }
    });
    await tx;
    try{ fs.unlinkSync(fi); }catch{}

    res.json({ ok:true, inserted, updated, total: valid.length });
  }catch(e){ res.status(500).json({ ok:false, error:e.message }); }
});

/* ===================== SYNC SHARES CHO USER ===================== */
app.post("/admin/users/sync-shares", ensureAdmin, async (req, res) => {
  await db.ready;
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    if (!email) return res.status(400).json({ ok: false, error: "Thiếu email" });

    const u = await db.get(
      "SELECT email, role, manageAll, manageUnits, manageChiBo FROM users WHERE email=?",
      email
    );
    if (!u) return res.status(404).json({ ok: false, error: "Không tìm thấy user" });

    // Admin: bỏ qua (không cần sync share)
    if (u.role === "admin") return res.json({ ok: true, skipped: "admin" });

    const manageAll = !!u.manageAll || u.role === "manager_all";
    const units = parseArrayOrCSV(u.manageUnits);
    const chis  = parseArrayOrCSV(u.manageChiBo);

    // Phạm vi cần đồng bộ (đơn vị + chi bộ)
    const scopes = Array.from(new Set([ ...(units || []), ...(chis || []) ]));

    // manager_all: không cần share thêm (proxy đã đủ)
    if (manageAll) {
      return res.json({ ok: true, updated: 0, note: "manager_all không cần đồng bộ" });
    }
    if (!scopes.length) {
      return res.json({ ok: true, updated: 0, note: "User không có phạm vi quản lý" });
    }

    const placeholders = scopes.map(() => "?").join(",");
    const docs = await db.all(
      `
      SELECT id
      FROM docs
      WHERE flow IN ('den','di')
        AND COALESCE(donVi,'') IN (${placeholders})
      ORDER BY createdAt DESC
      LIMIT 500
    `,
      ...scopes
    );

    if (!docs.length) return res.json({ ok: true, updated: 0 });

    // Cấp quyền reader trên Drive
    const auth  = await authAsCentral();
    const drive = google.drive({ version: "v3", auth });

    let ok = 0, fail = 0;
    for (const d of docs) {
      try {
        await drive.permissions.create({
          fileId: d.id,
          requestBody: { type: "user", role: "reader", emailAddress: email },
          sendNotificationEmail: false,
          supportsAllDrives: true
        });

        // Lưu bảng shares (idempotent sơ bộ; nếu trùng cứ bỏ qua lỗi)
        try {
          await db.run(
            "INSERT INTO shares(fileId,email,role,notified,message) VALUES (?,?,?,?,?)",
            d.id, email, "reader", 0, null
          );
        } catch (_) {}

        ok++;
      } catch (_) {
        fail++;
      }
    }

    return res.json({ ok: true, updated: ok, failed: fail, total: docs.length });
  } catch (e) {
    if (isInvalidGrant(e)) {
      try { if (fs.existsSync(GOOGLE_TOKEN_PATH)) fs.unlinkSync(GOOGLE_TOKEN_PATH); } catch {}
      return res
        .status(401)
        .json({ ok: false, error: "invalid_grant – Vào /auth/admin/drive để cấp quyền lại." });
    }
    return res.status(500).json({ ok: false, error: e.message });
  }
});

/* ===================== START ===================== */
const PORT = Number(process.env.PORT || 4000);
const HOST = process.env.HOST || '0.0.0.0';

app.listen(PORT, HOST, () => {
  const printableHost = (HOST === '0.0.0.0' || HOST === '::') ? 'localhost' : HOST;
  console.log(`Server listening at http://${printableHost}:${PORT}`);
});





