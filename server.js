/**
 * Techgati Quiz Monitor — Server
 *
 * Express + Socket.IO server:
 * - Serves registration page and real-time admin monitoring dashboard
 * - Receives heartbeats and security violation reports from Quiz Guard extension
 * - Persists participant sessions, audit trails, and violations in SQLite
 * - Exports exam results as CSV
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const os = require('os');

let Database;
let isNodeSqlite = false;
try {
  const { DatabaseSync } = require('node:sqlite');
  Database = DatabaseSync;
  isNodeSqlite = true;
} catch (e) {
  Database = require('better-sqlite3');
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.redirect('/register.html'));
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

const db = new Database(path.join(__dirname, 'quiz_monitor.db'));
if (isNodeSqlite) {
  db.exec('PRAGMA journal_mode = WAL;');
} else {
  db.pragma('journal_mode = WAL');
}

db.exec(`
  CREATE TABLE IF NOT EXISTS participants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    roll TEXT NOT NULL,
    seat TEXT DEFAULT '',
    registered_at TEXT NOT NULL,
    last_heartbeat TEXT,
    warning_count INTEGER DEFAULT 0,
    is_connected INTEGER DEFAULT 1,
    ip_address TEXT DEFAULT '',
    time_remaining INTEGER DEFAULT 1800,
    status TEXT DEFAULT 'registered',
    submitted_at TEXT DEFAULT '',
    exam_started_at TEXT DEFAULT ''
  );

  CREATE TABLE IF NOT EXISTS violations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    participant_id TEXT NOT NULL,
    participant_name TEXT DEFAULT '',
    participant_roll TEXT DEFAULT '',
    type TEXT NOT NULL,
    details TEXT DEFAULT '{}',
    timestamp TEXT NOT NULL,
    warning_number INTEGER DEFAULT 0,
    FOREIGN KEY (participant_id) REFERENCES participants(id)
  );

  CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_violations_participant ON violations(participant_id);
  CREATE INDEX IF NOT EXISTS idx_violations_timestamp ON violations(timestamp);
`);

try { db.exec("ALTER TABLE participants ADD COLUMN time_remaining INTEGER DEFAULT 1800;"); } catch (e) {}
try { db.exec("ALTER TABLE participants ADD COLUMN status TEXT DEFAULT 'registered';"); } catch (e) {}
try { db.exec("ALTER TABLE participants ADD COLUMN submitted_at TEXT DEFAULT '';"); } catch (e) {}
try { db.exec("ALTER TABLE participants ADD COLUMN exam_started_at TEXT DEFAULT '';"); } catch (e) {}

const initConfig = db.prepare('INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)');
db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run('form_url', 'https://forms.gle/ahA9Hmb2tpZEVjXCA');
initConfig.run('quiz_active', 'false');
initConfig.run('quiz_started_at', '');

db.exec(`
  INSERT OR IGNORE INTO participants (id, name, roll, registered_at, status)
  VALUES ('unregistered', 'Unregistered Candidate', 'N/A', datetime('now'), 'registered');
  INSERT OR IGNORE INTO participants (id, name, roll, registered_at, status)
  VALUES ('unknown', 'Unknown Candidate', 'N/A', datetime('now'), 'registered');
`);

const stmts = {
  registerParticipant: db.prepare(`
    INSERT OR REPLACE INTO participants (id, name, roll, seat, registered_at, last_heartbeat, warning_count, is_connected, ip_address, time_remaining, status, exam_started_at, submitted_at)
    VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?, 1800, 'registered', '', '')
  `),
  startParticipantExam: db.prepare(`
    UPDATE participants SET
      status = CASE WHEN status = 'submitted' THEN 'submitted' ELSE 'in_progress' END,
      exam_started_at = CASE WHEN exam_started_at IS NULL OR exam_started_at = '' THEN ? ELSE exam_started_at END
    WHERE id = ?
  `),
  updateHeartbeat: db.prepare(`
    UPDATE participants SET
      last_heartbeat = ?,
      warning_count = ?,
      is_connected = 1,
      time_remaining = CASE WHEN (status = 'submitted' OR ? = 'submitted') THEN 0 ELSE COALESCE(?, time_remaining) END,
      status = CASE
        WHEN status = 'submitted' THEN 'submitted'
        WHEN ? = 'submitted' THEN 'submitted'
        WHEN ? = 'in_progress' THEN 'in_progress'
        ELSE status
      END,
      submitted_at = CASE
        WHEN (status = 'submitted' OR ? = 'submitted') AND (submitted_at IS NULL OR submitted_at = '') THEN ?
        ELSE submitted_at
      END,
      exam_started_at = CASE
        WHEN (exam_started_at IS NULL OR exam_started_at = '') AND (? = 'in_progress' OR ? < 1800) THEN ?
        ELSE exam_started_at
      END
    WHERE id = ?
  `),
  submitParticipant: db.prepare(`
    UPDATE participants SET
      status = 'submitted',
      time_remaining = 0,
      submitted_at = CASE WHEN (submitted_at IS NULL OR submitted_at = '') THEN ? ELSE submitted_at END
    WHERE id = ?
  `),
  insertViolation: db.prepare(`
    INSERT INTO violations (participant_id, participant_name, participant_roll, type, details, timestamp, warning_number)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `),
  incrementWarnings: db.prepare(`
    UPDATE participants SET warning_count = warning_count + 1 WHERE id = ?
  `),
  getAllParticipants: db.prepare('SELECT * FROM participants ORDER BY registered_at DESC'),
  getParticipant: db.prepare('SELECT * FROM participants WHERE id = ?'),
  getViolations: db.prepare('SELECT * FROM violations WHERE participant_id = ? ORDER BY timestamp DESC'),
  getAllViolations: db.prepare('SELECT * FROM violations ORDER BY timestamp DESC'),
  getRecentViolations: db.prepare('SELECT * FROM violations ORDER BY timestamp DESC LIMIT ?'),
  getConfig: db.prepare('SELECT value FROM config WHERE key = ?'),
  setConfig: db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)'),
  disconnectStale: db.prepare(`
    UPDATE participants SET is_connected = 0
    WHERE datetime(last_heartbeat) < datetime('now', '-15 seconds')
    AND is_connected = 1
  `),
  deleteAllData: db.prepare('DELETE FROM violations'),
  deleteAllParticipants: db.prepare('DELETE FROM participants'),
  getParticipantStats: db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN is_connected = 1 THEN 1 ELSE 0 END) as connected,
      SUM(CASE WHEN is_connected = 0 THEN 1 ELSE 0 END) as disconnected,
      SUM(warning_count) as total_warnings
    FROM participants
  `)
};

const staleCheckTimer = setInterval(() => {
  try {
    const changes = stmts.disconnectStale.run();
    if (changes.changes > 0) {
      io.to('admin').emit('participants_updated', getParticipantsData());
    }
  } catch (err) {
    console.error('[Monitor] Stale check error:', err);
  }
}, 10000);

function generateId() {
  return 'p_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8);
}

function getParticipantsData() {
  const rawParticipants = stmts.getAllParticipants.all().filter(p => p.id !== 'unregistered' && p.id !== 'unknown');
  const now = Date.now();
  const participants = rawParticipants.map(p => {
    if (p.status === 'submitted') {
      return { ...p, time_remaining: 0 };
    }
    if (p.status === 'in_progress' && (p.exam_started_at || p.registered_at)) {
      const startRef = p.exam_started_at || p.registered_at;
      const elapsed = Math.floor((now - new Date(startRef).getTime()) / 1000);
      const calculatedRemaining = Math.max(0, 1800 - elapsed);
      const rem = (typeof p.time_remaining === 'number' && p.time_remaining < 1800 && p.time_remaining >= 0)
        ? Math.min(p.time_remaining, calculatedRemaining)
        : calculatedRemaining;
      return { ...p, time_remaining: rem };
    }
    return p;
  });
  const stats = stmts.getParticipantStats.get();
  return { participants, stats };
}

function getNetworkIPs() {
  const interfaces = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        ips.push({ name, address: iface.address });
      }
    }
  }
  return ips;
}


app.post('/api/register', (req, res) => {
  try {
    const { name, roll, seat } = req.body;

    if (!name || !roll || !seat) {
      return res.status(400).json({ error: 'Name, Roll Number, and Seat / PC Number are required' });
    }

    const id = generateId();
    const now = new Date().toISOString();
    const ip = req.ip || req.socket?.remoteAddress || '';

    stmts.registerParticipant.run(id, name.trim(), roll.trim(), seat?.trim() || '', now, now, ip);

    const participant = { id, name: name.trim(), roll: roll.trim(), seat: seat?.trim() || '' };

    io.to('admin').emit('participant_registered', participant);
    io.to('admin').emit('participants_updated', getParticipantsData());

    console.log(`[Register] ${name} (${roll}) — ID: ${id}`);

    res.json({
      success: true,
      participant,
      formUrl: stmts.getConfig.get('form_url')?.value || ''
    });
  } catch (err) {
    console.error('[Register] Error:', err);
    res.status(500).json({ error: 'Failed to register participant' });
  }
});

app.post('/api/start-exam', (req, res) => {
  try {
    const { participantId } = req.body;
    if (participantId) {
      const now = new Date().toISOString();
      stmts.startParticipantExam.run(now, participantId);
      io.to('admin').emit('participants_updated', getParticipantsData());
    }
    res.json({ success: true });
  } catch (err) {
    console.error('[StartExam] Error:', err);
    res.status(500).json({ error: 'Failed to start exam' });
  }
});

app.post('/api/heartbeat', (req, res) => {
  try {
    const { participantId, warningCount, timeRemaining, status } = req.body;

    if (!participantId) {
      return res.status(400).json({ error: 'participantId required' });
    }

    const now = new Date().toISOString();
    const st = (status && status !== 'registered') ? status : (timeRemaining !== undefined && timeRemaining < 1800 ? 'in_progress' : null);
    const rem = timeRemaining !== undefined ? timeRemaining : 1800;

    stmts.updateHeartbeat.run(
      now,
      warningCount || 0,
      st,
      timeRemaining !== undefined ? timeRemaining : null,
      st,
      st,
      st,
      now,
      st,
      rem,
      now,
      participantId
    );

    io.to('admin').emit('participants_updated', getParticipantsData());
    res.json({ success: true });
  } catch (err) {
    console.error('[Heartbeat] Error:', err);
    res.status(500).json({ error: 'Failed to update heartbeat' });
  }
});

app.post('/api/submit', (req, res) => {
  try {
    const { participantId } = req.body;
    let targetId = participantId;

    if (!targetId || targetId === 'unregistered' || targetId === 'unknown') {
      const latest = db.prepare("SELECT id FROM participants WHERE status != 'submitted' AND id NOT IN ('unregistered', 'unknown') ORDER BY registered_at DESC LIMIT 1").get();
      if (latest) {
        targetId = latest.id;
      }
    }

    if (targetId) {
      const now = new Date().toISOString();
      stmts.submitParticipant.run(now, targetId);
      io.to('admin').emit('participants_updated', getParticipantsData());
      console.log(`[Submit] Participant ${targetId} successfully recorded as submitted at ${now}`);
    } else {
      console.warn('[Submit] Received submission request but no eligible participant found');
    }

    res.json({ success: true, participantId: targetId });
  } catch (err) {
    console.error('[Submit] Error:', err);
    res.status(500).json({ error: 'Failed to record submission' });
  }
});

app.post('/api/violation', (req, res) => {
  try {
    const {
      type, details, timestamp, warningNumber,
      participantId, participantName, participantRoll
    } = req.body;

    if (!type) {
      return res.status(400).json({ error: 'Violation type required' });
    }

    let id = participantId || 'unregistered';
    const existing = stmts.getParticipant.get(id);
    if (!existing) {
      id = 'unregistered';
    }
    const now = timestamp || new Date().toISOString();

    stmts.insertViolation.run(
      id,
      participantName || '',
      participantRoll || '',
      type,
      JSON.stringify(details || {}),
      now,
      warningNumber || 0
    );

    if (id !== 'unknown' && id !== 'unregistered') {
      stmts.incrementWarnings.run(id);
    }

    const violation = {
      participant_id: id,
      participant_name: participantName || 'Unknown',
      participant_roll: participantRoll || '',
      type,
      details,
      timestamp: now,
      warning_number: warningNumber
    };

    io.to('admin').emit('violation', violation);
    io.to('admin').emit('participants_updated', getParticipantsData());

    console.log(`[Violation] ${participantName || id}: ${type}`);

    res.json({ success: true });
  } catch (err) {
    console.error('[Violation] Error:', err);
    res.status(500).json({ error: 'Failed to record violation' });
  }
});

app.get('/api/participants', (req, res) => {
  try {
    res.json(getParticipantsData());
  } catch (err) {
    console.error('[Participants] Error:', err);
    res.status(500).json({ error: 'Failed to retrieve participants' });
  }
});

app.get('/api/violations/:participantId', (req, res) => {
  try {
    const violations = stmts.getViolations.all(req.params.participantId);
    res.json({ violations });
  } catch (err) {
    console.error('[Violations] Error:', err);
    res.status(500).json({ error: 'Failed to retrieve participant violations' });
  }
});

app.get('/api/violations', (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 200;
    const violations = stmts.getRecentViolations.all(limit);
    res.json({ violations });
  } catch (err) {
    console.error('[Violations] Error:', err);
    res.status(500).json({ error: 'Failed to retrieve violations' });
  }
});

app.get('/api/config', (req, res) => {
  try {
    const formUrl = stmts.getConfig.get('form_url')?.value || '';
    const quizActive = stmts.getConfig.get('quiz_active')?.value === 'true';
    const quizStartedAt = stmts.getConfig.get('quiz_started_at')?.value || '';
    res.json({ formUrl, quizActive, quizStartedAt });
  } catch (err) {
    console.error('[Config] Error:', err);
    res.status(500).json({ error: 'Failed to get configuration' });
  }
});

app.post('/api/config', (req, res) => {
  try {
    const { formUrl, quizActive } = req.body;
    if (formUrl !== undefined) {
      stmts.setConfig.run('form_url', formUrl);
    }
    if (quizActive !== undefined) {
      stmts.setConfig.run('quiz_active', quizActive.toString());
      if (quizActive) {
        stmts.setConfig.run('quiz_started_at', new Date().toISOString());
      }
    }
    io.to('admin').emit('config_updated', req.body);
    res.json({ success: true });
  } catch (err) {
    console.error('[Config] Error:', err);
    res.status(500).json({ error: 'Failed to update configuration' });
  }
});

app.get('/api/export/csv', (req, res) => {
  try {
    const participants = stmts.getAllParticipants.all().filter(p => p.id !== 'unregistered' && p.id !== 'unknown');
    const allViolations = stmts.getAllViolations.all();

    let csv = 'Name,Roll Number,Seat,Registration ID,Status,Registered At,Exam Started At,Submitted At,Total Time Taken,Warnings,Connected,Violations\n';

    for (const p of participants) {
      const pViolations = allViolations
        .filter(v => v.participant_id === p.id)
        .map(v => `${v.type} @ ${v.timestamp}`)
        .join(' | ');

      let timeTaken = 'Not started';
      const startRef = p.exam_started_at || p.registered_at;
      if (p.submitted_at && startRef) {
        const diffMs = Math.max(0, new Date(p.submitted_at) - new Date(startRef));
        const mins = Math.floor(diffMs / 60000);
        const secs = Math.floor((diffMs % 60000) / 1000);
        timeTaken = `${mins}m ${secs}s`;
      } else if (p.status === 'in_progress' && startRef) {
        const diffMs = Math.max(0, Date.now() - new Date(startRef));
        const mins = Math.floor(diffMs / 60000);
        const secs = Math.floor((diffMs % 60000) / 1000);
        timeTaken = `In Progress (${mins}m ${secs}s)`;
      } else if (p.status === 'registered') {
        timeTaken = 'Not started';
      }

      const regStr = p.registered_at ? new Date(p.registered_at).toLocaleString() : '';
      const startStr = p.exam_started_at ? new Date(p.exam_started_at).toLocaleString() : (p.status === 'registered' ? 'Not started' : '—');
      const subStr = p.submitted_at ? new Date(p.submitted_at).toLocaleString() : (p.status === 'submitted' ? 'Submitted' : '—');

      csv += [
        `"${(p.name || '').replace(/"/g, '""')}"`,
        `"${p.roll || ''}"`,
        `"${p.seat || ''}"`,
        `"${p.id || ''}"`,
        `"${p.status || 'registered'}"`,
        `"${regStr}"`,
        `"${startStr}"`,
        `"${subStr}"`,
        `"${timeTaken}"`,
        p.warning_count || 0,
        p.is_connected ? 'Yes' : 'No',
        `"${pViolations.replace(/"/g, '""')}"`
      ].join(',') + '\n';
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=techgati_quiz_results_${Date.now()}.csv`);
    res.send(csv);
  } catch (err) {
    console.error('[ExportCSV] Error:', err);
    res.status(500).send('Failed to export CSV results');
  }
});

app.post('/api/reset', (req, res) => {
  try {
    stmts.deleteAllData.run();
    stmts.deleteAllParticipants.run();
    db.exec(`
      INSERT OR IGNORE INTO participants (id, name, roll, registered_at, status)
      VALUES ('unregistered', 'Unregistered Candidate', 'N/A', datetime('now'), 'registered');
      INSERT OR IGNORE INTO participants (id, name, roll, registered_at, status)
      VALUES ('unknown', 'Unknown Candidate', 'N/A', datetime('now'), 'registered');
    `);
    io.to('admin').emit('participants_updated', getParticipantsData());
    console.log('[Admin] All participant data reset');
    res.json({ success: true });
  } catch (err) {
    console.error('[Reset] Error:', err);
    res.status(500).json({ error: 'Failed to reset data' });
  }
});

io.on('connection', (socket) => {
  socket.on('join_admin', () => {
    socket.join('admin');
    socket.emit('participants_updated', getParticipantsData());
  });
});

const PORT = process.env.PORT || 3000;
server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`\n[ERROR] Port ${PORT} is already in use.`);
    console.error(`Another monitoring server or process is currently using port ${PORT}.\n`);
    process.exit(1);
  } else {
    console.error('[Server Error]', err);
    process.exit(1);
  }
});
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[Techgati Quiz Monitor] Server listening on port ${PORT}`);
  console.log(`  Local:     http://localhost:${PORT}`);
  const ips = getNetworkIPs();
  for (const ip of ips) {
    console.log(`  Network:   http://${ip.address}:${PORT}`);
  }
  console.log(`  Dashboard: http://localhost:${PORT}/admin`);
  console.log(`  Register:  http://localhost:${PORT}/register.html\n`);
});

function gracefulShutdown() {
  console.log('\n[Server] Shutting down gracefully...');
  clearInterval(staleCheckTimer);
  server.close(() => {
    try {
      db.close();
    } catch (e) {}
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 3000);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
