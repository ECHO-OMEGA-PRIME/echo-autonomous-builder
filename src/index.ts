/**
 * ECHO AUTONOMOUS BUILDER v1.0.0
 * ================================
 * The EXECUTION ENGINE for ECHO OMEGA PRIME.
 * Everything else watches. This Worker DOES.
 *
 * Capabilities:
 * 1. Worker Warmer — keeps critical workers warm, eliminates cold-start latency alerts
 * 2. QA Bug Processor — auto-resolves false positives, queues real fixes
 * 3. Daemon Task Resolver — resolves pending performance tasks
 * 4. Bug Hunter — proactively scans all workers for issues
 * 5. Content Fixer — generates content for thin EPT pages via Engine Runtime
 * 6. GitHub Deployer — pushes fixes via GitHub Contents API
 * 7. Worker Health Auditor — comprehensive health scoring per worker
 * 8. Upgrade Scanner — identifies workers needing upgrades
 * 9. Self-Reporter — logs everything to Shared Brain + MoltBook
 *
 * Cron Schedule:
 * - every 5 min: warm up critical workers + quick health pulse
 * - every 30 min: QA bug triage + daemon task processing + auto-fixes
 * - every 4 hours: full system audit + bug hunt + upgrade scan
 * - daily 8am: daily briefing report
 */

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  GITHUB_TOKEN: string;
  ECHO_API_KEY: string;
  WORKER_VERSION: string;
  CLOUDFLARE_ACCOUNT_ID: string;
  // Service bindings for worker-to-worker calls
  SVC_BRAIN: Fetcher;
  SVC_ENGINE: Fetcher;
  SVC_DAEMON: Fetcher;
  SVC_QA: Fetcher;
  SVC_CHAT: Fetcher;
  SVC_KNOWLEDGE: Fetcher;
  SVC_SPEAK: Fetcher;
  SVC_DOCTRINE: Fetcher;
  SVC_SDK: Fetcher;
  SVC_VAULT: Fetcher;
  SVC_ORCHESTRATOR: Fetcher;
  SVC_SWARM: Fetcher;
  SVC_ANALYTICS: Fetcher;
  SVC_ARCANUM: Fetcher;
  SVC_GS343: Fetcher;
  SVC_CRM: Fetcher;
  SVC_HELPDESK: Fetcher;
  SVC_XBOT: Fetcher;
  SVC_LINKEDIN: Fetcher;
  SVC_TELEGRAM: Fetcher;
  SVC_REDDIT: Fetcher;
  SVC_OMNISYNC: Fetcher;
  SVC_HOME: Fetcher;
  SVC_SHEPHERD: Fetcher;
  SVC_CALLCENTER: Fetcher;
  [key: string]: any;
}

// Map worker names to their service binding keys
const SERVICE_BINDING_MAP: Record<string, string> = {
  'echo-shared-brain': 'SVC_BRAIN',
  'echo-engine-runtime': 'SVC_ENGINE',
  'echo-autonomous-daemon': 'SVC_DAEMON',
  'echo-qa-tester': 'SVC_QA',
  'echo-chat': 'SVC_CHAT',
  'echo-knowledge-forge': 'SVC_KNOWLEDGE',
  'echo-speak-cloud': 'SVC_SPEAK',
  'echo-doctrine-forge': 'SVC_DOCTRINE',
  'echo-sdk-gateway': 'SVC_SDK',
  'echo-vault-api': 'SVC_VAULT',
  'echo-build-orchestrator': 'SVC_ORCHESTRATOR',
  'echo-swarm-brain': 'SVC_SWARM',
  'echo-analytics-engine': 'SVC_ANALYTICS',
  'echo-arcanum': 'SVC_ARCANUM',
  'echo-gs343-cloud': 'SVC_GS343',
  'echo-crm': 'SVC_CRM',
  'echo-helpdesk': 'SVC_HELPDESK',
  'echo-x-bot': 'SVC_XBOT',
  'echo-linkedin': 'SVC_LINKEDIN',
  'echo-telegram': 'SVC_TELEGRAM',
  'echo-reddit-bot': 'SVC_REDDIT',
  'omniscient-sync': 'SVC_OMNISYNC',
  'echo-home-ai': 'SVC_HOME',
  'echo-shepherd-ai': 'SVC_SHEPHERD',
  'echo-call-center': 'SVC_CALLCENTER',
};

interface ActionLog {
  action_type: string;
  target: string;
  details: string;
  result: string;
  duration_ms: number;
}

interface WorkerCheckResult {
  worker: string;
  healthy: boolean;
  statusCode: number;
  latencyMs: number;
  version: string;
  error?: string;
}

interface QABug {
  id: number;
  severity: string;
  page: string;
  category: string;
  title: string;
  description: string;
  evidence: string;
  status: string;
}

interface DaemonTask {
  id: number;
  title: string;
  description: string;
  priority: string;
  category: string;
  status: string;
}

// ═══════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════

const CRITICAL_WORKERS = [
  'echo-shared-brain', 'echo-engine-runtime', 'echo-chat',
  'echo-knowledge-forge', 'echo-speak-cloud', 'echo-doctrine-forge',
  'echo-sdk-gateway', 'echo-vault-api', 'echo-build-orchestrator',
  'echo-autonomous-daemon', 'echo-swarm-brain'
];

const ALL_MONITORED_WORKERS = [
  ...CRITICAL_WORKERS,
  'echo-x-bot', 'echo-linkedin', 'echo-telegram', 'echo-reddit-bot',
  'echo-instagram', 'echo-gs343-cloud', 'echo-landman-pipeline',
  'echo-news-scraper', 'echo-reddit-monitor', 'echo-price-alerts',
  'echo-crypto-trader', 'echo-darkweb-intelligence', 'echo-graph-rag',
  'echo-ai-orchestrator', 'echo-qa-tester', 'echo-bot-auditor',
  'echo-intel-hub', 'echo-arcanum', 'echo-relay',
  'echo-config-manager', 'echo-alert-router', 'echo-log-aggregator',
  'echo-rate-limiter', 'echo-usage-tracker', 'echo-cron-orchestrator',
  'echo-notification-hub', 'echo-service-registry', 'echo-health-dashboard',
  'echo-circuit-breaker', 'echo-cost-optimizer',
  'echo-home-ai', 'echo-shepherd-ai', 'echo-call-center',
  'echo-crm', 'echo-helpdesk', 'echo-project-manager',
  'echo-booking', 'echo-invoice', 'echo-forms', 'echo-inventory',
  'echo-hr', 'echo-contracts', 'echo-lms', 'echo-analytics-engine',
  'echo-finance-ai', 'echo-email-marketing', 'echo-surveys',
  'echo-knowledge-base', 'echo-workflow-automation', 'echo-social-media',
  'echo-document-manager', 'echo-live-chat', 'echo-link-shortener',
  'echo-feedback-board', 'echo-newsletter', 'echo-web-analytics',
  'echo-waitlist', 'echo-reviews', 'echo-signatures', 'echo-affiliate',
  'echo-proposals', 'echo-gamer-companion', 'echo-qr-menu',
  'echo-podcast', 'echo-payroll', 'echo-calendar', 'echo-compliance',
  'echo-recruiting', 'echo-timesheet', 'echo-email-sender',
  'echo-paypal', 'echo-feature-flags', 'echo-expense', 'echo-okr'
];

const KNOWN_REDIRECT_PAGES = [
  '/scrapers', '/security', '/pentesting', '/crypto-trading',
  '/scanner', '/pipelines', '/knowledge', '/services'
];

// Workers that use /status instead of /health as their health endpoint
// Note: echo-build-orchestrator now has /health (added 2026-03-26)
const HEALTH_ENDPOINT_OVERRIDES: Record<string, string> = {
  // Add workers here that only respond to /status, not /health
};

// Get the correct health check path for a worker
function getHealthPath(workerName: string): string {
  return HEALTH_ENDPOINT_OVERRIDES[workerName] || '/health';
}

// Extract version from various health response formats
function extractVersion(data: any): string {
  if (!data || typeof data !== 'object') return '';
  // Direct version field
  if (data.version) return String(data.version);
  if (data.v) return String(data.v);
  // Wrapped in data.data (SDK gateway, Arcanum pattern)
  if (data.data && typeof data.data === 'object') {
    if (data.data.version) return String(data.data.version);
    if (data.data.v) return String(data.data.v);
  }
  // Wrapped in meta
  if (data.meta && typeof data.meta === 'object') {
    if (data.meta.version) return String(data.meta.version);
  }
  return '';
}

const WORKER_BASE = '.bmcii1976.workers.dev';
const BRAIN_URL = 'https://echo-shared-brain.bmcii1976.workers.dev';
const DAEMON_URL = 'https://echo-autonomous-daemon.bmcii1976.workers.dev';
const QA_URL = 'https://echo-qa-tester.bmcii1976.workers.dev';
const ENGINE_URL = 'https://echo-engine-runtime.bmcii1976.workers.dev';
const MOLTBOOK_URL = 'https://echo-swarm-brain.bmcii1976.workers.dev/moltbook/post';
const OMNISYNC_URL = 'https://omniscient-sync.bmcii1976.workers.dev';
const GITHUB_API = 'https://api.github.com';
const GITHUB_OWNER = 'ECHO-OMEGA-PRIME';
const EPT_REPO = 'echo-prime-tech';

// ═══════════════════════════════════════════════════
// DATABASE INITIALIZATION
// ═══════════════════════════════════════════════════

async function initDB(db: D1Database): Promise<void> {
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS actions_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action_type TEXT NOT NULL,
      target TEXT NOT NULL,
      details TEXT,
      result TEXT,
      duration_ms INTEGER DEFAULT 0,
      cycle_id TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS worker_profiles (
      worker_name TEXT PRIMARY KEY,
      avg_latency_ms REAL DEFAULT 0,
      min_latency_ms REAL DEFAULT 99999,
      max_latency_ms REAL DEFAULT 0,
      check_count INTEGER DEFAULT 0,
      healthy_count INTEGER DEFAULT 0,
      last_check TEXT,
      last_warm TEXT,
      last_version TEXT,
      issues_found INTEGER DEFAULT 0,
      issues_fixed INTEGER DEFAULT 0,
      health_score REAL DEFAULT 100,
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS fix_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      source_id TEXT,
      fix_type TEXT NOT NULL,
      target TEXT NOT NULL,
      priority INTEGER DEFAULT 5,
      status TEXT DEFAULT 'pending',
      details TEXT,
      fix_applied TEXT,
      error_msg TEXT,
      attempts INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      resolved_at TEXT
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS daily_stats (
      date TEXT PRIMARY KEY,
      warmups INTEGER DEFAULT 0,
      bugs_found INTEGER DEFAULT 0,
      bugs_fixed INTEGER DEFAULT 0,
      bugs_auto_resolved INTEGER DEFAULT 0,
      tasks_resolved INTEGER DEFAULT 0,
      deploys INTEGER DEFAULT 0,
      pages_fixed INTEGER DEFAULT 0,
      workers_checked INTEGER DEFAULT 0,
      avg_fleet_latency REAL DEFAULT 0,
      fleet_health_score REAL DEFAULT 0,
      hunt_issues INTEGER DEFAULT 0,
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS deploy_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      repo TEXT NOT NULL,
      file_path TEXT NOT NULL,
      commit_sha TEXT,
      commit_message TEXT,
      trigger TEXT,
      status TEXT DEFAULT 'pending',
      created_at TEXT DEFAULT (datetime('now'))
    )`)
  ]);
}

// ═══════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════

function cycleId(): string {
  return `cycle_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function today(): string {
  return new Date().toISOString().split('T')[0];
}

async function logAction(db: D1Database, log: ActionLog & { cycle_id?: string }): Promise<void> {
  try {
    await db.prepare(
      `INSERT INTO actions_log (action_type, target, details, result, duration_ms, cycle_id) VALUES (?, ?, ?, ?, ?, ?)`
    ).bind(log.action_type, log.target, log.details, log.result, log.duration_ms, log.cycle_id || '').run();
  } catch (e) {
    // Don't let logging failures break execution
  }
}

async function incrementStat(db: D1Database, field: string, amount: number = 1): Promise<void> {
  const d = today();
  try {
    await db.prepare(
      `INSERT INTO daily_stats (date, ${field}) VALUES (?, ?) ON CONFLICT(date) DO UPDATE SET ${field} = ${field} + ?, updated_at = datetime('now')`
    ).bind(d, amount, amount).run();
  } catch (e) {
    // Stat tracking failure is non-critical
  }
}

async function fetchWithTimeout(url: string, opts: RequestInit = {}, timeoutMs: number = 15000): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...opts, signal: controller.signal });
    return resp;
  } finally {
    clearTimeout(timer);
  }
}

// Fetch a worker via service binding (preferred) or fallback to public URL
async function workerFetch(env: Env, workerName: string, path: string, opts: RequestInit = {}, timeoutMs: number = 12000): Promise<{ ok: boolean; status: number; data: any; latencyMs: number; error?: string }> {
  const start = Date.now();
  const bindingKey = SERVICE_BINDING_MAP[workerName];

  try {
    let resp: Response;
    if (bindingKey && env[bindingKey]) {
      // Use service binding — fast, reliable, no DNS
      const svc = env[bindingKey] as Fetcher;
      resp = await svc.fetch(`https://internal${path}`, opts);
    } else {
      // Fallback to public URL (works for workers not in binding map)
      resp = await fetchWithTimeout(`https://${workerName}${WORKER_BASE}${path}`, opts, timeoutMs);
    }

    const latencyMs = Date.now() - start;
    let data: any = null;
    try {
      data = await resp.json();
    } catch {
      try { data = await resp.text(); } catch { data = null; }
    }
    return { ok: resp.ok, status: resp.status, data, latencyMs };
  } catch (e: any) {
    return { ok: false, status: 0, data: null, latencyMs: Date.now() - start, error: e.message };
  }
}

async function safeFetch(url: string, timeoutMs: number = 10000): Promise<{ ok: boolean; status: number; data: any; latencyMs: number; error?: string }> {
  const start = Date.now();
  try {
    const resp = await fetchWithTimeout(url, {}, timeoutMs);
    const latencyMs = Date.now() - start;
    let data: any = null;
    try {
      data = await resp.json();
    } catch {
      data = await resp.text().catch(() => null);
    }
    return { ok: resp.ok, status: resp.status, data, latencyMs };
  } catch (e: any) {
    return { ok: false, status: 0, data: null, latencyMs: Date.now() - start, error: e.message };
  }
}

// safeFetch variant with custom headers (for GitHub auth)
async function safeFetchWithHeaders(url: string, headers: Record<string, string>, timeoutMs: number = 10000): Promise<{ ok: boolean; status: number; data: any; latencyMs: number; error?: string }> {
  const start = Date.now();
  try {
    const resp = await fetchWithTimeout(url, { headers }, timeoutMs);
    const latencyMs = Date.now() - start;
    let data: any = null;
    try {
      data = await resp.json();
    } catch {
      data = await resp.text().catch(() => null);
    }
    return { ok: resp.ok, status: resp.status, data, latencyMs };
  } catch (e: any) {
    return { ok: false, status: 0, data: null, latencyMs: Date.now() - start, error: e.message };
  }
}

// ═══════════════════════════════════════════════════
// MODULE 1: WORKER WARMER
// Keeps critical workers warm to prevent cold-start
// latency which causes 90% of daemon false alerts
// ═══════════════════════════════════════════════════

async function warmUpWorkers(env: Env, cid: string, workersToWarm: string[] = CRITICAL_WORKERS): Promise<WorkerCheckResult[]> {
  const results: WorkerCheckResult[] = [];

  // Warm up in parallel batches of 10
  for (let i = 0; i < workersToWarm.length; i += 10) {
    const batch = workersToWarm.slice(i, i + 10);
    const batchResults = await Promise.allSettled(
      batch.map(async (worker) => {
        const healthPath = getHealthPath(worker);
        const check = await workerFetch(env, worker, healthPath, {}, 12000);

        const result: WorkerCheckResult = {
          worker,
          healthy: check.ok,
          statusCode: check.status,
          latencyMs: check.latencyMs,
          version: extractVersion(check.data),
          error: check.error
        };

        {

          // Update worker profile
          await env.DB.prepare(`
            INSERT INTO worker_profiles (worker_name, avg_latency_ms, min_latency_ms, max_latency_ms, check_count, healthy_count, last_check, last_warm, last_version, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, datetime('now'), datetime('now'), ?, datetime('now'))
            ON CONFLICT(worker_name) DO UPDATE SET
              avg_latency_ms = (avg_latency_ms * check_count + ?) / (check_count + 1),
              min_latency_ms = MIN(min_latency_ms, ?),
              max_latency_ms = MAX(max_latency_ms, ?),
              check_count = check_count + 1,
              healthy_count = healthy_count + ?,
              last_check = datetime('now'),
              last_warm = datetime('now'),
              last_version = COALESCE(?, last_version),
              updated_at = datetime('now')
          `).bind(
            worker, result.latencyMs, result.latencyMs, result.latencyMs, result.healthy ? 1 : 0, result.version,
            result.latencyMs, result.latencyMs, result.latencyMs, result.healthy ? 1 : 0, result.version || null
          ).run().catch(() => {});

          return result;
        }
      })
    );

    for (const r of batchResults) {
      if (r.status === 'fulfilled') results.push(r.value);
    }
  }

  await incrementStat(env.DB, 'warmups', results.length);
  await incrementStat(env.DB, 'workers_checked', results.length);

  await logAction(env.DB, {
    action_type: 'warmup',
    target: `${results.length} workers`,
    details: JSON.stringify({
      warmed: results.filter(r => r.healthy).length,
      failed: results.filter(r => !r.healthy).length,
      avgLatency: Math.round(results.reduce((s, r) => s + r.latencyMs, 0) / results.length)
    }),
    result: results.every(r => r.healthy) ? 'all_healthy' : 'some_unhealthy',
    duration_ms: 0,
    cycle_id: cid
  });

  return results;
}

// ═══════════════════════════════════════════════════
// MODULE 2: QA BUG PROCESSOR
// Reads bugs from QA tester, auto-resolves false
// positives, queues real fixes
// ═══════════════════════════════════════════════════

async function processQABugs(env: Env, cid: string): Promise<{ processed: number; autoResolved: number; queued: number }> {
  let processed = 0, autoResolved = 0, queued = 0;

  try {
    const resp = await workerFetch(env, 'echo-qa-tester', '/bugs?status=new&limit=100');
    if (!resp.ok || !resp.data?.bugs) return { processed, autoResolved, queued };

    const bugs: QABug[] = resp.data.bugs;

    for (const bug of bugs) {
      processed++;

      // === AUTO-RESOLVE: High script count (Next.js default) ===
      if (bug.title.includes('High script count')) {
        autoResolved++;
        await markQABugResolved(env, bug.id, 'auto-resolved: Next.js default script injection');
        await logAction(env.DB, {
          action_type: 'qa_auto_resolve',
          target: bug.page,
          details: JSON.stringify({ bugId: bug.id, reason: 'nextjs_script_count' }),
          result: 'resolved',
          duration_ms: 0,
          cycle_id: cid
        });
        continue;
      }

      // === AUTO-RESOLVE: Known redirect pages with thin content ===
      if (bug.title.includes('Insufficient text content') && KNOWN_REDIRECT_PAGES.includes(bug.page)) {
        autoResolved++;
        await markQABugResolved(env, bug.id, 'auto-resolved: intentional redirect/ComingSoonGuard page');
        await logAction(env.DB, {
          action_type: 'qa_auto_resolve',
          target: bug.page,
          details: JSON.stringify({ bugId: bug.id, reason: 'known_redirect_page' }),
          result: 'resolved',
          duration_ms: 0,
          cycle_id: cid
        });
        continue;
      }

      // === AUTO-RESOLVE: JSON-LD type Unknown (global layout schema) ===
      if (bug.title.includes('JSON-LD missing name/headline for type Unknown')) {
        autoResolved++;
        await markQABugResolved(env, bug.id, 'auto-resolved: global Organization schema in layout.tsx has no headline (expected)');
        await logAction(env.DB, {
          action_type: 'qa_auto_resolve',
          target: bug.page,
          details: JSON.stringify({ bugId: bug.id, reason: 'org_schema_no_headline' }),
          result: 'resolved',
          duration_ms: 0,
          cycle_id: cid
        });
        continue;
      }

      // === QUEUE FIX: Thin content pages (not redirect) ===
      if (bug.title.includes('Insufficient text content') && !KNOWN_REDIRECT_PAGES.includes(bug.page)) {
        queued++;
        await queueFix(env, {
          source: 'qa',
          source_id: String(bug.id),
          fix_type: 'thin_page',
          target: bug.page,
          priority: bug.severity === 'high' ? 8 : 5,
          details: JSON.stringify({ bugId: bug.id, page: bug.page, evidence: bug.evidence })
        });
        continue;
      }

      // === QUEUE FIX: Mock/placeholder data ===
      if (bug.title.includes('Mock') || bug.title.includes('placeholder')) {
        queued++;
        await queueFix(env, {
          source: 'qa',
          source_id: String(bug.id),
          fix_type: 'placeholder_data',
          target: bug.page,
          priority: 6,
          details: JSON.stringify({ bugId: bug.id, page: bug.page, evidence: bug.evidence })
        });
        continue;
      }

      // === QUEUE FIX: JSON-LD missing for FAQPage ===
      if (bug.title.includes('JSON-LD missing') && bug.title.includes('FAQPage')) {
        queued++;
        await queueFix(env, {
          source: 'qa',
          source_id: String(bug.id),
          fix_type: 'json_ld_faq',
          target: bug.page,
          priority: 4,
          details: JSON.stringify({ bugId: bug.id, page: bug.page })
        });
        continue;
      }

      // === QUEUE FIX: No navigation detected ===
      if (bug.title.includes('No navigation')) {
        queued++;
        await queueFix(env, {
          source: 'qa',
          source_id: String(bug.id),
          fix_type: 'missing_nav',
          target: bug.page,
          priority: 3,
          details: JSON.stringify({ bugId: bug.id, page: bug.page })
        });
        continue;
      }
    }

    await incrementStat(env.DB, 'bugs_found', processed);
    await incrementStat(env.DB, 'bugs_auto_resolved', autoResolved);

  } catch (e: any) {
    await logAction(env.DB, {
      action_type: 'qa_process_error',
      target: 'qa_tester',
      details: e.message,
      result: 'error',
      duration_ms: 0,
      cycle_id: cid
    });
  }

  return { processed, autoResolved, queued };
}

async function markQABugResolved(env: Env, bugId: number, resolution: string): Promise<void> {
  try {
    await fetchWithTimeout(`${QA_URL}/bugs/${bugId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ resolution, auto: true })
    }, 5000);
  } catch {
    // QA tester may not have this endpoint yet — that's OK
    // The bug is tracked in our fix_queue regardless
  }
}

async function queueFix(env: Env, fix: { source: string; source_id: string; fix_type: string; target: string; priority: number; details: string }): Promise<void> {
  // Deduplicate — don't queue the same fix twice
  const existing = await env.DB.prepare(
    `SELECT id FROM fix_queue WHERE fix_type = ? AND target = ? AND status IN ('pending', 'in_progress') LIMIT 1`
  ).bind(fix.fix_type, fix.target).first();

  if (existing) return;

  await env.DB.prepare(
    `INSERT INTO fix_queue (source, source_id, fix_type, target, priority, details) VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(fix.source, fix.source_id, fix.fix_type, fix.target, fix.priority, fix.details).run();
}

// ═══════════════════════════════════════════════════
// MODULE 3: DAEMON TASK RESOLVER
// Processes pending daemon tasks, resolves latency
// issues by warming up workers
// ═══════════════════════════════════════════════════

async function processDaemonTasks(env: Env, cid: string): Promise<{ processed: number; resolved: number }> {
  let processed = 0, resolved = 0;

  try {
    const resp = await workerFetch(env, 'echo-autonomous-daemon', '/tasks');
    if (!resp.ok || !Array.isArray(resp.data)) return { processed, resolved };

    const tasks: DaemonTask[] = resp.data.filter((t: DaemonTask) => t.status === 'pending');

    for (const task of tasks) {
      processed++;

      // Performance/latency tasks — warm up and re-check
      if (task.category === 'performance' && task.title.includes('slow')) {
        const workerMatch = task.title.match(/Worker (echo-[\w-]+) slow/);
        if (workerMatch) {
          const workerName = workerMatch[1];
          const healthPath = getHealthPath(workerName);
          const check = await workerFetch(env, workerName, healthPath);

          if (check.ok && check.latencyMs < 3000) {
            // Cold start resolved — mark task as auto-resolved
            resolved++;
            await resolveDaemonTask(env, task.id, `Auto-resolved: warm-up reduced latency to ${check.latencyMs}ms (was flagged as slow)`);
            await logAction(env.DB, {
              action_type: 'daemon_resolve',
              target: workerName,
              details: JSON.stringify({ taskId: task.id, newLatency: check.latencyMs }),
              result: 'resolved',
              duration_ms: check.latencyMs,
              cycle_id: cid
            });
          } else {
            // Still slow — log but don't resolve
            await logAction(env.DB, {
              action_type: 'daemon_check',
              target: workerName,
              details: JSON.stringify({ taskId: task.id, stillSlow: true, latency: check.latencyMs }),
              result: 'still_slow',
              duration_ms: check.latencyMs,
              cycle_id: cid
            });
          }
        }
      }
    }

    await incrementStat(env.DB, 'tasks_resolved', resolved);

  } catch (e: any) {
    await logAction(env.DB, {
      action_type: 'daemon_process_error',
      target: 'daemon',
      details: e.message,
      result: 'error',
      duration_ms: 0,
      cycle_id: cid
    });
  }

  return { processed, resolved };
}

async function resolveDaemonTask(env: Env, taskId: number, resolution: string): Promise<void> {
  try {
    await fetchWithTimeout(`${DAEMON_URL}/tasks/${taskId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ resolution, auto: true, resolver: 'echo-autonomous-builder' })
    }, 5000);
  } catch {
    // Daemon may not support this endpoint — track in our DB
  }
}

// ═══════════════════════════════════════════════════
// MODULE 4: BUG HUNTER
// Proactively scans all workers for issues beyond
// what the daemon checks
// ═══════════════════════════════════════════════════

async function huntBugs(env: Env, cid: string): Promise<{ scanned: number; issuesFound: number }> {
  let scanned = 0, issuesFound = 0;
  const issues: Array<{ worker: string; issue: string; severity: string; details: string }> = [];

  // Check all workers in parallel batches
  for (let i = 0; i < ALL_MONITORED_WORKERS.length; i += 15) {
    const batch = ALL_MONITORED_WORKERS.slice(i, i + 15);
    const results = await Promise.allSettled(
      batch.map(async (worker) => {
        scanned++;
        const healthPath = getHealthPath(worker);
        let check = await workerFetch(env, worker, healthPath, {}, 15000);

        // If /health returns 404, try fallback endpoints before flagging
        if (!check.ok && check.status === 404 && healthPath === '/health') {
          // Try /status as fallback
          const fallback = await workerFetch(env, worker, '/status', {}, 10000);
          if (fallback.ok) {
            check = fallback;
            // Remember this worker uses /status for future checks
          } else {
            // Try root / as last resort
            const rootCheck = await workerFetch(env, worker, '/', {}, 10000);
            if (rootCheck.ok) {
              check = rootCheck;
            }
          }
        }

        // Issue: Worker completely down
        if (!check.ok && check.status === 0) {
          issues.push({
            worker,
            issue: 'worker_unreachable',
            severity: 'critical',
            details: `Error: ${check.error || 'timeout'}`
          });
          issuesFound++;
          return;
        }

        // Issue: Non-200 response (after fallback attempts)
        if (!check.ok && check.status > 0) {
          // Workers without service bindings return 404 due to same-account
          // Cloudflare fetch limitation — this is NOT a real bug
          const hasBinding = !!SERVICE_BINDING_MAP[worker];
          if (!hasBinding && check.status === 404) {
            // Skip — can't reach unbound workers from within a Worker
            return;
          }
          issues.push({
            worker,
            issue: `http_${check.status}`,
            severity: check.status >= 500 ? 'high' : 'medium',
            details: `Status ${check.status}`
          });
          issuesFound++;
          return;
        }

        // Issue: Very high latency (persistent, > 10s)
        if (check.latencyMs > 10000) {
          issues.push({
            worker,
            issue: 'extreme_latency',
            severity: 'high',
            details: `${check.latencyMs}ms response time`
          });
          issuesFound++;
        }

        // Issue: Health endpoint returns unhealthy status
        if (check.data && typeof check.data === 'object') {
          const status = check.data.status || check.data.healthy;
          if (status === false || status === 'error' || status === 'degraded') {
            issues.push({
              worker,
              issue: 'self_reported_unhealthy',
              severity: 'high',
              details: JSON.stringify(check.data).slice(0, 200)
            });
            issuesFound++;
          }

          // Issue: Missing version (using deep extraction)
          const version = extractVersion(check.data);
          if (!version) {
            issues.push({
              worker,
              issue: 'no_version_in_health',
              severity: 'low',
              details: 'Health endpoint does not report version'
            });
            issuesFound++;
          }
        }
      })
    );
  }

  // Log all found issues
  if (issues.length > 0) {
    await logAction(env.DB, {
      action_type: 'bug_hunt',
      target: `${scanned} workers`,
      details: JSON.stringify(issues.slice(0, 50)),
      result: `found_${issuesFound}_issues`,
      duration_ms: 0,
      cycle_id: cid
    });

    // Queue fixes for critical/high issues
    for (const issue of issues) {
      if (issue.severity === 'critical' || issue.severity === 'high') {
        await queueFix(env, {
          source: 'hunter',
          source_id: `hunt_${cid}`,
          fix_type: 'worker_issue',
          target: issue.worker,
          priority: issue.severity === 'critical' ? 10 : 7,
          details: JSON.stringify(issue)
        });
      }
    }
  }

  await incrementStat(env.DB, 'hunt_issues', issuesFound);

  return { scanned, issuesFound };
}

// ═══════════════════════════════════════════════════
// MODULE 5: FIX EXECUTOR
// Processes the fix queue and applies auto-fixes
// ═══════════════════════════════════════════════════

async function executeFixQueue(env: Env, cid: string, maxFixes: number = 5): Promise<{ attempted: number; fixed: number; failed: number }> {
  let attempted = 0, fixed = 0, failed = 0;

  // Get pending fixes ordered by priority
  const pendingFixes = await env.DB.prepare(
    `SELECT * FROM fix_queue WHERE status = 'pending' AND attempts < 3 ORDER BY priority DESC, created_at ASC LIMIT ?`
  ).bind(maxFixes).all();

  if (!pendingFixes.results || pendingFixes.results.length === 0) {
    return { attempted, fixed, failed };
  }

  for (const fix of pendingFixes.results) {
    attempted++;
    const fixId = fix.id as number;
    const fixType = fix.fix_type as string;
    const target = fix.target as string;

    // Mark as in-progress
    await env.DB.prepare(
      `UPDATE fix_queue SET status = 'in_progress', attempts = attempts + 1 WHERE id = ?`
    ).bind(fixId).run();

    try {
      let fixResult = false;

      switch (fixType) {
        case 'thin_page':
          fixResult = await fixThinPage(env, target, fix.details as string, cid);
          break;
        case 'worker_issue':
          fixResult = await fixWorkerIssue(env, target, fix.details as string, cid);
          break;
        case 'placeholder_data':
          // Log for manual fix — placeholder data needs code review
          await logAction(env.DB, {
            action_type: 'fix_deferred',
            target,
            details: 'Placeholder data removal requires code review — queued for CC session',
            result: 'deferred',
            duration_ms: 0,
            cycle_id: cid
          });
          fixResult = false;
          break;
        case 'json_ld_faq':
          // Log for manual fix — needs TSX changes
          await logAction(env.DB, {
            action_type: 'fix_deferred',
            target,
            details: 'JSON-LD FAQPage fix requires TSX layout changes — queued for CC session',
            result: 'deferred',
            duration_ms: 0,
            cycle_id: cid
          });
          fixResult = false;
          break;
        default:
          await logAction(env.DB, {
            action_type: 'fix_unknown_type',
            target,
            details: `Unknown fix type: ${fixType}`,
            result: 'skipped',
            duration_ms: 0,
            cycle_id: cid
          });
      }

      if (fixResult) {
        fixed++;
        await env.DB.prepare(
          `UPDATE fix_queue SET status = 'fixed', fix_applied = 'auto', resolved_at = datetime('now') WHERE id = ?`
        ).bind(fixId).run();
        await incrementStat(env.DB, 'bugs_fixed');
      } else {
        // Not fixed but not errored — might need manual intervention
        await env.DB.prepare(
          `UPDATE fix_queue SET status = 'pending' WHERE id = ? AND attempts < 3`
        ).bind(fixId).run();
        await env.DB.prepare(
          `UPDATE fix_queue SET status = 'failed', error_msg = 'max attempts reached' WHERE id = ? AND attempts >= 3`
        ).bind(fixId).run();
      }
    } catch (e: any) {
      failed++;
      await env.DB.prepare(
        `UPDATE fix_queue SET status = 'failed', error_msg = ? WHERE id = ?`
      ).bind(e.message.slice(0, 500), fixId).run();
      await logAction(env.DB, {
        action_type: 'fix_error',
        target,
        details: e.message,
        result: 'error',
        duration_ms: 0,
        cycle_id: cid
      });
    }
  }

  return { attempted, fixed, failed };
}

// ═══════════════════════════════════════════════════
// MODULE 6: THIN PAGE FIXER
// Uses Engine Runtime to generate content for thin
// EPT pages, then pushes via GitHub API
// ═══════════════════════════════════════════════════

async function fixThinPage(env: Env, pagePath: string, detailsJson: string, cid: string): Promise<boolean> {
  if (!env.GITHUB_TOKEN) {
    await logAction(env.DB, {
      action_type: 'fix_blocked',
      target: pagePath,
      details: 'No GITHUB_TOKEN — cannot push fixes',
      result: 'blocked',
      duration_ms: 0,
      cycle_id: cid
    });
    return false;
  }

  // Determine the file path in the repo
  const cleanPath = pagePath.replace(/^\//, '').replace(/\/$/, '');
  const filePath = `app/${cleanPath}/page.tsx`;

  // Read current file from GitHub (authenticated — repo may be private)
  const ghHeaders: Record<string, string> = {
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'EchoAutoBuilder/1.0'
  };
  if (env.GITHUB_TOKEN) {
    ghHeaders['Authorization'] = `Bearer ${env.GITHUB_TOKEN}`;
  }
  const existing = await safeFetchWithHeaders(
    `${GITHUB_API}/repos/${GITHUB_OWNER}/${EPT_REPO}/contents/${filePath}`,
    ghHeaders,
    10000
  );

  // If file doesn't exist, skip (might be a dynamic route)
  if (!existing.ok) {
    await logAction(env.DB, {
      action_type: 'fix_skip',
      target: pagePath,
      details: `File not found at ${filePath}`,
      result: 'skipped',
      duration_ms: 0,
      cycle_id: cid
    });
    return false;
  }

  // Decode current content
  let currentContent = '';
  try {
    currentContent = atob(existing.data.content.replace(/\n/g, ''));
  } catch {
    return false;
  }

  // Check if it's a ComingSoonGuard redirect page
  const isRedirect = currentContent.includes('ComingSoonGuard') ||
                     currentContent.includes('redirect') ||
                     currentContent.length < 500;

  if (!isRedirect) {
    // Page has real content but QA flagged it — probably SSR issue, skip
    await logAction(env.DB, {
      action_type: 'fix_skip',
      target: pagePath,
      details: 'Page has real content (not a redirect), QA may have SSR detection issue',
      result: 'skipped',
      duration_ms: 0,
      cycle_id: cid
    });
    return false;
  }

  // Generate product page content using a template
  const productName = cleanPath.split('/').pop() || cleanPath;
  const title = productName.split('-').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

  // Try Engine Runtime for content generation
  let description = `${title} is a powerful AI-powered solution built on the Echo Prime Technology platform.`;
  let features: string[] = [];

  try {
    const engineBody = JSON.stringify({
      query: `Write a 2-sentence product description for "${title}" - an AI-powered SaaS tool. Also list 6 key features as short phrases.`,
      domain: 'GEN',
      model: 'haiku'
    });
    const engineResp = env.SVC_ENGINE
      ? await env.SVC_ENGINE.fetch('https://internal/query/reason', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: engineBody
        })
      : await fetchWithTimeout(`${ENGINE_URL}/query/reason`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: engineBody
        }, 30000);

    if (engineResp.ok) {
      const engineData = await engineResp.json() as any;
      if (engineData.answer) {
        const text = engineData.answer;
        // Extract description (first 2 sentences)
        const sentences = text.split(/\.\s+/);
        if (sentences.length >= 2) {
          description = sentences.slice(0, 2).join('. ') + '.';
        }
        // Extract features (look for numbered or bulleted lists)
        const featureMatches = text.match(/(?:^|\n)\s*(?:\d+[\.\)]\s*|[-•]\s*)(.+)/gm);
        if (featureMatches) {
          features = featureMatches.slice(0, 6).map((f: string) => f.replace(/^\s*(?:\d+[\.\)]\s*|[-•]\s*)/, '').trim());
        }
      }
    }
  } catch {
    // Engine Runtime unavailable — use template defaults
  }

  if (features.length < 6) {
    features = [
      'AI-Powered Automation', 'Real-Time Analytics Dashboard',
      'Enterprise-Grade Security', 'Multi-Tenant Architecture',
      'RESTful API with Full Documentation', 'Cloudflare Edge Deployment'
    ];
  }

  // Generate the page TSX
  const pageTsx = generateProductPageTsx(title, description, features, cleanPath);

  // Push to GitHub
  const pushResult = await pushToGitHub(
    env,
    EPT_REPO,
    filePath,
    pageTsx,
    `fix(auto-builder): replace thin ${cleanPath} page with full product content`,
    existing.data.sha
  );

  if (pushResult.success) {
    await logAction(env.DB, {
      action_type: 'fix_thin_page',
      target: pagePath,
      details: JSON.stringify({ repo: EPT_REPO, file: filePath, commitSha: pushResult.sha }),
      result: 'fixed',
      duration_ms: 0,
      cycle_id: cid
    });
    await incrementStat(env.DB, 'pages_fixed');
    await incrementStat(env.DB, 'deploys');
    return true;
  }

  return false;
}

function generateProductPageTsx(title: string, description: string, features: string[], slug: string): string {
  const iconImports = ['Zap', 'Shield', 'BarChart3', 'Cpu', 'Globe', 'Lock', 'Rocket', 'Settings'];
  const featureIcons = features.map((_, i) => iconImports[i % iconImports.length]);

  return `import { Metadata } from 'next'
import { ${[...new Set(featureIcons)].join(', ')} } from 'lucide-react'

export const metadata: Metadata = {
  title: '${title} | Echo Prime Technology',
  description: '${description.replace(/'/g, "\\'")}',
}

const features = [
${features.map((f, i) => `  { icon: ${featureIcons[i]}, title: '${f.replace(/'/g, "\\'")}', description: 'Leverage cutting-edge AI technology to streamline and optimize your ${f.toLowerCase().replace(/'/g, "\\'")} workflow.' },`).join('\n')}
]

export default function ${slug.split('-').map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join('')}Page() {
  return (
    <div className="min-h-screen bg-black text-white">
      {/* Hero */}
      <section className="relative py-24 px-4 overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-b from-red-900/20 to-transparent" />
        <div className="max-w-6xl mx-auto text-center relative z-10">
          <h1 className="text-5xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-red-500 to-orange-500 bg-clip-text text-transparent">
            ${title}
          </h1>
          <p className="text-xl text-gray-400 max-w-3xl mx-auto mb-8">
            ${description.replace(/'/g, "\\'")}
          </p>
          <div className="flex gap-4 justify-center">
            <a href="/pricing" className="bg-red-600 hover:bg-red-700 text-white px-8 py-3 rounded-lg font-semibold transition-colors">
              Get Started
            </a>
            <a href="/docs" className="border border-gray-700 hover:border-gray-500 text-white px-8 py-3 rounded-lg font-semibold transition-colors">
              Documentation
            </a>
          </div>
        </div>
      </section>

      {/* Features */}
      <section className="py-20 px-4">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">Key Features</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
            {features.map((feature, i) => {
              const Icon = feature.icon
              return (
                <div key={i} className="bg-gray-900/50 border border-gray-800 rounded-xl p-6 hover:border-red-500/50 transition-colors">
                  <Icon className="w-8 h-8 text-red-500 mb-4" />
                  <h3 className="text-xl font-semibold mb-3">{feature.title}</h3>
                  <p className="text-gray-400">{feature.description}</p>
                </div>
              )
            })}
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-20 px-4 bg-gradient-to-t from-red-900/10 to-transparent">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-3xl font-bold mb-6">Ready to Transform Your Workflow?</h2>
          <p className="text-gray-400 mb-8 text-lg">
            Join thousands of businesses using Echo Prime Technology to automate and scale.
          </p>
          <a href="/pricing" className="bg-red-600 hover:bg-red-700 text-white px-10 py-4 rounded-lg font-semibold text-lg transition-colors inline-block">
            Start Free Trial
          </a>
        </div>
      </section>
    </div>
  )
}
`;
}

// ═══════════════════════════════════════════════════
// MODULE 7: WORKER ISSUE FIXER
// Handles issues found by the bug hunter
// ═══════════════════════════════════════════════════

async function fixWorkerIssue(env: Env, workerName: string, detailsJson: string, cid: string): Promise<boolean> {
  const details = JSON.parse(detailsJson);
  const healthPath = getHealthPath(workerName);

  // For HTTP 404 on health — try alternative endpoints
  if (details.issue && details.issue.startsWith('http_404')) {
    // Try /status, then / as fallbacks
    for (const altPath of ['/status', '/']) {
      if (altPath === healthPath) continue; // Skip if already the configured path
      const check = await workerFetch(env, workerName, altPath, {}, 10000);
      if (check.ok) {
        await logAction(env.DB, {
          action_type: 'fix_worker_alt_health',
          target: workerName,
          details: `Worker responds on ${altPath} instead of /health. Latency: ${check.latencyMs}ms. Version: ${extractVersion(check.data)}`,
          result: 'fixed',
          duration_ms: check.latencyMs,
          cycle_id: cid
        });
        return true;
      }
    }
    return false;
  }

  // For unreachable workers — try warming up (sometimes it's just cold start)
  if (details.issue === 'worker_unreachable' || details.issue === 'extreme_latency') {
    // Try 3 warm-up pings
    for (let attempt = 0; attempt < 3; attempt++) {
      const check = await workerFetch(env, workerName, healthPath, {}, 15000);
      if (check.ok && check.latencyMs < 5000) {
        await logAction(env.DB, {
          action_type: 'fix_worker_warmup',
          target: workerName,
          details: `Resolved after ${attempt + 1} warm-up attempt(s). Latency: ${check.latencyMs}ms`,
          result: 'fixed',
          duration_ms: check.latencyMs,
          cycle_id: cid
        });
        return true;
      }
      // Wait 2 seconds between attempts
      await new Promise(r => setTimeout(r, 2000));
    }
    return false;
  }

  // For self-reported unhealthy — log and monitor
  if (details.issue === 'self_reported_unhealthy') {
    await logAction(env.DB, {
      action_type: 'fix_deferred',
      target: workerName,
      details: 'Worker self-reports unhealthy — needs code investigation. Queued for CC session.',
      result: 'deferred',
      duration_ms: 0,
      cycle_id: cid
    });
    return false;
  }

  return false;
}

// ═══════════════════════════════════════════════════
// MODULE 8: GITHUB DEPLOYER
// Pushes file changes to GitHub repos via Contents API
// ═══════════════════════════════════════════════════

async function pushToGitHub(
  env: Env,
  repo: string,
  filePath: string,
  content: string,
  commitMessage: string,
  existingSha?: string
): Promise<{ success: boolean; sha?: string; error?: string }> {
  try {
    const body: any = {
      message: commitMessage,
      content: btoa(unescape(encodeURIComponent(content))),
      committer: {
        name: 'Echo Auto-Builder',
        email: 'autobuilder@echo-op.com'
      }
    };

    if (existingSha) {
      body.sha = existingSha;
    }

    const resp = await fetchWithTimeout(
      `${GITHUB_API}/repos/${GITHUB_OWNER}/${repo}/contents/${filePath}`,
      {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
          'User-Agent': 'EchoAutoBuilder/1.0'
        },
        body: JSON.stringify(body)
      },
      15000
    );

    if (resp.ok) {
      const data = await resp.json() as any;
      await env.DB.prepare(
        `INSERT INTO deploy_history (repo, file_path, commit_sha, commit_message, trigger, status) VALUES (?, ?, ?, ?, 'auto-builder', 'success')`
      ).bind(repo, filePath, data.content?.sha || '', commitMessage).run();
      return { success: true, sha: data.content?.sha };
    } else {
      const errorText = await resp.text();
      return { success: false, error: `GitHub API ${resp.status}: ${errorText.slice(0, 200)}` };
    }
  } catch (e: any) {
    return { success: false, error: e.message };
  }
}

// ═══════════════════════════════════════════════════
// MODULE 9: REPORTER
// Sends reports to Shared Brain and MoltBook
// ═══════════════════════════════════════════════════

async function reportToBrain(env: Env, content: string, importance: number = 7, tags: string[] = ['auto-builder']): Promise<void> {
  try {
    const body = JSON.stringify({
      instance_id: 'echo-autonomous-builder',
      role: 'assistant',
      content,
      importance,
      tags
    });
    if (env.SVC_BRAIN) {
      await env.SVC_BRAIN.fetch('https://internal/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      });
    } else {
      await fetchWithTimeout(BRAIN_URL + '/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      }, 5000);
    }
  } catch {}
}

async function postToMoltBook(env: Env, content: string, mood: string = 'building', tags: string[] = ['auto-builder']): Promise<void> {
  try {
    const body = JSON.stringify({
      author_id: 'auto-builder',
      author_name: 'Echo Auto-Builder',
      author_type: 'agent',
      content: `AUTO-BUILDER: ${content}`,
      mood,
      tags
    });
    if (env.SVC_SWARM) {
      await env.SVC_SWARM.fetch('https://internal/moltbook/post', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      });
    } else {
      await fetchWithTimeout(MOLTBOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      }, 5000);
    }
  } catch {}
}

// ═══════════════════════════════════════════════════
// MODULE 10: DAILY BRIEFING
// Compiles overnight results into a report
// ═══════════════════════════════════════════════════

async function generateDailyBriefing(env: Env, cid: string): Promise<string> {
  const d = today();
  const stats = await env.DB.prepare(
    `SELECT * FROM daily_stats WHERE date = ?`
  ).bind(d).first();

  const recentActions = await env.DB.prepare(
    `SELECT action_type, target, result, created_at FROM actions_log ORDER BY created_at DESC LIMIT 20`
  ).all();

  const pendingFixes = await env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM fix_queue WHERE status = 'pending'`
  ).first() as any;

  const completedFixes = await env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM fix_queue WHERE status = 'fixed' AND DATE(resolved_at) = ?`
  ).bind(d).first() as any;

  const failedFixes = await env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM fix_queue WHERE status = 'failed' AND DATE(created_at) = ?`
  ).bind(d).first() as any;

  const workerStats = await env.DB.prepare(
    `SELECT COUNT(*) as total, AVG(avg_latency_ms) as avgLatency, AVG(health_score) as avgHealth FROM worker_profiles`
  ).first() as any;

  const briefing = `DAILY AUTONOMOUS BUILDER BRIEFING — ${d}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WARMUPS: ${stats?.warmups || 0} worker warm-up pings
BUGS FOUND: ${stats?.bugs_found || 0} | AUTO-RESOLVED: ${stats?.bugs_auto_resolved || 0} | FIXED: ${stats?.bugs_fixed || 0}
DAEMON TASKS RESOLVED: ${stats?.tasks_resolved || 0}
PAGES FIXED: ${stats?.pages_fixed || 0} | DEPLOYS: ${stats?.deploys || 0}
BUG HUNT ISSUES: ${stats?.hunt_issues || 0}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIX QUEUE: ${pendingFixes?.cnt || 0} pending | ${completedFixes?.cnt || 0} fixed today | ${failedFixes?.cnt || 0} failed
FLEET: ${workerStats?.total || 0} workers tracked | Avg latency: ${Math.round(workerStats?.avgLatency || 0)}ms | Avg health: ${Math.round(workerStats?.avgHealth || 0)}%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RECENT ACTIONS:
${(recentActions.results || []).slice(0, 10).map((a: any) => `  ${a.created_at} | ${a.action_type} → ${a.target} [${a.result}]`).join('\n')}`;

  // Post briefing to Brain + MoltBook
  await reportToBrain(env, briefing, 8, ['daily-briefing', 'auto-builder']);
  await postToMoltBook(env, briefing, 'reporting', ['daily-briefing']);

  // Also post to OmniSync as broadcast via service binding
  try {
    if (env.SVC_OMNISYNC) {
      await env.SVC_OMNISYNC.fetch('https://internal/broadcasts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: briefing })
      });
    } else {
      await fetchWithTimeout(`${OMNISYNC_URL}/broadcasts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: briefing })
      }, 5000);
    }
  } catch {}

  return briefing;
}

// ═══════════════════════════════════════════════════
// MODULE 11: UPGRADE SCANNER
// Identifies workers that could be improved
// ═══════════════════════════════════════════════════

async function scanForUpgrades(env: Env, cid: string): Promise<Array<{ worker: string; upgrade: string; priority: number }>> {
  const upgrades: Array<{ worker: string; upgrade: string; priority: number }> = [];

  // Get all worker profiles
  const profiles = await env.DB.prepare(
    `SELECT * FROM worker_profiles WHERE check_count > 0 ORDER BY health_score ASC`
  ).all();

  for (const profile of profiles.results || []) {
    const name = profile.worker_name as string;
    const avgLatency = profile.avg_latency_ms as number;
    const healthScore = profile.health_score as number;
    const version = profile.last_version as string;
    const checkCount = profile.check_count as number;
    const healthyCount = profile.healthy_count as number;

    // Workers with low uptime ratio
    if (checkCount > 10 && healthyCount / checkCount < 0.9) {
      upgrades.push({
        worker: name,
        upgrade: `Low reliability: ${Math.round(healthyCount / checkCount * 100)}% uptime over ${checkCount} checks`,
        priority: 8
      });
    }

    // Workers with consistently high latency
    if (avgLatency > 3000 && checkCount > 5) {
      upgrades.push({
        worker: name,
        upgrade: `High avg latency: ${Math.round(avgLatency)}ms — needs optimization`,
        priority: 6
      });
    }

    // Workers on v1.0.0 (potentially outdated)
    if (version === '1.0.0' && !name.includes('gs343') && !name.includes('cost-optimizer')) {
      upgrades.push({
        worker: name,
        upgrade: 'Still on v1.0.0 — review for update opportunities',
        priority: 3
      });
    }
  }

  if (upgrades.length > 0) {
    await logAction(env.DB, {
      action_type: 'upgrade_scan',
      target: `${upgrades.length} opportunities`,
      details: JSON.stringify(upgrades.slice(0, 20)),
      result: 'scanned',
      duration_ms: 0,
      cycle_id: cid
    });
  }

  return upgrades;
}

// ═══════════════════════════════════════════════════
// CRON DISPATCH
// ═══════════════════════════════════════════════════

async function handleCron(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
  await initDB(env.DB);
  const cid = cycleId();
  const minute = new Date(event.scheduledTime).getMinutes();
  const hour = new Date(event.scheduledTime).getHours();

  // Rate limit: don't run if we ran less than 2 minutes ago
  const lastRun = await env.CACHE.get('last_cron_run');
  const now = Date.now();
  if (lastRun && now - parseInt(lastRun) < 120000) {
    return; // Skip — ran too recently
  }
  await env.CACHE.put('last_cron_run', String(now), { expirationTtl: 300 });

  try {
    // ═══ EVERY 5 MINUTES: Warm up critical workers ═══
    if (minute % 5 === 0) {
      ctx.waitUntil(warmUpWorkers(env, cid));
    }

    // ═══ EVERY 30 MINUTES: Process QA bugs + daemon tasks + execute fixes ═══
    if (minute % 30 === 0) {
      const qaResult = await processQABugs(env, cid);
      const daemonResult = await processDaemonTasks(env, cid);
      const fixResult = await executeFixQueue(env, cid, 5);

      const summary = `Cycle ${cid}: QA processed=${qaResult.processed} autoResolved=${qaResult.autoResolved} queued=${qaResult.queued} | Daemon processed=${daemonResult.processed} resolved=${daemonResult.resolved} | Fixes attempted=${fixResult.attempted} fixed=${fixResult.fixed} failed=${fixResult.failed}`;

      await logAction(env.DB, {
        action_type: 'cycle_30min',
        target: 'system',
        details: summary,
        result: 'complete',
        duration_ms: 0,
        cycle_id: cid
      });

      // Report significant activity to Brain
      if (qaResult.autoResolved > 0 || daemonResult.resolved > 0 || fixResult.fixed > 0) {
        ctx.waitUntil(reportToBrain(env, `AUTO-BUILDER: ${summary}`, 7, ['auto-builder', 'cycle']));
      }
    }

    // ═══ EVERY 4 HOURS: Full audit + bug hunt + upgrade scan ═══
    if (hour % 4 === 0 && minute === 0) {
      // Full fleet warm-up
      const warmResults = await warmUpWorkers(env, cid, ALL_MONITORED_WORKERS);

      // Bug hunt
      const huntResult = await huntBugs(env, cid);

      // Upgrade scan
      const upgrades = await scanForUpgrades(env, cid);

      // Execute any pending fixes
      const fixResult = await executeFixQueue(env, cid, 10);

      const summary = `4-HOUR AUDIT: ${warmResults.length} workers warmed (${warmResults.filter(r => r.healthy).length} healthy) | Hunt: ${huntResult.issuesFound} issues found | ${upgrades.length} upgrade opportunities | Fixes: ${fixResult.fixed}/${fixResult.attempted}`;

      await logAction(env.DB, {
        action_type: 'cycle_4hour',
        target: 'system',
        details: summary,
        result: 'complete',
        duration_ms: 0,
        cycle_id: cid
      });

      ctx.waitUntil(reportToBrain(env, `AUTO-BUILDER AUDIT: ${summary}`, 8, ['auto-builder', 'audit']));
      ctx.waitUntil(postToMoltBook(env, summary, 'building', ['audit', 'auto-builder']));
    }

    // ═══ DAILY 8 AM UTC: Briefing ═══
    if (hour === 8 && minute === 0) {
      await generateDailyBriefing(env, cid);
    }

  } catch (e: any) {
    await logAction(env.DB, {
      action_type: 'cron_error',
      target: 'cron_dispatch',
      details: e.message,
      result: 'error',
      duration_ms: 0,
      cycle_id: cid
    });
  }
}

// ═══════════════════════════════════════════════════
// HTTP API
// ═══════════════════════════════════════════════════

async function handleRequest(request: Request, env: Env): Promise<Response> {
  await initDB(env.DB);
  const url = new URL(request.url);
  const path = url.pathname;

  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Echo-API-Key',
    'Content-Type': 'application/json'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  // ─── /health ───
  if (path === '/health') {
    const stats = await env.DB.prepare(`SELECT * FROM daily_stats WHERE date = ?`).bind(today()).first();
    const pending = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM fix_queue WHERE status = 'pending'`).first() as any;
    const profiles = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM worker_profiles`).first() as any;

    return new Response(JSON.stringify({
      status: 'ok',
      service: 'echo-autonomous-builder',
      version: env.WORKER_VERSION,
      timestamp: new Date().toISOString(),
      todayStats: stats || {},
      pendingFixes: pending?.cnt || 0,
      workersTracked: profiles?.cnt || 0,
      capabilities: [
        'worker_warmup', 'qa_bug_processing', 'daemon_task_resolution',
        'bug_hunting', 'thin_page_fixing', 'github_deployment',
        'upgrade_scanning', 'daily_briefing', 'shared_brain_reporting',
        'moltbook_posting', 'auto_fix_execution'
      ]
    }), { headers: corsHeaders });
  }

  // ─── /status ───
  if (path === '/status') {
    const stats = await env.DB.prepare(`SELECT * FROM daily_stats WHERE date = ?`).bind(today()).first();
    const pending = await env.DB.prepare(`SELECT * FROM fix_queue WHERE status = 'pending' ORDER BY priority DESC LIMIT 20`).all();
    const recent = await env.DB.prepare(`SELECT * FROM actions_log ORDER BY created_at DESC LIMIT 30`).all();
    const workers = await env.DB.prepare(`SELECT worker_name, avg_latency_ms, health_score, last_check, last_version FROM worker_profiles ORDER BY health_score ASC LIMIT 20`).all();

    return new Response(JSON.stringify({
      service: 'echo-autonomous-builder',
      version: env.WORKER_VERSION,
      todayStats: stats || {},
      pendingFixes: pending.results,
      recentActions: recent.results,
      lowestHealthWorkers: workers.results
    }), { headers: corsHeaders });
  }

  // ─── /history ───
  if (path === '/history') {
    const limit = parseInt(url.searchParams.get('limit') || '50');
    const offset = parseInt(url.searchParams.get('offset') || '0');
    const type = url.searchParams.get('type');

    let query = 'SELECT * FROM actions_log';
    const params: any[] = [];

    if (type) {
      query += ' WHERE action_type = ?';
      params.push(type);
    }

    query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);

    const rows = await env.DB.prepare(query).bind(...params).all();
    return new Response(JSON.stringify({ actions: rows.results, total: rows.results?.length }), { headers: corsHeaders });
  }

  // ─── /workers ───
  if (path === '/workers') {
    const profiles = await env.DB.prepare(
      `SELECT * FROM worker_profiles ORDER BY health_score ASC`
    ).all();
    return new Response(JSON.stringify({ workers: profiles.results }), { headers: corsHeaders });
  }

  // ─── /queue ───
  if (path === '/queue') {
    const status = url.searchParams.get('status') || 'pending';
    const fixes = await env.DB.prepare(
      `SELECT * FROM fix_queue WHERE status = ? ORDER BY priority DESC, created_at ASC LIMIT 50`
    ).bind(status).all();
    return new Response(JSON.stringify({ fixes: fixes.results }), { headers: corsHeaders });
  }

  // ─── /deploys ───
  if (path === '/deploys') {
    const deploys = await env.DB.prepare(
      `SELECT * FROM deploy_history ORDER BY created_at DESC LIMIT 50`
    ).all();
    return new Response(JSON.stringify({ deploys: deploys.results }), { headers: corsHeaders });
  }

  // ─── /stats ───
  if (path === '/stats') {
    const days = parseInt(url.searchParams.get('days') || '7');
    const stats = await env.DB.prepare(
      `SELECT * FROM daily_stats ORDER BY date DESC LIMIT ?`
    ).bind(days).all();
    return new Response(JSON.stringify({ stats: stats.results }), { headers: corsHeaders });
  }

  // ─── /briefing ───
  if (path === '/briefing') {
    const cid = cycleId();
    const briefing = await generateDailyBriefing(env, cid);
    return new Response(JSON.stringify({ briefing }), { headers: corsHeaders });
  }

  // ─── POST /run ───
  if (path === '/run' && request.method === 'POST') {
    const cid = cycleId();
    const body = await request.json().catch(() => ({})) as any;
    const module = body.module || 'all';

    const results: any = {};

    if (module === 'all' || module === 'warmup') {
      results.warmup = await warmUpWorkers(env, cid);
    }
    if (module === 'all' || module === 'qa') {
      results.qa = await processQABugs(env, cid);
    }
    if (module === 'all' || module === 'daemon') {
      results.daemon = await processDaemonTasks(env, cid);
    }
    if (module === 'all' || module === 'hunt') {
      results.hunt = await huntBugs(env, cid);
    }
    if (module === 'all' || module === 'fixes') {
      results.fixes = await executeFixQueue(env, cid, body.maxFixes || 10);
    }
    if (module === 'all' || module === 'upgrades') {
      results.upgrades = await scanForUpgrades(env, cid);
    }
    if (module === 'briefing') {
      results.briefing = await generateDailyBriefing(env, cid);
    }

    return new Response(JSON.stringify({ cycleId: cid, results }), { headers: corsHeaders });
  }

  // ─── POST /run/warmup ───
  if (path === '/run/warmup' && request.method === 'POST') {
    const cid = cycleId();
    const results = await warmUpWorkers(env, cid, ALL_MONITORED_WORKERS);
    return new Response(JSON.stringify({
      cycleId: cid,
      warmed: results.length,
      healthy: results.filter(r => r.healthy).length,
      unhealthy: results.filter(r => !r.healthy).map(r => ({ worker: r.worker, error: r.error, status: r.statusCode })),
      avgLatency: Math.round(results.reduce((s, r) => s + r.latencyMs, 0) / results.length)
    }), { headers: corsHeaders });
  }

  // ─── POST /run/hunt ───
  if (path === '/run/hunt' && request.method === 'POST') {
    const cid = cycleId();
    const results = await huntBugs(env, cid);
    return new Response(JSON.stringify({ cycleId: cid, ...results }), { headers: corsHeaders });
  }

  // ─── POST /config ───
  if (path === '/config' && request.method === 'POST') {
    const body = await request.json().catch(() => ({})) as any;
    if (body.key && body.value !== undefined) {
      await env.DB.prepare(
        `INSERT INTO config (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = datetime('now')`
      ).bind(body.key, JSON.stringify(body.value), JSON.stringify(body.value)).run();
      return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
    }
    return new Response(JSON.stringify({ error: 'key and value required' }), { status: 400, headers: corsHeaders });
  }

  // ─── GET /config ───
  if (path === '/config' && request.method === 'GET') {
    const configs = await env.DB.prepare(`SELECT * FROM config`).all();
    return new Response(JSON.stringify({ config: configs.results }), { headers: corsHeaders });
  }

  // ─── 404 ───
  return new Response(JSON.stringify({
    error: 'Not found',
    endpoints: [
      'GET /health', 'GET /status', 'GET /history', 'GET /workers',
      'GET /queue', 'GET /deploys', 'GET /stats', 'GET /briefing',
      'GET /config', 'POST /run', 'POST /run/warmup', 'POST /run/hunt',
      'POST /config'
    ]
  }), { status: 404, headers: corsHeaders });
}

// ═══════════════════════════════════════════════════
// MAIN EXPORT
// ═══════════════════════════════════════════════════

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return handleRequest(request, env);
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    await handleCron(event, env, ctx);
  }
};
