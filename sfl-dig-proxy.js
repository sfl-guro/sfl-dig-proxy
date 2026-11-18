const ALLOWED_ORIGINS = new Set([
  "https://sfl-guro.github.io",
  "http://localhost:8158",
]);

const API_BASE = "https://api.sunflower-land.com/community";
const SPACING_MS = 5200;
const BACKOFF_FIRST_MS = 5500;
const BACKOFF_MAX_MS = 10500;
const MAX_WAIT_MS = 8000;
const BATCH_MAX = 20;
const UPSTREAM_TIMEOUT_MS = 7000;

function json(obj, status, extra) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...extra },
  });
}

function isBatchable(id) {
  return /^[0-9]+$/.test(id) && Number.isSafeInteger(Number(id));
}

export class FarmGate {
  constructor(state, env) {
    this.env = env;
    this.pending = new Map();
    this.timer = null;
    this.running = false;
    this.nextAllowedAt = 0;
    this.backoffMs = 0;
    this.batchDisabled = false;
  }

  async fetch(request) {
    const id = new URL(request.url).searchParams.get("id");
    if (!id) return json({ error: "Missing id" }, 400);
    const result = await this.enqueue(id);
    return json(result.payload, result.status, result.retryAfter ? { "Retry-After": String(result.retryAfter) } : undefined);
  }

  enqueue(id) {
    return new Promise((resolve) => {
      const waiter = { resolve: null, timer: null };
      waiter.resolve = (value) => {
        clearTimeout(waiter.timer);
        resolve(value);
      };
      waiter.timer = setTimeout(() => {
        const entry = this.pending.get(id);
        if (entry) {
          entry.waiters.delete(waiter);
          if (!entry.waiters.size) this.pending.delete(id);
        }
        resolve({ status: 429, retryAfter: 5, payload: { error: "Server is busy — try again shortly", retryAfter: 5 } });
      }, MAX_WAIT_MS);
      let entry = this.pending.get(id);
      if (!entry) {
        entry = { waiters: new Set(), single: !isBatchable(id) };
        this.pending.set(id, entry);
      }
      entry.waiters.add(waiter);
      this.schedule();
    });
  }

  schedule() {
    if (this.running || this.timer || !this.pending.size) return;
    const delay = Math.max(0, this.nextAllowedAt - Date.now());
    this.timer = setTimeout(() => {
      this.timer = null;
      this.run();
    }, delay);
  }

  settle(ids, result) {
    ids.forEach((id) => {
      const entry = this.pending.get(id);
      if (!entry) return;
      this.pending.delete(id);
      entry.waiters.forEach((w) => w.resolve(result(id)));
    });
  }

  async run() {
    const batchIds = [];
    let singleId = null;
    for (const [id, entry] of this.pending) {
      if (!entry.waiters.size) continue;
      if (entry.single || this.batchDisabled) {
        if (singleId === null) singleId = id;
      } else if (batchIds.length < BATCH_MAX) {
        batchIds.push(id);
      }
    }
    if (!batchIds.length && singleId === null) {
      this.schedule();
      return;
    }
    this.running = true;
    const headers = { "x-api-key": this.env.SFL_API_KEY, "accept": "application/json" };
    let rateLimited = false;
    let retryAfter = 0;
    try {
      if (batchIds.length) {
        const taken = new Map();
        batchIds.forEach((id) => {
          taken.set(id, this.pending.get(id));
          this.pending.delete(id);
        });
        const restore = () => taken.forEach((entry, id) => {
          const current = this.pending.get(id);
          if (current) entry.waiters.forEach((w) => current.waiters.add(w));
          else this.pending.set(id, entry);
        });
        let res;
        try {
          res = await fetch(`${API_BASE}/getFarms`, {
            method: "POST",
            headers: { ...headers, "content-type": "application/json" },
            body: JSON.stringify({ ids: batchIds.map(Number) }),
            signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
            cf: { cacheTtl: 0 },
          });
        } catch (e) {
          const timedOut = e && e.name === "TimeoutError";
          taken.forEach((entry) => entry.waiters.forEach((w) => w.resolve({
            status: timedOut ? 504 : 502,
            payload: { error: timedOut ? "Upstream timed out" : "Upstream fetch failed" },
          })));
          return;
        }
        if (res.status === 429) {
          rateLimited = true;
          retryAfter = Number(res.headers.get("retry-after")) || 0;
          await res.arrayBuffer().catch(() => {});
          restore();
        } else if ([404, 405, 410, 501].includes(res.status)) {
          this.batchDisabled = true;
          await res.arrayBuffer().catch(() => {});
          restore();
        } else if (!res.ok) {
          await res.arrayBuffer().catch(() => {});
          const status = res.status === 401 ? 502 : res.status;
          const error = res.status === 401 ? "Proxy API key rejected by the SFL API" : "Upstream error " + res.status;
          taken.forEach((entry) => entry.waiters.forEach((w) => w.resolve({ status, payload: { error } })));
        } else {
          let data = null;
          try {
            data = await res.json();
          } catch (e) {}
          const farms = data && data.farms && typeof data.farms === "object" ? data.farms : null;
          if (!farms) {
            taken.forEach((entry) => entry.waiters.forEach((w) => w.resolve({
              status: 502,
              payload: { error: "Upstream sent an unreadable response" },
            })));
          } else {
            taken.forEach((entry, id) => {
              const farm = farms[id];
              if (farm && typeof farm === "object") {
                const payload = { id: Number(id), farm, isBlacklisted: farm.isBlacklisted, updatedAt: farm.updatedAt };
                entry.waiters.forEach((w) => w.resolve({ status: 200, payload }));
              } else {
                entry.single = true;
                const current = this.pending.get(id);
                if (current) entry.waiters.forEach((w) => current.waiters.add(w));
                else this.pending.set(id, entry);
              }
            });
          }
        }
      } else {
        const entry = this.pending.get(singleId);
        this.pending.delete(singleId);
        let res;
        try {
          res = await fetch(`${API_BASE}/farms/${singleId}`, {
            headers,
            signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
            cf: { cacheTtl: 0 },
          });
        } catch (e) {
          const timedOut = e && e.name === "TimeoutError";
          entry.waiters.forEach((w) => w.resolve({
            status: timedOut ? 504 : 502,
            payload: { error: timedOut ? "Upstream timed out" : "Upstream fetch failed" },
          }));
          return;
        }
        if (res.status === 429) {
          rateLimited = true;
          retryAfter = Number(res.headers.get("retry-after")) || 0;
          await res.arrayBuffer().catch(() => {});
          const current = this.pending.get(singleId);
          if (current) entry.waiters.forEach((w) => current.waiters.add(w));
          else this.pending.set(singleId, entry);
        } else if (res.ok) {
          let data = null;
          try {
            data = await res.json();
          } catch (e) {}
          entry.waiters.forEach((w) => w.resolve(data && typeof data === "object"
            ? { status: 200, payload: data }
            : { status: 502, payload: { error: "Upstream sent an unreadable response" } }));
        } else {
          await res.arrayBuffer().catch(() => {});
          const status = res.status === 401 ? 502 : res.status;
          const error = res.status === 404 ? "Farm not found — check the Farm ID" : res.status === 401 ? "Proxy API key rejected by the SFL API" : "Upstream error " + res.status;
          entry.waiters.forEach((w) => w.resolve({ status, payload: { error } }));
        }
      }
    } finally {
      this.running = false;
      if (rateLimited) {
        this.backoffMs = this.backoffMs ? Math.min(this.backoffMs * 2, BACKOFF_MAX_MS) : BACKOFF_FIRST_MS;
        this.nextAllowedAt = Date.now() + Math.max(this.backoffMs, retryAfter * 1000 + 300);
      } else {
        this.backoffMs = 0;
        this.nextAllowedAt = Date.now() + SPACING_MS;
      }
      this.schedule();
    }
  }
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get("Origin");
    const originAllowed = !!origin && ALLOWED_ORIGINS.has(origin);

    const corsHeaders = {
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "content-type",
      "Access-Control-Expose-Headers": "Retry-After",
      "Access-Control-Max-Age": "86400",
      "Vary": "Origin",
    };
    if (originAllowed) corsHeaders["Access-Control-Allow-Origin"] = origin;

    if (request.method === "OPTIONS") {
      if (!originAllowed) return new Response(null, { status: 403, headers: { Vary: "Origin" } });
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (!originAllowed) {
      return json({ error: "Forbidden" }, 403, { Vary: "Origin" });
    }

    if (request.method !== "GET") {
      return json({ error: "Only GET is supported" }, 405, corsHeaders);
    }

    const url = new URL(request.url);
    const match = url.pathname.match(/^\/community\/farms\/([A-Za-z0-9_-]{1,64})$/);
    if (!match) {
      return json({ error: "Use /community/farms/{farmId}" }, 400, corsHeaders);
    }

    if (!env.SFL_API_KEY) {
      return json({ error: "Worker is missing its SFL_API_KEY secret — set it in Cloudflare dashboard" }, 500, corsHeaders);
    }
    if (!env.FARM_GATE) {
      return json({ error: "Worker is missing its FARM_GATE Durable Object binding" }, 500, corsHeaders);
    }

    let upstream;
    try {
      const stub = env.FARM_GATE.get(env.FARM_GATE.idFromName("gate"));
      upstream = await stub.fetch("https://gate/farm?id=" + encodeURIComponent(match[1]));
    } catch (e) {
      return json({ error: "Gate unavailable: " + (e && e.message ? e.message : "error") }, 502, corsHeaders);
    }

    const body = await upstream.text();
    const headers = { ...corsHeaders, "Content-Type": "application/json", "Cache-Control": "no-store" };
    const retryAfter = upstream.headers.get("retry-after");
    if (retryAfter) headers["Retry-After"] = retryAfter;
    return new Response(body, { status: upstream.status, headers });
  },
};
