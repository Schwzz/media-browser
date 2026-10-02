// Personal media browser: run with `node server.js` (Node 18+), open http://localhost:3000
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { Readable, Transform } = require('stream');
const { pipeline } = require('stream/promises');

const PORT = process.env.PORT || 3000;
// Reachable from your phone on the same Wi-Fi. Set LAN=0 to keep it PC-only.
const HOST = process.env.LAN === '0' ? '127.0.0.1' : '0.0.0.0';

// Settings live in config.json next to this file. Restart the server after editing it.
let cfg = {};
try { cfg = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8')); } catch {}
// "contact" (email or GitHub URL) is sent to sites in the User-Agent; nekos.best requires one.
const CONTACT = cfg.contact || '';
// Largest single download the proxy will pass through or save. Optional "max_download_mb" in config.json.
const maxMb = Number(cfg.max_download_mb);
const MAX_MB = Number.isFinite(maxMb) && maxMb > 0 ? maxMb : 250;
const MAX_BYTES = MAX_MB * 1024 * 1024;
const UA = 'PersonalMediaBrowser/1.0' + (CONTACT ? ` (${CONTACT})` : '');

// Gelbooru needs a free account key (gelbooru.com > Account > Options > API Access Credentials)
const GELBOORU = { user_id: cfg.gelbooru_user_id || '', api_key: cfg.gelbooru_api_key || '' };

// The download/preview proxy only fetches from these hosts
const OK_HOSTS = ['donmai.us', 'yande.re', 'konachan.com', 'konachan.net', 'wallhaven.cc', 'nekos.best', 'waifu.im', 'gelbooru.com'];
// Returns a URL object only if `u` parses, is http(s), and its host is an OK_HOSTS entry or a subdomain
// of one (same rule as before). Returns null for malformed, non-http(s) or disallowed URLs.
const parseAllowed = (u) => {
  let url;
  try { url = new URL(u); } catch { return null; }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return null;
  const h = url.hostname;
  return OK_HOSTS.some((d) => h === d || h.endsWith('.' + d)) ? url : null;
};
const hostOk = (u) => parseAllowed(u) !== null;

// An error that carries the HTTP status the proxy should answer with
class ProxyError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

// Posts carrying any of these tags are dropped (child-coded content)
const BLOCK = new Set(['loli', 'lolicon', 'shota', 'shotacon', 'toddlercon', 'toddler', 'child', 'underage', 'infant', 'baby', 'prepubescent', 'aged_down']);
const blocked = (tags) => (tags || '').split(/\s+/).some((t) => BLOCK.has(t));

const fixUrl = (u) => (u && u.startsWith('//') ? 'https:' + u : u);
const extOf = (u) => String(u || '').split('?')[0].split('.').pop().toLowerCase();
const kind = (ext) =>
  ext === 'gif' ? 'gif'
  : /^(mp4|webm)$/.test(ext) ? 'video'
  : /^(jpe?g|png|webp|avif)$/.test(ext) ? 'image' : null;
const normRating = (r) => (r === 'g' || r === 's' ? 's' : r); // s = safe, q = questionable, e = explicit
const clamp = (n, d) => Math.min(50, Math.max(5, parseInt(n, 10) || d));

// Builds a tag string, adding extra tags (type/order) only while under the site's tag limit
function tagsWith(q, extras, max) {
  const out = (q || '').trim().split(/\s+/).filter(Boolean);
  for (const e of extras) if (e && out.length < max) out.push(e);
  return out.join(' ');
}
const TYPETAG = { gif: 'animated_gif', video: 'video' };

async function getJson(url, ua = UA) {
  const r = await fetch(url, { headers: { 'User-Agent': ua }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error(`Source returned ${r.status}`);
  return r.json();
}

// ---- Adapters: each returns { items: [{title,type,url,thumb,link,rating,w,h,tags}], next } ----
// o = { type: 'all'|'image'|'gif'|'video', lim: items per load }
async function danbooru(host, q, page, sort, o) {
  const p = parseInt(page, 10) || 1;
  const tags = tagsWith(q, [TYPETAG[o.type], { popular: 'order:rank', random: 'order:random' }[sort]], 2);
  const j = await getJson(`https://${host}/posts.json?limit=${o.lim}&page=${p}&tags=${encodeURIComponent(tags)}`);
  const items = [];
  for (const x of j) {
    const type = kind(x.file_ext);
    if (!x.file_url || !type || blocked(x.tag_string)) continue;
    items.push({
      title: '#' + x.id, type, url: x.file_url,
      thumb: type === 'video' ? x.preview_file_url : x.large_file_url || x.file_url,
      link: `https://${host}/posts/${x.id}`,
      rating: normRating(x.rating), w: x.image_width, h: x.image_height, tags: x.tag_string,
    });
  }
  return { items, next: j.length ? String(p + 1) : null };
}

async function moebooru(host, q, page, sort, o) {
  const p = parseInt(page, 10) || 1;
  const tags = tagsWith(q, [{ popular: 'order:score', random: 'order:random' }[sort]], 5);
  const j = await getJson(`https://${host}/post.json?limit=${o.lim}&page=${p}&tags=${encodeURIComponent(tags)}`);
  const items = [];
  for (const x of j) {
    const url = fixUrl(x.file_url);
    const type = url && kind(extOf(url));
    if (!type || blocked(x.tags)) continue;
    items.push({
      title: '#' + x.id, type, url, thumb: fixUrl(x.sample_url) || url,
      link: `https://${host}/post/show/${x.id}`,
      rating: x.rating, w: x.width, h: x.height, tags: x.tags,
    });
  }
  return { items, next: j.length ? String(p + 1) : null };
}

async function wallhaven(q, page, sort) {
  const p = parseInt(page, 10) || 1;
  const sorting = sort === 'popular' ? 'toplist' : sort === 'random' ? 'random' : q ? 'relevance' : 'date_added';
  const j = await getJson(`https://wallhaven.cc/api/v1/search?q=${encodeURIComponent(q || '')}&page=${p}&sorting=${sorting}&purity=100`);
  const items = j.data.map((x) => ({
    title: '#' + x.id, type: 'image', url: x.path, thumb: x.thumbs.large,
    link: x.url, rating: 's', w: x.dimension_x, h: x.dimension_y,
  }));
  return { items, next: p < j.meta.last_page ? String(p + 1) : null };
}

// waifu.im: anonymous anime art API. Search box = tags separated by spaces (e.g. waifu maid).
async function waifuim(q, page, sort, o, nsfw) {
  const p = parseInt(page, 10) || 1;
  const params = new URLSearchParams({ pageNumber: p, pageSize: o.lim });
  (q || '').trim().split(/\s+/).filter(Boolean).forEach((t) => params.append('included_tags', t));
  if (nsfw) params.set('isNsfw', 'true');
  if (o.type === 'gif') params.set('isAnimated', 'true');
  const order = { popular: 'FAVORITES', random: 'RANDOM' }[sort];
  if (order) params.set('orderBy', order);
  const j = await getJson(`https://api.waifu.im/images?${params}`);
  const items = [];
  for (const x of j.items || []) {
    const type = kind(extOf(x.url));
    const tags = (x.tags || []).map((t) => t.slug || t.name).join(' ');
    if (!type || blocked(tags)) continue;
    items.push({
      title: '#' + x.id, type, url: x.url, thumb: x.url,
      link: /^https?:/.test(x.source || '') ? x.source : 'https://www.waifu.im/',
      rating: x.isNsfw ? 'e' : 's', w: x.width, h: x.height, tags,
    });
  }
  return { items, next: j.hasNextPage ? String(p + 1) : null };
}

// Gelbooru: needs the key set at the top of this file. Tags are unlimited.
async function gelbooru(q, page, sort, o) {
  if (!GELBOORU.user_id || !GELBOORU.api_key) throw new Error('Add gelbooru_user_id and gelbooru_api_key to config.json');
  const p = parseInt(page, 10) || 0; // Gelbooru pages start at 0
  const tags = tagsWith(q, [TYPETAG[o.type], { popular: 'sort:score', random: 'sort:random' }[sort]], 20);
  const j = await getJson(
    `https://gelbooru.com/index.php?page=dapi&s=post&q=index&json=1&limit=${o.lim}&pid=${p}` +
    `&tags=${encodeURIComponent(tags)}&api_key=${encodeURIComponent(GELBOORU.api_key)}&user_id=${encodeURIComponent(GELBOORU.user_id)}`
  );
  const posts = [].concat(j.post || []);
  const RATE = { general: 's', sensitive: 's', questionable: 'q', explicit: 'e' };
  const items = [];
  for (const x of posts) {
    const type = kind(extOf(x.file_url));
    if (!type || blocked(x.tags)) continue;
    items.push({
      title: '#' + x.id, type, url: x.file_url, thumb: x.sample_url || x.file_url,
      link: 'https://gelbooru.com/index.php?page=post&s=view&id=' + x.id,
      rating: RATE[x.rating] || 'e', w: x.width, h: x.height, tags: x.tags,
    });
  }
  return { items, next: posts.length ? String(p + 1) : null };
}

// nekos.best: random anime images/GIFs. The search box is the category (neko, waifu, hug, pat...). SFW only.
async function nekos(q, o) {
  if (!CONTACT) throw new Error('Add "contact" (your email or GitHub URL) to config.json. nekos.best requires it');
  const cat = (q || 'neko').trim().toLowerCase().replace(/[^a-z]/g, '') || 'neko';
  const j = await getJson(`https://nekos.best/api/v2/${cat}?amount=${Math.min(o.lim, 20)}`);
  const items = j.results.map((x) => ({
    title: cat + '-' + x.url.split('/').pop().slice(0, 8),
    type: x.url.endsWith('.gif') ? 'gif' : 'image',
    url: x.url, thumb: x.url, link: x.source_url || x.url, rating: 's',
    w: x.dimensions && x.dimensions.width, h: x.dimensions && x.dimensions.height,
  }));
  return { items, next: 'more' }; // random batches, so it never runs out
}

// Tag autocomplete: returns [{name, count}]
async function suggest(source, q) {
  const e = encodeURIComponent(q);
  if (source === 'danbooru' || source === 'safebooru') {
    const host = source === 'danbooru' ? 'danbooru.donmai.us' : 'safebooru.donmai.us';
    const j = await getJson(`https://${host}/tags.json?search[name_matches]=${e}*&search[order]=count&limit=10`);
    return j.map((t) => ({ name: t.name, count: t.post_count }));
  }
  if (source === 'yandere' || source === 'konachan') {
    const host = source === 'yandere' ? 'yande.re' : 'konachan.com';
    const j = await getJson(`https://${host}/tag.json?name_pattern=${e}&order=count&limit=10`);
    return j.map((t) => ({ name: t.name, count: t.count }));
  }
  if (source === 'gelbooru' && GELBOORU.api_key) {
    const j = await getJson(
      `https://gelbooru.com/index.php?page=dapi&s=tag&q=index&json=1&name_pattern=${e}%25&orderby=count&order=DESC&limit=10` +
      `&api_key=${encodeURIComponent(GELBOORU.api_key)}&user_id=${encodeURIComponent(GELBOORU.user_id)}`
    );
    return [].concat(j.tag || []).map((t) => ({ name: t.name, count: t.count }));
  }
  if (source === 'nekos' && CONTACT) {
    const j = await getJson('https://nekos.best/api/v2/endpoints');
    return Object.keys(j).filter((k) => k.startsWith(q.toLowerCase())).slice(0, 10).map((name) => ({ name }));
  }
  return [];
}

const SOURCES = {
  danbooru: (a, o) => danbooru('danbooru.donmai.us', a.q, a.after, a.sort, o),
  safebooru: (a, o) => danbooru('safebooru.donmai.us', a.q, a.after, a.sort, o),
  yandere: (a, o) => moebooru('yande.re', a.q, a.after, a.sort, o),
  konachan: (a, o) => moebooru('konachan.com', a.q, a.after, a.sort, o),
  wallhaven: (a) => wallhaven(a.q, a.after, a.sort),
  nekos: (a, o) => nekos(a.q, o),
  waifuim: (a, o) => waifuim(a.q, a.after, a.sort, o, false),
  waifuimx: (a, o) => waifuim(a.q, a.after, a.sort, o, true),
  gelbooru: (a, o) => gelbooru(a.q, a.after, a.sort, o),
};

// Small feed cache (60s) so scrolling back and re-loading is instant and polite to the sites
const cache = new Map();

// ---- HTTP server ----
const send = (res, code, body, type = 'application/json') => {
  res.writeHead(code, { 'Content-Type': type });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
};
const upHeaders = (url, range) => {
  const h = { 'User-Agent': UA };
  if (new URL(url).hostname.endsWith('gelbooru.com')) h.Referer = 'https://gelbooru.com/';
  if (range) h.Range = range;
  return h;
};

// Rejects early when the upstream declares a size over the limit. The body is cancelled, never streamed.
async function checkDeclaredSize(up) {
  const len = Number(up.headers.get('content-length'));
  if (Number.isFinite(len) && len > MAX_BYTES) {
    await up.body?.cancel().catch(() => {});
    throw new ProxyError(413, `File too large (${Math.round(len / 1048576)} MB, limit ${MAX_MB} MB)`);
  }
}

// Pass-through stream that counts bytes and fails once the limit is exceeded. Content-Length can be
// missing or wrong, so this is the real guard. Failing it makes pipeline() destroy every stream in
// the chain, which also cancels the upstream download.
function limitBytes() {
  let n = 0;
  return new Transform({
    transform(chunk, _enc, cb) {
      n += chunk.length;
      if (n > MAX_BYTES) return cb(new ProxyError(413, `File too large (limit ${MAX_MB} MB)`));
      cb(null, chunk);
    },
  });
}

process.on('unhandledRejection', (e) => console.log('Warning:', e && e.message));

const MAX_REDIRECTS = 5;
const REDIRECT_CODES = new Set([301, 302, 303, 307, 308]);

// fetch() for the proxy routes. Validates the starting URL, then follows redirects manually so every
// hop is checked against the same allowlist (an allowed host can't bounce us to an untrusted one).
async function fetchAllowed(startUrl, { range, signal } = {}) {
  let url = parseAllowed(startUrl);
  if (!url) throw new ProxyError(400, 'Host not allowed');
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    const res = await fetch(url.href, { headers: upHeaders(url.href, range), redirect: 'manual', signal });
    if (!REDIRECT_CODES.has(res.status)) return res;
    await res.body?.cancel().catch(() => {}); // free the connection; we only need the Location header
    const loc = res.headers.get('location');
    if (!loc) throw new ProxyError(502, 'Redirect without a location');
    let target = null;
    try { target = parseAllowed(new URL(loc, url).href); } catch {} // resolves relative redirects too
    if (!target) throw new ProxyError(502, 'Redirect to a disallowed or invalid URL was blocked');
    url = target;
  }
  throw new ProxyError(502, 'Too many redirects');
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  const sp = u.searchParams;
  try {
    if (u.pathname === '/api/feed') {
      const src = SOURCES[sp.get('source')];
      if (!src) return send(res, 400, { error: 'Unknown source' });
      const k = new URLSearchParams(sp); k.delete('nc');
      const key = k.toString();
      const hit = cache.get(key);
      const fresh = sp.get('nc') === '1' || sp.get('sort') === 'random' || sp.get('source') === 'nekos';
      if (hit && !fresh && Date.now() - hit.t < 60000) return send(res, 200, hit.v);
      const out = await src(
        { q: sp.get('q'), after: sp.get('after'), sort: sp.get('sort') },
        { type: sp.get('type') || 'all', lim: clamp(sp.get('limit'), 15) }
      );
      if (cache.size > 100) cache.clear();
      cache.set(key, { t: Date.now(), v: out });
      return send(res, 200, out);
    }
    if (u.pathname === '/api/tags') {
      const q = (sp.get('q') || '').slice(0, 60);
      try { return send(res, 200, q.length < 2 ? [] : await suggest(sp.get('source'), q)); }
      catch { return send(res, 200, []); }
    }
    if (u.pathname === '/api/save') {
      const url = sp.get('url');
      if (!hostOk(url)) return send(res, 400, { error: 'Host not allowed' });
      const name = (sp.get('name') || 'file').replace(/[^\w.-]/g, '_').slice(0, 100);
      const dir = path.join(__dirname, 'downloads');
      fs.mkdirSync(dir, { recursive: true });
      const file = path.join(dir, name);
      if (fs.existsSync(file)) return send(res, 200, { ok: true, file: name, existed: true });
      const up = await fetchAllowed(url, { signal: AbortSignal.timeout(120000) });
      if (!up.ok) { await up.body?.cancel().catch(() => {}); throw new Error('Source returned ' + up.status); }
      await checkDeclaredSize(up); // too big: rejected before any file is created
      try { await pipeline(Readable.fromWeb(up.body), limitBytes(), fs.createWriteStream(file + '.part')); }
      catch (e) { fs.rmSync(file + '.part', { force: true }); throw e; }
      fs.renameSync(file + '.part', file);
      return send(res, 200, { ok: true, file: name });
    }
    if (u.pathname === '/api/dl') {
      const url = sp.get('url');
      if (!hostOk(url)) return send(res, 400, { error: 'Host not allowed' });
      // Cancel the upstream download if the browser stops waiting (scrolled away, closed viewer)
      const ac = new AbortController();
      res.on('close', () => ac.abort());
      const up = await fetchAllowed(url, { range: req.headers.range, signal: ac.signal });
      await checkDeclaredSize(up); // too big: JSON 413 before any headers/body are sent
      const h = { 'Content-Type': up.headers.get('content-type') || 'application/octet-stream' };
      for (const k of ['content-length', 'content-range', 'accept-ranges']) {
        if (up.headers.get(k)) h[k] = up.headers.get(k);
      }
      if (sp.get('inline')) {
        h['Cache-Control'] = 'public, max-age=86400'; // repeat views come from the browser cache
      } else {
        const name = (sp.get('name') || 'download').replace(/[^\w.-]/g, '_').slice(0, 80);
        h['Content-Disposition'] = `attachment; filename="${name}"`;
      }
      res.writeHead(up.status, h);
      // Headers are already sent, so an overrun here can't change the status: the connection is cut instead.
      return pipeline(Readable.fromWeb(up.body), limitBytes(), res).catch((e) => {
        if (e instanceof ProxyError) console.log(`Stopped oversized download: ${url}`);
      });
    }
    if (u.pathname === '/') {
      return send(res, 200, fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8'), 'text/html');
    }
    send(res, 404, { error: 'Not found' });
  } catch (e) {
    if (!res.headersSent && !res.destroyed) send(res, e instanceof ProxyError ? e.status : 500, { error: e.message });
  }
}).listen(PORT, HOST, () => {
  console.log(`Running at http://localhost:${PORT}`);
  if (HOST !== '127.0.0.1') {
    for (const list of Object.values(os.networkInterfaces())) {
      for (const n of list) {
        if (n.family === 'IPv4' && !n.internal) console.log(`On your phone (same Wi-Fi): http://${n.address}:${PORT}`);
      }
    }
  }
});
