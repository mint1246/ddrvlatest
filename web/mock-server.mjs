import { createServer } from 'node:http';

const PORT = 2525;
const TOKEN = 'ddrv-local-demo-token';
let counter = 20;

const now = Date.now();
const item = (id, name, dir, parent, size = 0, daysAgo = 0) => ({
  id, name, dir, size, parent, mtime: new Date(now - daysAgo * 86_400_000).toISOString(),
});

const items = new Map([
  ['root', item('root', '/', true, null)],
  ['projects', item('projects', 'Projects', true, 'root', 0, 1)],
  ['photos', item('photos', 'Photography', true, 'root', 0, 3)],
  ['archive', item('archive', 'Archive', true, 'root', 0, 40)],
  ['welcome', item('welcome', 'Welcome to DDrv.md', false, 'root', 1268, 0)],
  ['sunset', item('sunset', 'lagoon-sunset.svg', false, 'root', 4890, 2)],
  ['budget', item('budget', 'Q3 budget.csv', false, 'root', 34812, 4)],
  ['bundle', item('bundle', 'release-bundle.zip', false, 'root', 18_463_004, 7)],
  ['roadmap', item('roadmap', 'Product roadmap.md', false, 'projects', 2940, 1)],
  ['spec', item('spec', 'storage-api.json', false, 'projects', 9234, 6)],
  ['portrait', item('portrait', 'desert-study.svg', false, 'photos', 3821, 10)],
]);

const contents = new Map([
  ['welcome', '# Welcome to DDrv\n\nThis is the local UI development workspace.\n\n- Upload files with **Add** or drag and drop.\n- Select an item for bulk actions.\n- Press **Ctrl+K** to open the command menu.\n- Choose Calm, Balanced, or Power in the control center.\n'],
  ['roadmap', '# Product roadmap\n\n## Now\n- Material 3 Expressive React workspace\n- Resilient transfer center\n- Text editing and previews\n\n## Next\n- Shareable collections\n'],
  ['spec', JSON.stringify({ name: 'DDrv', api: 3, features: ['files', 'folders', 'uploads', 'previews', 'streaming'] }, null, 2)],
  ['budget', 'Category,Planned,Actual\nInfrastructure,42000,38650\nDesign,18000,17220\nResearch,12000,9800\n'],
]);

const lagoonSvg = (title, a = '#006b61', b = '#ffb56b') => `
<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="800" viewBox="0 0 1200 800">
  <defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="${a}"/><stop offset="1" stop-color="${b}"/></linearGradient></defs>
  <rect width="1200" height="800" fill="#ecf4ef"/><circle cx="920" cy="205" r="170" fill="url(#g)" opacity=".92"/>
  <path d="M0 610Q190 470 380 610T760 610T1200 590V800H0Z" fill="${a}" opacity=".78"/>
  <path d="M0 690Q230 535 480 690T930 670T1200 650V800H0Z" fill="${b}" opacity=".65"/>
  <text x="72" y="112" font-family="system-ui" font-size="54" font-weight="700" fill="#12332d">${title}</text>
</svg>`;

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
};

function send(res, status, body, headers = {}) {
  const payload = typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
  res.writeHead(status, { ...cors, 'Content-Type': typeof body === 'object' && !Buffer.isBuffer(body) ? 'application/json' : 'text/plain; charset=utf-8', ...headers });
  res.end(payload);
}

const ok = (res, data, status = 200) => send(res, status, { message: status === 201 ? 'created' : 'ok', data });
const error = (res, status, message) => send(res, status, { message });
const authorized = (req) => req.headers.authorization === `Bearer ${TOKEN}`;
const children = (id) => [...items.values()].filter((entry) => entry.parent === id);

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks);
}

function removeTree(id) {
  for (const child of children(id)) removeTree(child.id);
  items.delete(id);
  contents.delete(id);
}

createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://127.0.0.1:${PORT}`);
  const path = decodeURIComponent(url.pathname);
  if (req.method === 'OPTIONS') return send(res, 204, '');

  if (req.method === 'GET' && path === '/api/config') return ok(res, { login: true, anonymous: true });
  if (req.method === 'POST' && path === '/api/user/login') {
    const data = JSON.parse((await readBody(req)).toString() || '{}');
    return data.username === 'demo' && data.password === 'demo' ? ok(res, { token: TOKEN }) : error(res, 401, 'Use demo / demo for the local workspace.');
  }
  if (req.method === 'GET' && path === '/api/check_token') return authorized(req) ? ok(res, { valid: true }) : error(res, 401, 'invalid token');

  const directoryMatch = path.match(/^\/api\/directories\/([^/]+)$/);
  if (req.method === 'GET' && directoryMatch) {
    const directory = items.get(directoryMatch[1]);
    return directory?.dir ? ok(res, { ...directory, files: children(directory.id) }) : error(res, 404, 'folder not found');
  }

  if (path.startsWith('/api/') && req.method !== 'GET' && !authorized(req)) return error(res, 401, 'Sign in to make changes.');

  if (req.method === 'POST' && path === '/api/directories/') {
    const data = JSON.parse((await readBody(req)).toString() || '{}');
    if (children(data.parent || 'root').some((entry) => entry.name === data.name)) return error(res, 409, 'file already exists');
    const created = item(`folder-${++counter}`, data.name, true, data.parent || 'root');
    items.set(created.id, created);
    return ok(res, created, 201);
  }

  const fileMutationMatch = path.match(/^\/api\/directories\/([^/]+)\/files\/([^/]+)$/);
  const targetId = fileMutationMatch?.[2] || directoryMatch?.[1];
  if (targetId && req.method === 'PUT') {
    const target = items.get(targetId);
    if (!target) return error(res, 404, 'item not found');
    const data = JSON.parse((await readBody(req)).toString() || '{}');
    Object.assign(target, data.name ? { name: data.name } : {}, data.parent ? { parent: data.parent } : {}, { mtime: new Date().toISOString() });
    return ok(res, target);
  }
  if (targetId && req.method === 'DELETE') {
    if (!items.has(targetId)) return error(res, 404, 'item not found');
    removeTree(targetId);
    return send(res, 200, { message: 'deleted' });
  }

  const overwriteMatch = path.match(/^\/api\/directories\/([^/]+)\/files\/([^/]+)\/content$/);
  if (req.method === 'PUT' && overwriteMatch) {
    const content = (await readBody(req)).toString();
    contents.set(overwriteMatch[2], content);
    const target = items.get(overwriteMatch[2]);
    if (target) Object.assign(target, { size: Buffer.byteLength(content), mtime: new Date().toISOString() });
    return ok(res, target || {});
  }

  const uploadMatch = path.match(/^\/api\/directories\/([^/]+)\/files$/);
  if (req.method === 'POST' && uploadMatch) {
    const body = await readBody(req);
    const header = body.toString('latin1', 0, Math.min(body.length, 4096));
    const filename = header.match(/filename="([^"]+)"/)?.[1] || `upload-${counter}.bin`;
    const created = item(`file-${++counter}`, filename, false, uploadMatch[1], Math.max(0, body.length - 180));
    items.set(created.id, created);
    contents.set(created.id, `Local preview for ${filename}`);
    return ok(res, created, 201);
  }

  const manifestMatch = path.match(/^\/files\/([^/]+)\/manifest$/);
  if (req.method === 'GET' && manifestMatch) {
    const target = items.get(manifestMatch[1]);
    if (!target) return error(res, 404, 'file not found');
    const content = contents.get(target.id) || `Mock content for ${target.name}`;
    const bytes = Buffer.byteLength(content);
    return send(res, 200, { total_chunks: 1, name: target.name, mime: 'application/octet-stream', size: bytes, chunks: [{ url: `http://127.0.0.1:${PORT}/chunks/${target.id}`, download_url: `http://127.0.0.1:${PORT}/chunks/${target.id}`, size: bytes }] }, { 'Content-Type': 'application/json' });
  }

  const chunkMatch = path.match(/^\/chunks\/([^/]+)$/);
  if (req.method === 'GET' && chunkMatch) return send(res, 200, contents.get(chunkMatch[1]) || `Mock content for ${items.get(chunkMatch[1])?.name || 'file'}`, { 'Content-Type': 'application/octet-stream' });

  const streamMatch = path.match(/^\/files\/([^/]+)(?:\/.*)?$/);
  if (req.method === 'GET' && streamMatch) {
    const target = items.get(streamMatch[1]);
    if (!target || target.dir) return error(res, 404, 'file not found');
    if (target.id === 'sunset') return send(res, 200, lagoonSvg('Lagoon sunset'), { 'Content-Type': 'image/svg+xml' });
    if (target.id === 'portrait') return send(res, 200, lagoonSvg('Desert study', '#8f5225', '#72518f'), { 'Content-Type': 'image/svg+xml' });
    return send(res, 200, contents.get(target.id) || `Mock content for ${target.name}`, { 'Content-Type': target.name.endsWith('.json') ? 'application/json' : 'text/plain; charset=utf-8' });
  }

  return error(res, 404, 'mock route not found');
}).listen(PORT, '127.0.0.1', () => console.log(`DDrv mock API: http://127.0.0.1:${PORT}`));
