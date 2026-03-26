/**
 * DDrv – Material 3 Expressive WebUI
 * Vanilla JS, no framework dependencies
 */

/* ══════════════════════════════════════════════════════════
   State
═══════════════════════════════════════════════════════════ */
function readTokenFromCookie() {
  const entry = document.cookie
    .split(';')
    .map(s => s.trim())
    .find(c => c.startsWith('ddrv_token='));
  if (!entry) return null;
  return decodeURIComponent(entry.split('=')[1] || '');
}

const state = {
  config: { login: false, anonymous: true },
  authenticated: false,
  token: localStorage.getItem('auth_token') || readTokenFromCookie() || null,
  directory: { id: 'root', name: '/', files: [], parent: null },
  breadcrumbs: [{ id: 'root', name: 'Home' }],
  selected: new Set(),   // ids
  filterText: '',
  sortBy: 'name',
  sortDesc: false,
  viewMode: localStorage.getItem('view_mode') || 'list',
  isDragging: false,
  uploads: new Map(),    // filename → { el, pct }
  viewer: null,
  snackTimer: null,
};

/* ══════════════════════════════════════════════════════════
   API
═══════════════════════════════════════════════════════════ */
const api = {
  _headers() {
    const h = { 'Content-Type': 'application/json' };
    if (state.token) h['Authorization'] = 'Bearer ' + state.token;
    return h;
  },

  async _fetch(url, opts = {}) {
    const res = await fetch(url, {
      headers: this._headers(),
      ...opts,
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({ message: res.statusText }));
      const err = new Error(body.message || res.statusText);
      err.status = res.status;
      throw err;
    }
    return res.json();
  },

  async getConfig() {
    return this._fetch('/api/config');
  },

  async checkToken() {
    return this._fetch('/api/check_token');
  },

  async login(username, password) {
    const res = await this._fetch('/api/user/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    const token = res?.data?.token;
    if (!token) throw new Error('Invalid login response');
    setAuthToken(token);
    return token;
  },

  logout() {
    clearAuthToken();
  },

  async getDir(id) {
    const dirId = id || 'root';
    const res = await this._fetch('/api/directories/' + dirId);
    const dir = res.data;
    if (!dir.files) dir.files = [];
    dir.files = dir.files.map(f => ({
      ...f,
      rawSize: f.size,
      size: f.dir ? null : humanReadableSize(f.size),
      mtime: new Date(f.mtime),
    }));
    return dir;
  },

  async createDir(name, parentId) {
    return this._fetch('/api/directories/', {
      method: 'POST',
      body: JSON.stringify({ name, parent: parentId || 'root' }),
    });
  },

  async updateDir(id, data) {
    return this._fetch('/api/directories/' + id, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async deleteItem(id) {
    const headers = {};
    if (state.token) headers['Authorization'] = 'Bearer ' + state.token;
    const res = await fetch('/api/directories/' + id, {
      method: 'DELETE',
      headers,
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({ message: res.statusText }));
      const err = new Error(body.message || res.statusText);
      err.status = res.status;
      throw err;
    }
  },

  async updateFile(dirId, id, data) {
    return this._fetch('/api/directories/' + dirId + '/files/' + id, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  },

  async overwriteFile(dirId, id, content, mime = 'text/plain;charset=utf-8') {
    const headers = {};
    if (state.token) headers['Authorization'] = 'Bearer ' + state.token;
    if (mime) headers['Content-Type'] = mime;
    const res = await fetch('/api/directories/' + dirId + '/files/' + id + '/content', {
      method: 'PUT',
      headers,
      body: content,
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({ message: res.statusText }));
      const err = new Error(body.message || res.statusText);
      err.status = res.status;
      throw err;
    }
    return res.json();
  },

  uploadFile(dirId, file, onProgress) {
    return new Promise((resolve, reject) => {
      const formData = new FormData();
      formData.append('file', file);

      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/directories/' + dirId + '/files');
      if (state.token) xhr.setRequestHeader('Authorization', 'Bearer ' + state.token);

      xhr.upload.onprogress = e => {
        if (e.lengthComputable) onProgress(Math.round(e.loaded / e.total * 100));
      };
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(JSON.parse(xhr.responseText));
        } else {
          const body = JSON.parse(xhr.responseText || '{}');
          const err = new Error(body.message || xhr.statusText);
          err.status = xhr.status;
          reject(err);
        }
      };
      xhr.onerror = () => reject(new Error('Network error'));
      xhr.send(formData);
    });
  },
};

/* ══════════════════════════════════════════════════════════
   Helpers
═══════════════════════════════════════════════════════════ */
function setAuthToken(token) {
  state.token = token;
  localStorage.setItem('auth_token', token);
  document.cookie = `ddrv_token=${encodeURIComponent(token)}; Max-Age=${60 * 60 * 24 * 30}; Path=/; SameSite=Lax`;
}

function clearAuthToken() {
  state.token = null;
  localStorage.removeItem('auth_token');
  document.cookie = 'ddrv_token=; Max-Age=0; Path=/; SameSite=Lax';
}

function humanReadableSize(bytes, si = false, dp = 1) {
  const thresh = si ? 1000 : 1024;
  if (Math.abs(bytes) < thresh) return bytes + ' B';
  const units = si
    ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  let u = -1;
  const r = 10 ** dp;
  do { bytes /= thresh; u++; }
  while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);
  return bytes.toFixed(dp) + ' ' + units[u];
}

function formatDate(d) {
  if (!d) return '';
  const now = new Date();
  const diff = now - d;
  if (diff < 60000) return 'just now';
  if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
  if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
  if (diff < 604800000) return Math.floor(diff / 86400000) + 'd ago';
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: d.getFullYear() !== now.getFullYear() ? 'numeric' : undefined });
}

function fileIcon(file) {
  if (file.dir) return 'folder';
  const ext = (file.name || '').split('.').pop().toLowerCase();
  const map = {
    pdf: 'picture_as_pdf',
    doc: 'description', docx: 'description', odt: 'description', rtf: 'description', txt: 'description',
    xls: 'table_chart', xlsx: 'table_chart', ods: 'table_chart', csv: 'table_chart',
    ppt: 'slideshow', pptx: 'slideshow', odp: 'slideshow',
    jpg: 'image', jpeg: 'image', png: 'image', gif: 'image', webp: 'image', svg: 'image', bmp: 'image', ico: 'image',
    mp4: 'videocam', mkv: 'videocam', avi: 'videocam', mov: 'videocam', webm: 'videocam', flv: 'videocam',
    mp3: 'music_note', ogg: 'music_note', wav: 'music_note', flac: 'music_note', aac: 'music_note', m4a: 'music_note',
    zip: 'folder_zip', tar: 'folder_zip', gz: 'folder_zip', rar: 'folder_zip', '7z': 'folder_zip',
    js: 'code', ts: 'code', py: 'code', rs: 'code', go: 'code', java: 'code', c: 'code', cpp: 'code',
    html: 'code', css: 'code', json: 'code', xml: 'code', yaml: 'code', yml: 'code',
    exe: 'terminal', sh: 'terminal', bash: 'terminal', cmd: 'terminal',
  };
  return map[ext] || 'insert_drive_file';
}

function fileStreamUrl(file) {
  return '/files/' + file.id + '/' + encodeURIComponent(file.name);
}

function isAudioFile(file) {
  const ext = (file.name || '').split('.').pop().toLowerCase();
  return ['mp3', 'ogg', 'wav', 'flac', 'aac', 'm4a'].includes(ext);
}

function isVideoFile(file) {
  const ext = (file.name || '').split('.').pop().toLowerCase();
  return ['mp4', 'mkv', 'avi', 'mov', 'webm'].includes(ext);
}

function isImageFile(file) {
  const ext = (file.name || '').split('.').pop().toLowerCase();
  return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'ico', 'svg'].includes(ext);
}

function isTextFile(file) {
  const ext = (file.name || '').split('.').pop().toLowerCase();
  return ['txt', 'md', 'json', 'js', 'ts', 'py', 'rs', 'go', 'java', 'c', 'cpp', 'css', 'html', 'csv', 'log', 'yaml', 'yml'].includes(ext);
}

function filteredAndSorted() {
  const query = (state.filterText || '').toLowerCase().trim();
  let files = (state.directory.files || []).filter(f => !query || f.name.toLowerCase().includes(query));
  const key = state.sortBy;
  files.sort((a, b) => {
    // Folders always first
    if (a.dir !== b.dir) return a.dir ? -1 : 1;
    let av, bv;
    if (key === 'mtime') {
      av = a.mtime ? a.mtime.getTime() : 0;
      bv = b.mtime ? b.mtime.getTime() : 0;
    } else if (key === 'size') {
      av = a.dir ? -1 : (a.rawSize || 0);
      bv = b.dir ? -1 : (b.rawSize || 0);
    } else {
      av = (a.name || '').toLowerCase();
      bv = (b.name || '').toLowerCase();
    }
    if (av < bv) return state.sortDesc ? 1 : -1;
    if (av > bv) return state.sortDesc ? -1 : 1;
    return 0;
  });
  return files;
}

/* ══════════════════════════════════════════════════════════
   Snackbar
═══════════════════════════════════════════════════════════ */
function showSnack(msg, type = '') {
  const el = document.getElementById('snackbar');
  el.textContent = msg;
  el.classList.remove('hidden', 'error');
  if (type) el.classList.add(type);
  clearTimeout(state.snackTimer);
  state.snackTimer = setTimeout(() => el.classList.add('hidden'), 4000);
}

/* ══════════════════════════════════════════════════════════
   Render
═══════════════════════════════════════════════════════════ */
function renderBreadcrumbs() {
  const nav = document.getElementById('breadcrumb');
  nav.innerHTML = '';
  state.breadcrumbs.forEach((crumb, i) => {
    const isLast = i === state.breadcrumbs.length - 1;
    const btn = document.createElement('button');
    btn.className = 'breadcrumb-item' + (isLast ? ' active' : '');
    if (i === 0) {
      btn.innerHTML = '<span class="material-symbols-outlined" style="font-size:18px">home</span>' + crumb.name;
    } else {
      btn.textContent = crumb.name;
    }
    if (!isLast) {
      btn.addEventListener('click', () => navigateTo(crumb.id, i));
    }
    nav.appendChild(btn);
    if (!isLast) {
      const sep = document.createElement('span');
      sep.className = 'breadcrumb-sep';
      sep.textContent = '/';
      sep.setAttribute('aria-hidden', 'true');
      nav.appendChild(sep);
    }
  });
}

function renderFiles() {
  const files = filteredAndSorted();
  const container = document.getElementById('files-container');
  const empty = document.getElementById('empty-state');
  const loading = document.getElementById('loading-state');

  loading.classList.add('hidden');
  container.innerHTML = '';

  if (files.length === 0) {
    container.classList.add('hidden');
    empty.classList.remove('hidden');
    document.getElementById('item-count').textContent = '';
    return;
  }

  container.classList.remove('hidden');
  empty.classList.add('hidden');
  document.getElementById('item-count').textContent = files.length + ' item' + (files.length !== 1 ? 's' : '');

  const folders = files.filter(f => f.dir);
  const fileItems = files.filter(f => !f.dir);

  if (state.viewMode === 'grid' && folders.length > 0) {
    // Folders section
    const secLabel = document.createElement('div');
    secLabel.className = 'section-label';
    secLabel.textContent = 'Folders';
    container.appendChild(secLabel);

    const grid = document.createElement('div');
    grid.className = 'folders-grid';
    folders.forEach(f => grid.appendChild(makeFolderCard(f)));
    container.appendChild(grid);

    if (fileItems.length > 0) {
      const secLabel2 = document.createElement('div');
      secLabel2.className = 'section-label';
      secLabel2.style.marginTop = '12px';
      secLabel2.textContent = 'Files';
      container.appendChild(secLabel2);

      const list = document.createElement('div');
      list.className = 'files-list';
      fileItems.forEach(f => list.appendChild(makeFileRow(f)));
      container.appendChild(list);
    }
  } else {
    // List view: all items in one list
    const list = document.createElement('div');
    list.className = 'files-list';
    files.forEach(f => list.appendChild(makeFileRow(f)));
    container.appendChild(list);
  }
}

function makeFolderCard(file) {
  const card = document.createElement('div');
  card.className = 'folder-card' + (state.selected.has(file.id) ? ' selected' : '');
  card.setAttribute('data-id', file.id);
  card.setAttribute('tabindex', '0');
  card.setAttribute('role', 'button');
  card.setAttribute('aria-label', 'Folder: ' + file.name);

  card.innerHTML = `
    <div class="folder-icon">
      <span class="material-symbols-outlined">folder</span>
    </div>
    <div class="folder-name" title="${escHtml(file.name)}">${escHtml(file.name)}</div>
    ${file.mtime ? `<div class="folder-meta">${formatDate(file.mtime)}</div>` : ''}
    <div class="select-overlay" aria-hidden="true">
      <span class="material-symbols-outlined">check</span>
    </div>`;

  card.addEventListener('click', e => {
    if (state.selected.size > 0) {
      toggleSelect(file.id, card);
    } else {
      navigateTo(file.id);
    }
  });

  card.addEventListener('contextmenu', e => {
    e.preventDefault();
    toggleSelect(file.id, card);
  });

  return card;
}

function makeFileRow(file) {
  const row = document.createElement('div');
  row.className = 'file-row' + (state.selected.has(file.id) ? ' selected' : '');
  row.setAttribute('data-id', file.id);
  row.setAttribute('tabindex', '0');
  row.setAttribute('role', 'row');

  const icon = fileIcon(file);
  const dateStr = file.mtime ? formatDate(file.mtime) : '';
  const sizeStr = file.size || (file.dir ? 'Folder' : '');

  row.innerHTML = `
    <div class="file-checkbox" aria-hidden="true">
      <span class="material-symbols-outlined">check</span>
    </div>
    <div class="file-icon-wrap">
      <span class="material-symbols-outlined">${escHtml(icon)}</span>
    </div>
    <div class="file-info">
      <div class="file-name" title="${escHtml(file.name)}">${escHtml(file.name)}</div>
      <div class="file-meta">
        ${sizeStr ? `<span>${sizeStr}</span>` : ''}
        ${dateStr ? `<span>${dateStr}</span>` : ''}
      </div>
    </div>
    <div class="file-size" aria-label="Size: ${sizeStr}">${sizeStr}</div>
    <div class="file-date" aria-label="Modified: ${dateStr}">${dateStr}</div>`;

  row.addEventListener('click', e => {
    if (state.selected.size > 0 || e.ctrlKey || e.metaKey) {
      toggleSelect(file.id, row);
    } else if (file.dir) {
      navigateTo(file.id);
    } else {
      openFile(file);
    }
  });

  row.addEventListener('dblclick', () => {
    if (!file.dir) openFile(file);
  });

  row.addEventListener('contextmenu', e => {
    e.preventDefault();
    toggleSelect(file.id, row);
  });

  return row;
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/* ══════════════════════════════════════════════════════════
   Selection
═══════════════════════════════════════════════════════════ */
function toggleSelect(id, el) {
  if (state.selected.has(id)) {
    state.selected.delete(id);
    el.classList.remove('selected');
  } else {
    state.selected.add(id);
    el.classList.add('selected');
  }
  updateSelectionToolbar();
}

function clearSelection() {
  state.selected.clear();
  document.querySelectorAll('.folder-card.selected, .file-row.selected').forEach(el => el.classList.remove('selected'));
  updateSelectionToolbar();
}

function updateSelectionToolbar() {
  const toolbar = document.getElementById('selection-toolbar');
  const count = state.selected.size;
  const countEl = document.getElementById('selection-count');
  const topBar = document.getElementById('top-app-bar');

  if (count > 0) {
    toolbar.classList.remove('hidden');
    topBar.classList.add('hidden');
    countEl.textContent = count + ' selected';

    // Copy link only for single file
    const copyBtn = document.getElementById('copy-link-btn');
    const selectedFiles = state.directory.files.filter(f => state.selected.has(f.id));
    copyBtn.disabled = !(selectedFiles.length === 1 && !selectedFiles[0].dir);
    copyBtn.style.opacity = copyBtn.disabled ? '0.38' : '';
  } else {
    toolbar.classList.add('hidden');
    topBar.classList.remove('hidden');
  }
}

/* ══════════════════════════════════════════════════════════
   Navigation
═══════════════════════════════════════════════════════════ */
async function navigateTo(id, crumbIndex = null) {
  clearSelection();
  document.getElementById('loading-state').classList.remove('hidden');
  document.getElementById('files-container').classList.add('hidden');
  document.getElementById('empty-state').classList.add('hidden');

  try {
    const dir = await api.getDir(id);
    state.directory = dir;

    if (crumbIndex !== null) {
      // Navigate back to a specific breadcrumb
      state.breadcrumbs = state.breadcrumbs.slice(0, crumbIndex + 1);
    } else if (id === 'root') {
      state.breadcrumbs = [{ id: 'root', name: 'Home' }];
    } else {
      // Check if already in breadcrumbs (back navigation)
      const existing = state.breadcrumbs.findIndex(b => b.id === id);
      if (existing >= 0) {
        state.breadcrumbs = state.breadcrumbs.slice(0, existing + 1);
      } else {
        state.breadcrumbs.push({ id: dir.id, name: dir.name });
      }
    }

    renderBreadcrumbs();
    renderFiles();
  } catch (err) {
    document.getElementById('loading-state').classList.add('hidden');
    if (err.status === 401) {
      handleUnauthorized();
    } else {
      showSnack('Failed to load directory: ' + err.message, 'error');
    }
  }
}

function openFile(file) {
  if (file.dir) {
    navigateTo(file.id);
    return;
  }
  if (isAudioFile(file) || isVideoFile(file) || isImageFile(file) || isTextFile(file)) {
    openViewer(file);
    return;
  }
  downloadFile(file);
}

/* ══════════════════════════════════════════════════════════
   Client-side file download via manifest
   Chunks are fetched directly from Discord CDN in batches.
   The server is only used to look up metadata and refresh
   expiring CDN URLs – it never proxies the file bytes.
═══════════════════════════════════════════════════════════ */

// How many chunk URLs to request per manifest call.  Keeping this small means
// only a few Discord URLs need refreshing per request, avoiding rate limiting.
const MANIFEST_BATCH = 5;

// How long (ms) to keep the Blob URL alive after triggering the Save dialog.
// The browser's download manager needs time to start reading the URL before
// it can be revoked; 30 s is ample even on slow devices.
const BLOB_URL_REVOKE_DELAY_MS = 30000;
const DISCORD_CDN_HOSTS = ['cdn.discordapp.com'];

function buildChunkUrlCandidates(chunk) {
  const urls = [];
  const seen = new Set();
  const preferred = chunk?.download_url || null;

  function add(url) {
    if (!url || seen.has(url)) return;
    seen.add(url);
    urls.push(url);
  }

  // Try the download=1 variant first because some edges attach
  // more permissive CORS headers on explicit download responses.
  add(preferred);
  add(chunk?.url);

  // Only swap among CDN hosts we know serve raw attachments.
  function addHostVariants(url) {
    try {
      const u = new URL(url);
      if (!DISCORD_CDN_HOSTS.includes(u.hostname)) return;
      for (const host of DISCORD_CDN_HOSTS) {
        if (host === u.hostname) continue;
        const copy = new URL(url);
        copy.hostname = host;
        add(copy.toString());
      }
    } catch (_) {
      /* ignore parse errors */
    }
  }

  if (preferred) addHostVariants(preferred);
  if (chunk?.url) addHostVariants(chunk.url);

  return urls;
}

async function fetchChunkBuffer(chunk) {
  const candidates = buildChunkUrlCandidates(chunk);
  let lastErr = null;

  for (const url of candidates) {
    try {
      const res = await fetch(url, { mode: 'cors', referrerPolicy: 'no-referrer' });
      if (!res.ok) throw new Error('Chunk fetch failed: ' + res.status);
      return await res.arrayBuffer();
    } catch (err) {
      console.warn('Chunk fetch failed, trying next host', url, err);
      lastErr = err;
    }
  }

  throw lastErr || new Error('Chunk fetch failed');
}

async function downloadFile(file) {
  // For directories there is nothing to download.
  if (file.dir) return;

  const progressContainer = document.getElementById('upload-progress');
  const cardId = 'dl-' + file.id;

  // Build a download-progress card reusing the upload-card styles.
const card = document.createElement('div');
  card.className = 'upload-card';
  card.id = cardId;
  card.innerHTML =
    '<div class="upload-header">' +
      '<div class="upload-filename">' + escHtml(file.name) + '</div>' +
      '<div class="upload-pct">0%</div>' +
    '</div>' +
    '<div class="upload-meta">' +
      '<span class="upload-speed">—</span>' +
      '<span class="upload-eta">--:--</span>' +
      '<span class="upload-elapsed">0:00</span>' +
    '</div>' +
    '<div class="progress-track"><div class="progress-fill" style="width:0%"></div></div>';
  progressContainer.classList.remove('hidden');
  progressContainer.appendChild(card);

  const pctEl  = card.querySelector('.upload-pct');
  const fillEl = card.querySelector('.progress-fill');
  const speedEl = card.querySelector('.upload-speed');
  const etaEl = card.querySelector('.upload-eta');
  const elapsedEl = card.querySelector('.upload-elapsed');

  function setProgress(pct) {
    if (pctEl)  pctEl.textContent  = Math.round(pct) + '%';
    if (fillEl) fillEl.style.width = Math.round(pct) + '%';
  }

  function formatDuration(seconds) {
    const s = Math.max(0, Math.round(seconds));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const r = s % 60;
    if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(r).padStart(2, '0')}`;
    return `${m}:${String(r).padStart(2, '0')}`;
  }

  try {
    const buffers = [];
    let offset       = 0;
    let totalChunks  = null;
    let totalBytes   = file.rawSize || file.size || 0;
    let downloaded   = 0;
    // Track how many individual chunks have completed so progress is smooth.
    let doneChunks   = 0;
    let lastName     = file.name;
    let lastMime     = 'application/octet-stream';
    const startedAt  = performance.now();
    let writer       = null;
    let streamSave   = false;
    let saveFailed   = false;
    let flushQueue   = [];

    // Stream directly to disk when supported to avoid large in-memory buffers.
    if (window.showSaveFilePicker) {
      try {
        const handle = await window.showSaveFilePicker({
          suggestedName: file.name || 'download',
        });
        writer = await handle.createWritable();
        streamSave = true;
      } catch (err) {
        streamSave = false; // user cancelled or API blocked; fall back to blob
      }
    }

    function updateStats() {
      const elapsedSec = (performance.now() - startedAt) / 1000;
      const speed = elapsedSec > 0 ? downloaded / elapsedSec : 0;
      if (speedEl) speedEl.textContent = speed > 0 ? humanReadableSize(speed, true, 1) + '/s' : '—';
      const remaining = Math.max(0, totalBytes - downloaded);
      const eta = speed > 0 ? formatDuration(remaining / speed) : '--:--';
      if (etaEl) etaEl.textContent = eta;
      if (elapsedEl) elapsedEl.textContent = formatDuration(elapsedSec);
    }

    // Fetch chunk URLs in batches to limit Discord URL-refresh API calls.
    do {
      const manifestUrl =
        '/files/' + file.id + '/manifest' +
        '?offset=' + offset + '&limit=' + MANIFEST_BATCH;
      const res = await fetch(manifestUrl);
      if (!res.ok) throw new Error('Manifest request failed: ' + res.status);

      const manifest = await res.json();
      if (totalChunks === null) {
        totalChunks = manifest.total_chunks;
        lastName    = manifest.name || file.name;
        lastMime    = manifest.mime || 'application/octet-stream';
        totalBytes  = manifest.size || totalBytes;
      }

      // Download each chunk in this batch directly from Discord CDN in parallel.
      // Update progress as each individual chunk completes for a smooth bar.
      const chunkBuffers = [];
      for (const chunk of manifest.chunks) {
        const buf = await fetchChunkBuffer(chunk);
        const chunkBytes = chunk?.size || buf.byteLength || 0;
        downloaded += chunkBytes;
        doneChunks++;
        setProgress(totalChunks > 0 ? (doneChunks / totalChunks) * 100 : 0);
        updateStats();

        if (streamSave && writer && !saveFailed) {
          // enqueue writes to avoid overwhelming the writer with parallel promises
          flushQueue.push((async () => {
            try {
              await writer.write(new Uint8Array(buf));
            } catch (_) {
              saveFailed = true;
              buffers.push(buf);
            }
          })());
          if (flushQueue.length > 4) {
            await Promise.all(flushQueue);
            flushQueue = [];
          }
        } else {
          buffers.push(buf);
        }
        chunkBuffers.push(buf);
      }
      if (flushQueue.length) {
        await Promise.all(flushQueue);
        flushQueue = [];
      }
      offset += manifest.chunks.length;
    } while (offset < totalChunks);

    setProgress(100);

    if (streamSave && writer && !saveFailed) {
      await writer.close();
      showSnack('Download saved');
    } else {
      // Reconstruct the file as a Blob and trigger the browser's Save dialog with
      // the correct filename (not the UUID used internally on Discord CDN).
      const blob    = new Blob(buffers, { type: lastMime });
      const blobUrl = URL.createObjectURL(blob);
      const a       = document.createElement('a');
      a.href        = blobUrl;
      a.download    = lastName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function() { URL.revokeObjectURL(blobUrl); }, BLOB_URL_REVOKE_DELAY_MS);
    }

    setTimeout(function() { card.remove(); }, 1500);
    if (progressContainer.children.length === 0) {
      progressContainer.classList.add('hidden');
    }
  } catch (err) {
    if (fillEl) fillEl.classList.add('failed');
    if (pctEl)  pctEl.textContent = 'Failed';
    showSnack('Download failed: ' + err.message, 'error');
    setTimeout(function() {
      card.remove();
      if (progressContainer.children.length === 0) {
        progressContainer.classList.add('hidden');
      }
    }, 4000);
  } finally {
    try { await writer?.abort(); } catch (_) { /* ignore */ }
  }
}

async function openViewer(file) {
  state.viewer = file;
  const dlg = document.getElementById('viewer-dialog');
  const title = document.getElementById('viewer-title');
  const subtitle = document.getElementById('viewer-subtitle');
  const content = document.getElementById('viewer-content');
  const saveBtn = document.getElementById('viewer-save-btn');
  const statusEl = document.getElementById('viewer-status');

  if (!dlg || !content || !title) {
    window.open(fileStreamUrl(file), '_blank');
    return;
  }

  title.textContent = file.name || 'File';
  if (subtitle) subtitle.textContent = file.size ? file.size : '';
  statusEl.textContent = '';
  if (saveBtn) saveBtn.classList.add('hidden');
  content.innerHTML = '<div class="viewer-loading">Loading preview…</div>';

  const url = fileStreamUrl(file);
  try {
    if (isAudioFile(file)) {
      content.innerHTML = `
        <div class="viewer-media">
          <audio controls autoplay src="${url}"></audio>
        </div>`;
    } else if (isVideoFile(file)) {
      content.innerHTML = `
        <div class="viewer-media">
          <video controls autoplay src="${url}" playsinline></video>
        </div>`;
    } else if (isImageFile(file)) {
      content.innerHTML = `
        <div class="viewer-media">
          <img src="${url}" alt="${escHtml(file.name)}" loading="lazy">
        </div>`;
    } else if (isTextFile(file)) {
      const res = await fetch(url);
      if (!res.ok) throw new Error('Failed to load file');
      const text = await res.text();
      content.innerHTML = `
        <div class="viewer-text-wrap">
          <textarea id="viewer-textarea" spellcheck="false"></textarea>
        </div>`;
      const ta = document.getElementById('viewer-textarea');
      if (ta) ta.value = text;
      if (saveBtn) saveBtn.classList.remove('hidden');
    } else {
      content.innerHTML = `
        <div class="viewer-fallback">
          <span class="material-symbols-outlined">cloud_download</span>
          <p>No preview available. Open in a new tab to download.</p>
        </div>`;
    }
    dlg.showModal();
  } catch (err) {
    content.innerHTML = `<div class="viewer-error">${escHtml(err.message || 'Unable to preview file')}</div>`;
    dlg.showModal();
  }
}

function closeViewer() {
  const dlg = document.getElementById('viewer-dialog');
  if (dlg?.open) dlg.close();
  state.viewer = null;
}

async function saveViewerEdits() {
  const file = state.viewer;
  if (!file) return;
  const ta = document.getElementById('viewer-textarea');
  const btn = document.getElementById('viewer-save-btn');
  const statusEl = document.getElementById('viewer-status');
  if (!ta || !btn) return;

  btn.disabled = true;
  statusEl.textContent = 'Saving…';
  try {
    await api.overwriteFile(state.directory.id || 'root', file.id, ta.value);
    statusEl.textContent = 'Saved';
    showSnack('Saved changes to ' + file.name);
    closeViewer();
    await navigateTo(state.directory.id);
  } catch (err) {
    statusEl.textContent = 'Save failed';
    if (err.status === 401) handleUnauthorized();
    else showSnack('Save failed: ' + err.message, 'error');
  } finally {
    btn.disabled = false;
  }
}

/* ══════════════════════════════════════════════════════════
   Auth
═══════════════════════════════════════════════════════════ */
function handleUnauthorized() {
  if (state.config.login) {
    openDialog('login-dialog');
  }
}

function updateAccountUI() {
  const nameEl = document.getElementById('account-name-display');
  const statusEl = document.getElementById('account-status-display');
  const loginItem = document.getElementById('account-login-item');
  const logoutItem = document.getElementById('account-logout-item');
  const railLabel = document.getElementById('account-label-rail');

  if (state.authenticated) {
    nameEl.textContent = 'Signed in';
    statusEl.textContent = 'Authenticated';
    loginItem.classList.add('hidden');
    logoutItem.classList.remove('hidden');
    if (railLabel) railLabel.textContent = 'Account';
  } else if (state.config.login) {
    nameEl.textContent = 'Not signed in';
    statusEl.textContent = state.config.anonymous ? 'Guest access' : 'Sign in required';
    loginItem.classList.remove('hidden');
    logoutItem.classList.add('hidden');
    if (railLabel) railLabel.textContent = 'Sign in';
  } else {
    nameEl.textContent = 'Open access';
    statusEl.textContent = 'No authentication configured';
    loginItem.classList.add('hidden');
    logoutItem.classList.add('hidden');
    if (railLabel) railLabel.textContent = 'Account';
  }
}

/* ══════════════════════════════════════════════════════════
   Uploads
═══════════════════════════════════════════════════════════ */
async function uploadFiles(files) {
  const dirId = state.directory.id || 'root';
  const progressContainer = document.getElementById('upload-progress');
  progressContainer.classList.remove('hidden');

  for (const file of files) {
    const cardId = 'upload-' + file.name.replace(/\W/g, '_') + '-' + Date.now();
    const card = document.createElement('div');
    card.className = 'upload-card';
    card.id = cardId;
    card.innerHTML = `
      <div class="upload-header">
        <div class="upload-filename">${escHtml(file.name)}</div>
        <div class="upload-pct">0%</div>
      </div>
      <div class="progress-track">
        <div class="progress-fill" style="width:0%"></div>
      </div>`;
    progressContainer.appendChild(card);

    try {
      await api.uploadFile(dirId, file, pct => {
        const pctEl = card.querySelector('.upload-pct');
        const fillEl = card.querySelector('.progress-fill');
        if (pctEl) pctEl.textContent = pct + '%';
        if (fillEl) fillEl.style.width = pct + '%';
      });
      card.remove();
      showSnack('Uploaded ' + file.name);
    } catch (err) {
      const fillEl = card.querySelector('.progress-fill');
      const pctEl = card.querySelector('.upload-pct');
      if (fillEl) fillEl.classList.add('failed');
      if (pctEl) pctEl.textContent = 'Failed';
      if (err.status === 401) handleUnauthorized();
      else showSnack('Upload failed: ' + err.message, 'error');
      setTimeout(() => card.remove(), 4000);
    }
  }

  if (progressContainer.children.length === 0) {
    progressContainer.classList.add('hidden');
  }
  await navigateTo(state.directory.id);
}

/* ══════════════════════════════════════════════════════════
   Dialogs
═══════════════════════════════════════════════════════════ */
function openDialog(id) {
  const dlg = document.getElementById(id);
  if (dlg && !dlg.open) {
    dlg.showModal();
    // Focus first input
    const input = dlg.querySelector('input');
    if (input) setTimeout(() => input.focus(), 50);
  }
}
function closeDialog(id) {
  const dlg = document.getElementById(id);
  if (dlg && dlg.open) dlg.close();
}

/* ══════════════════════════════════════════════════════════
   Theme
═══════════════════════════════════════════════════════════ */
function applyTheme(dark) {
  document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  localStorage.setItem('theme', dark ? 'dark' : 'light');

  const metaTheme = document.getElementById('meta-theme-color');
  if (metaTheme) metaTheme.content = dark ? '#1C1B1F' : '#6750A4';

  // Update all theme icons/labels
  ['theme-icon', 'theme-icon-rail', 'account-theme-icon'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.textContent = dark ? 'light_mode' : 'dark_mode';
  });
  ['theme-label-rail'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.textContent = dark ? 'Light' : 'Dark';
  });
  const themeText = document.getElementById('account-theme-text');
  if (themeText) themeText.textContent = dark ? 'Switch to Light mode' : 'Switch to Dark mode';
}

function toggleTheme() {
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  applyTheme(!isDark);
  closeDialog('account-menu');
}

/* ══════════════════════════════════════════════════════════
   Clipboard
═══════════════════════════════════════════════════════════ */
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(
      () => showSnack('Link copied to clipboard'),
      () => fallbackCopy(text)
    );
  } else {
    fallbackCopy(text);
  }
}
function fallbackCopy(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;top:0;left:0;opacity:0';
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  document.execCommand('copy');
  document.body.removeChild(ta);
  showSnack('Link copied to clipboard');
}

/* ══════════════════════════════════════════════════════════
   Sort Chips
═══════════════════════════════════════════════════════════ */
function updateSortChips() {
  document.querySelectorAll('#controls-bar .filter-chip').forEach(chip => {
    const sort = chip.getAttribute('data-sort');
    const isActive = sort === state.sortBy;
    chip.classList.toggle('active', isActive);
    const dirIcon = chip.querySelector('.sort-dir-icon');
    if (isActive) {
      if (!dirIcon) {
        const icon = document.createElement('span');
        icon.className = 'material-symbols-outlined sort-dir-icon';
        icon.style.fontSize = '14px';
        icon.textContent = state.sortDesc ? 'arrow_downward' : 'arrow_upward';
        chip.appendChild(icon);
      } else {
        dirIcon.textContent = state.sortDesc ? 'arrow_downward' : 'arrow_upward';
      }
    } else if (dirIcon) {
      dirIcon.remove();
    }
  });

  // Update sort menu checks
  document.querySelectorAll('[data-check]').forEach(el => {
    el.classList.toggle('hidden', el.getAttribute('data-check') !== state.sortBy);
  });
  document.querySelectorAll('[data-check-dir]').forEach(el => {
    const dir = el.getAttribute('data-check-dir');
    el.classList.toggle('hidden', (dir === 'asc' && state.sortDesc) || (dir === 'desc' && !state.sortDesc));
  });
}

/* ══════════════════════════════════════════════════════════
   App Init
═══════════════════════════════════════════════════════════ */
async function initApp() {
  // Apply saved theme
  const savedTheme = localStorage.getItem('theme');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  applyTheme(savedTheme === 'dark' || (!savedTheme && prefersDark));

  // Fetch config
  try {
    const configRes = await api.getConfig();
    state.config = configRes.data || {};
  } catch {
    state.config = { login: false, anonymous: true };
  }

  // Check token
  if (state.token) {
    try {
      await api.checkToken();
      state.authenticated = true;
    } catch {
      clearAuthToken();
      if (state.config.login && !state.config.anonymous) {
        openDialog('login-dialog');
      }
    }
  } else if (state.config.login && !state.config.anonymous) {
    openDialog('login-dialog');
  }

  updateAccountUI();

  // Apply view mode
  updateViewToggle();

  // Load root
  await navigateTo('root');

  // Register service worker
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  }
}

function updateViewToggle() {
  const icon = document.getElementById('view-icon');
  if (icon) icon.textContent = state.viewMode === 'grid' ? 'view_list' : 'grid_view';
}

/* ══════════════════════════════════════════════════════════
   Event Wiring
═══════════════════════════════════════════════════════════ */
document.addEventListener('DOMContentLoaded', () => {

  // ── Search ────────────────────────────────────────────
  const searchInput = document.getElementById('search-input');
  const mobileSearchInput = document.getElementById('mobile-search-input');
  const searchClearBtn = document.getElementById('search-clear-btn');
  const searchToggleBtn = document.getElementById('search-toggle-btn');
  const mobileSearchWrap = document.getElementById('mobile-search-wrap');

  function onSearchChange(val) {
    state.filterText = val;
    if (searchClearBtn) searchClearBtn.style.display = val ? '' : 'none';
    renderFiles();
  }

  searchInput?.addEventListener('input', e => {
    if (mobileSearchInput) mobileSearchInput.value = e.target.value;
    onSearchChange(e.target.value);
  });
  mobileSearchInput?.addEventListener('input', e => {
    if (searchInput) searchInput.value = e.target.value;
    onSearchChange(e.target.value);
  });
  searchClearBtn?.addEventListener('click', () => {
    if (searchInput) searchInput.value = '';
    if (mobileSearchInput) mobileSearchInput.value = '';
    onSearchChange('');
  });
  searchToggleBtn?.addEventListener('click', () => {
    if (mobileSearchWrap) {
      const hidden = mobileSearchWrap.classList.toggle('hidden');
      if (!hidden) {
        mobileSearchInput?.focus();
      }
    }
  });

  // ── Sort chips ────────────────────────────────────────
  document.querySelectorAll('#controls-bar .filter-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      const sort = chip.getAttribute('data-sort');
      if (state.sortBy === sort) {
        state.sortDesc = !state.sortDesc;
      } else {
        state.sortBy = sort;
        state.sortDesc = false;
      }
      updateSortChips();
      renderFiles();
    });
  });

  // ── Sort menu dialog ──────────────────────────────────
  document.getElementById('sort-btn')?.addEventListener('click', () => {
    updateSortChips();
    openDialog('sort-menu');
  });
  document.querySelectorAll('#sort-menu [data-sort]').forEach(item => {
    item.addEventListener('click', () => {
      state.sortBy = item.getAttribute('data-sort');
      state.sortDesc = false;
      updateSortChips();
      renderFiles();
      closeDialog('sort-menu');
    });
  });
  document.querySelectorAll('#sort-menu [data-dir]').forEach(item => {
    item.addEventListener('click', () => {
      state.sortDesc = item.getAttribute('data-dir') === 'desc';
      updateSortChips();
      renderFiles();
      closeDialog('sort-menu');
    });
  });

  // ── View toggle ───────────────────────────────────────
  document.getElementById('view-toggle-btn')?.addEventListener('click', () => {
    state.viewMode = state.viewMode === 'grid' ? 'list' : 'grid';
    localStorage.setItem('view_mode', state.viewMode);
    updateViewToggle();
    renderFiles();
  });

  // ── Theme ─────────────────────────────────────────────
  document.getElementById('theme-toggle-btn')?.addEventListener('click', toggleTheme);
  document.getElementById('nav-theme-rail')?.addEventListener('click', toggleTheme);
  document.getElementById('account-theme-item')?.addEventListener('click', toggleTheme);

  // ── Account ───────────────────────────────────────────
  document.getElementById('account-btn')?.addEventListener('click', () => {
    updateAccountUI();
    openDialog('account-menu');
  });
  document.getElementById('nav-account-rail')?.addEventListener('click', () => {
    updateAccountUI();
    openDialog('account-menu');
  });
  document.getElementById('bnav-account')?.addEventListener('click', () => {
    updateAccountUI();
    openDialog('account-menu');
  });
  document.getElementById('account-login-item')?.addEventListener('click', () => {
    closeDialog('account-menu');
    openDialog('login-dialog');
  });
  document.getElementById('account-logout-item')?.addEventListener('click', async () => {
    closeDialog('account-menu');
    api.logout();
    state.authenticated = false;
    state.selected.clear();
    updateAccountUI();
    showSnack('Signed out');
    await navigateTo('root');
  });

  // ── Login dialog ──────────────────────────────────────
  document.getElementById('login-submit-btn')?.addEventListener('click', async () => {
    const username = document.getElementById('login-username').value.trim();
    const password = document.getElementById('login-password').value;
    const errEl = document.getElementById('login-error');
    errEl.textContent = '';

    if (!username || !password) {
      errEl.textContent = 'Please enter username and password';
      return;
    }

    const btn = document.getElementById('login-submit-btn');
    btn.disabled = true;
    btn.textContent = 'Signing in…';

    try {
      await api.login(username, password);
      state.authenticated = true;
      updateAccountUI();
      closeDialog('login-dialog');
      document.getElementById('login-password').value = '';
      errEl.textContent = '';
      showSnack('Signed in successfully');
      await navigateTo('root');
    } catch (err) {
      errEl.textContent = err.message || 'Login failed';
    } finally {
      btn.disabled = false;
      btn.textContent = 'Sign in';
    }
  });

  document.getElementById('login-cancel-btn')?.addEventListener('click', () => {
    closeDialog('login-dialog');
  });

  // Allow Enter key in login form
  document.getElementById('login-form')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      document.getElementById('login-submit-btn')?.click();
    }
  });

  // ── Create folder ─────────────────────────────────────
  document.getElementById('create-folder-fab')?.addEventListener('click', () => {
    document.getElementById('folder-name-input').value = '';
    document.getElementById('cf-error').textContent = '';
    openDialog('create-folder-dialog');
  });
  document.getElementById('cf-cancel-btn')?.addEventListener('click', () => closeDialog('create-folder-dialog'));
  document.getElementById('cf-create-btn')?.addEventListener('click', async () => {
    const name = document.getElementById('folder-name-input').value.trim();
    const errEl = document.getElementById('cf-error');
    errEl.textContent = '';
    if (!name) { errEl.textContent = 'Please enter a folder name'; return; }

    const btn = document.getElementById('cf-create-btn');
    btn.disabled = true;
    try {
      await api.createDir(name, state.directory.id);
      closeDialog('create-folder-dialog');
      showSnack('Folder created: ' + name);
      await navigateTo(state.directory.id);
    } catch (err) {
      if (err.status === 401) { closeDialog('create-folder-dialog'); handleUnauthorized(); }
      else errEl.textContent = err.message || 'Failed to create folder';
    } finally {
      btn.disabled = false;
    }
  });
  document.getElementById('folder-name-input')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') document.getElementById('cf-create-btn')?.click();
  });

  // ── Upload ────────────────────────────────────────────
  const fileInput = document.getElementById('file-input');
  document.getElementById('upload-fab')?.addEventListener('click', () => fileInput.click());
  fileInput?.addEventListener('change', () => {
    if (fileInput.files && fileInput.files.length > 0) {
      uploadFiles(Array.from(fileInput.files));
      fileInput.value = '';
    }
  });

  // ── Drag & Drop ───────────────────────────────────────
  let dragCounter = 0;
  document.addEventListener('dragenter', e => {
    if (e.dataTransfer && e.dataTransfer.types.includes('Files')) {
      dragCounter++;
      document.getElementById('drop-overlay').classList.remove('hidden');
    }
  });
  document.addEventListener('dragleave', () => {
    dragCounter--;
    if (dragCounter <= 0) {
      dragCounter = 0;
      document.getElementById('drop-overlay').classList.add('hidden');
    }
  });
  document.addEventListener('dragover', e => e.preventDefault());
  document.addEventListener('drop', e => {
    e.preventDefault();
    dragCounter = 0;
    document.getElementById('drop-overlay').classList.add('hidden');
    const files = e.dataTransfer && e.dataTransfer.files;
    if (files && files.length > 0) uploadFiles(Array.from(files));
  });

  // ── Selection toolbar ─────────────────────────────────
  document.getElementById('clear-selection-btn')?.addEventListener('click', clearSelection);

  document.getElementById('copy-link-btn')?.addEventListener('click', () => {
    const f = state.directory.files.find(f => state.selected.has(f.id) && !f.dir);
    if (f) {
      const url = location.origin + '/files/' + f.id + '/' + encodeURIComponent(f.name);
      copyToClipboard(url);
      clearSelection();
    }
  });

  document.getElementById('rename-btn')?.addEventListener('click', () => {
    const f = state.directory.files.find(f => state.selected.has(f.id));
    if (!f) return;
    document.getElementById('rename-input').value = f.name;
    document.getElementById('rename-error').textContent = '';
    openDialog('rename-dialog');
  });
  document.getElementById('rename-cancel-btn')?.addEventListener('click', () => closeDialog('rename-dialog'));
  document.getElementById('rename-confirm-btn')?.addEventListener('click', async () => {
    const newName = document.getElementById('rename-input').value.trim();
    const errEl = document.getElementById('rename-error');
    errEl.textContent = '';
    if (!newName) { errEl.textContent = 'Please enter a name'; return; }

    const f = state.directory.files.find(f => state.selected.has(f.id));
    if (!f) return;

    const btn = document.getElementById('rename-confirm-btn');
    btn.disabled = true;
    try {
      if (f.dir) {
        await api.updateDir(f.id, { name: newName, parent: f.parent || state.directory.id });
      } else {
        await api.updateFile(state.directory.id, f.id, { name: newName, parent: f.parent || state.directory.id });
      }
      closeDialog('rename-dialog');
      clearSelection();
      showSnack('Renamed to ' + newName);
      await navigateTo(state.directory.id);
    } catch (err) {
      if (err.status === 401) { closeDialog('rename-dialog'); handleUnauthorized(); }
      else errEl.textContent = err.message || 'Rename failed';
    } finally {
      btn.disabled = false;
    }
  });
  document.getElementById('rename-input')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') document.getElementById('rename-confirm-btn')?.click();
  });

  document.getElementById('delete-btn')?.addEventListener('click', () => {
    const count = state.selected.size;
    document.getElementById('del-body').textContent =
      `Are you sure you want to delete ${count} item${count !== 1 ? 's' : ''}? This cannot be undone.`;
    openDialog('delete-dialog');
  });
  document.getElementById('del-cancel-btn')?.addEventListener('click', () => closeDialog('delete-dialog'));
  document.getElementById('del-confirm-btn')?.addEventListener('click', async () => {
    const ids = [...state.selected];
    closeDialog('delete-dialog');
    const btn = document.getElementById('del-confirm-btn');
    btn.disabled = true;
    let failed = 0;
    for (const id of ids) {
      try { await api.deleteItem(id); }
      catch (err) {
        if (err.status === 401) { handleUnauthorized(); break; }
        failed++;
      }
    }
    btn.disabled = false;
    clearSelection();
    if (failed > 0) showSnack(failed + ' item(s) failed to delete', 'error');
    else showSnack('Deleted ' + ids.length + ' item' + (ids.length !== 1 ? 's' : ''));
    await navigateTo(state.directory.id);
  });

  // ── Scroll shadow on top bar ──────────────────────────
  const mainContent = document.getElementById('main-content');
  mainContent?.addEventListener('scroll', () => {
    const bar = document.getElementById('top-app-bar');
    if (bar) bar.classList.toggle('scrolled', mainContent.scrollTop > 4);
  });

  // ── Close dialogs on backdrop click ──────────────────
  document.querySelectorAll('dialog').forEach(dlg => {
    dlg.addEventListener('click', e => {
      if (e.target === dlg) dlg.close();
    });
  });

  // ── Keyboard navigation ───────────────────────────────
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      clearSelection();
    }
    if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
      e.preventDefault();
      const files = filteredAndSorted();
      files.forEach(f => state.selected.add(f.id));
      document.querySelectorAll('.folder-card, .file-row').forEach(el => {
        const id = el.getAttribute('data-id');
        if (state.selected.has(id)) el.classList.add('selected');
      });
      updateSelectionToolbar();
    }
    if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
      e.preventDefault();
      const si = document.getElementById('search-input');
      si?.focus();
      si?.select();
    }
  });

  // ── Viewer ─────────────────────────────────────────────
  document.getElementById('viewer-close-btn')?.addEventListener('click', closeViewer);
  document.getElementById('viewer-cancel-btn')?.addEventListener('click', closeViewer);
  document.getElementById('viewer-save-btn')?.addEventListener('click', saveViewerEdits);
  document.getElementById('viewer-open-new-btn')?.addEventListener('click', () => {
    const f = state.viewer;
    if (f) window.open(fileStreamUrl(f), '_blank');
  });
  document.getElementById('viewer-download-btn')?.addEventListener('click', async () => {
    const f = state.viewer;
    if (f) await downloadFile(f);
  });

  // ── Init ──────────────────────────────────────────────
  initApp();
});
