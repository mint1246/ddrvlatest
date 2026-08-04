import type { AppSettings, SortKey, ViewMode } from './types';

const SETTINGS_KEY = 'ddrv:settings:v3';
const TOKEN_KEY = 'auth_token';

export const defaultSettings: AppSettings = {
  theme: 'system',
  accent: 'lagoon',
  complexity: 'balanced',
  density: 'comfortable',
  uploadConcurrency: 2,
  streamDownloads: true,
  reducedMotion: false,
};

export function loadSettings(): AppSettings {
  try {
    const parsed = JSON.parse(localStorage.getItem(SETTINGS_KEY) || '{}') as Partial<AppSettings>;
    return { ...defaultSettings, ...parsed };
  } catch {
    return defaultSettings;
  }
}

export function saveSettings(settings: AppSettings) {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
}

function readTokenCookie() {
  const entry = document.cookie
    .split(';')
    .map((value) => value.trim())
    .find((value) => value.startsWith('ddrv_token='));
  return entry ? decodeURIComponent(entry.slice('ddrv_token='.length)) : null;
}

export function loadToken() {
  return localStorage.getItem(TOKEN_KEY) || readTokenCookie();
}

export function storeToken(token: string | null) {
  const secure = location.protocol === 'https:' ? 'Secure; ' : '';
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
    document.cookie = `ddrv_token=${encodeURIComponent(token)}; Max-Age=2592000; Path=/; ${secure}SameSite=Lax`;
  } else {
    localStorage.removeItem(TOKEN_KEY);
    document.cookie = `ddrv_token=; Max-Age=0; Path=/; ${secure}SameSite=Lax`;
  }
}

export function loadViewMode(): ViewMode {
  return localStorage.getItem('view_mode') === 'grid' ? 'grid' : 'list';
}

export function loadSort(): { key: SortKey; descending: boolean } {
  const raw = localStorage.getItem('sort_by');
  const key: SortKey = raw === 'size' || raw === 'mtime' ? raw : 'name';
  return { key, descending: localStorage.getItem('sort_desc') === '1' };
}

