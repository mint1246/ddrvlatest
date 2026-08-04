import type { ApiEnvelope, AuthConfig, Breadcrumb, Directory, FileItem } from './types';

export class ApiError extends Error {
  constructor(message: string, public status: number) {
    super(message);
    this.name = 'ApiError';
  }
}

async function parseResponse(response: Response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return { message: text };
  }
}

export class DdrvApi {
  constructor(private getToken: () => string | null) {}

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    const token = this.getToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);
    if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }
    const response = await fetch(path, { ...init, headers });
    const body = await parseResponse(response) as { message?: string };
    if (!response.ok) throw new ApiError(body.message || response.statusText || 'Request failed', response.status);
    return body as T;
  }

  async config() {
    const response = await this.request<ApiEnvelope<AuthConfig>>('/api/config');
    return response.data ?? { login: false, anonymous: true };
  }

  async checkToken() {
    await this.request('/api/check_token');
  }

  async login(username: string, password: string) {
    const response = await this.request<ApiEnvelope<{ token: string }>>('/api/user/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    if (!response.data?.token) throw new ApiError('The server returned an invalid sign-in response.', 500);
    return response.data.token;
  }

  async getDirectory(id = 'root') {
    const response = await this.request<ApiEnvelope<Directory>>(`/api/directories/${encodeURIComponent(id)}`);
    if (!response.data) throw new ApiError('The directory response was empty.', 500);
    return { ...response.data, files: response.data.files ?? [] };
  }

  async breadcrumbs(id: string): Promise<Breadcrumb[]> {
    if (id === 'root') return [{ id: 'root', name: 'My drive' }];
    const trail: Breadcrumb[] = [];
    const seen = new Set<string>();
    let cursor: string | null = id;
    for (let depth = 0; cursor && cursor !== 'root' && depth < 32; depth += 1) {
      if (seen.has(cursor)) break;
      seen.add(cursor);
      const directory: Directory = await this.getDirectory(cursor);
      trail.unshift({ id: directory.id, name: directory.name });
      cursor = directory.parent;
    }
    return [{ id: 'root', name: 'My drive' }, ...trail];
  }

  async createDirectory(name: string, parent: string) {
    return this.request<ApiEnvelope<FileItem>>('/api/directories/', {
      method: 'POST',
      body: JSON.stringify({ name, parent }),
    });
  }

  async updateItem(item: FileItem, parentId: string, changes: { name?: string; parent?: string }) {
    const path = item.dir
      ? `/api/directories/${encodeURIComponent(item.id)}`
      : `/api/directories/${encodeURIComponent(parentId)}/files/${encodeURIComponent(item.id)}`;
    return this.request<ApiEnvelope<FileItem>>(path, { method: 'PUT', body: JSON.stringify(changes) });
  }

  async deleteItem(item: FileItem, parentId: string) {
    const path = item.dir
      ? `/api/directories/${encodeURIComponent(item.id)}`
      : `/api/directories/${encodeURIComponent(parentId)}/files/${encodeURIComponent(item.id)}`;
    await this.request(path, { method: 'DELETE' });
  }

  async overwriteFile(item: FileItem, parentId: string, content: string) {
    const token = this.getToken();
    const headers = new Headers({ 'Content-Type': 'text/plain;charset=utf-8' });
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const response = await fetch(`/api/directories/${encodeURIComponent(parentId)}/files/${encodeURIComponent(item.id)}/content`, {
      method: 'PUT', headers, body: content,
    });
    const body = await parseResponse(response) as { message?: string };
    if (!response.ok) throw new ApiError(body.message || response.statusText || 'Save failed', response.status);
  }

  uploadFile(directoryId: string, file: File, onProgress: (loaded: number, total: number) => void) {
    const xhr = new XMLHttpRequest();
    const promise = new Promise<void>((resolve, reject) => {
      const data = new FormData();
      data.append('file', file);
      xhr.open('POST', `/api/directories/${encodeURIComponent(directoryId)}/files`);
      const token = this.getToken();
      if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`);
      xhr.upload.onprogress = (event) => onProgress(event.loaded, event.lengthComputable ? event.total : file.size);
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else {
          let message = xhr.statusText || 'Upload failed';
          try { message = JSON.parse(xhr.responseText)?.message || message; } catch { /* response was not JSON */ }
          reject(new ApiError(message, xhr.status));
        }
      };
      xhr.onerror = () => reject(new ApiError('Network error while uploading.', 0));
      xhr.onabort = () => reject(new DOMException('Upload cancelled', 'AbortError'));
      xhr.send(data);
    });
    return { promise, cancel: () => xhr.abort() };
  }
}

