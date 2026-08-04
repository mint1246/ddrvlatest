export interface ApiEnvelope<T> {
  message: string;
  data?: T;
}

export interface AuthConfig {
  login: boolean;
  anonymous: boolean;
}

export interface FileItem {
  id: string;
  name: string;
  dir: boolean;
  size: number;
  parent: string | null;
  mtime: string;
}

export interface Directory extends FileItem {
  files: FileItem[];
}

export interface Breadcrumb {
  id: string;
  name: string;
}

export type ThemeMode = 'system' | 'light' | 'dark';
export type Accent = 'lagoon' | 'violet' | 'sunset';
export type Complexity = 'calm' | 'balanced' | 'power';
export type Density = 'comfortable' | 'compact';
export type ViewMode = 'list' | 'grid';
export type SortKey = 'name' | 'size' | 'mtime';
export type ItemFilter = 'all' | 'folders' | 'documents' | 'media' | 'archives';

export interface AppSettings {
  theme: ThemeMode;
  accent: Accent;
  complexity: Complexity;
  density: Density;
  uploadConcurrency: 1 | 2 | 3 | 4;
  streamDownloads: boolean;
  reducedMotion: boolean;
}

export type TransferKind = 'upload' | 'download';
export type TransferStatus = 'queued' | 'active' | 'complete' | 'error' | 'cancelled';

export interface Transfer {
  id: string;
  name: string;
  kind: TransferKind;
  status: TransferStatus;
  progress: number;
  loaded?: number;
  total?: number;
  speed?: number;
  error?: string;
  startedAt: number;
  cancel?: () => void;
  retry?: () => void;
}

export interface ToastMessage {
  id: number;
  message: string;
  tone?: 'default' | 'success' | 'error';
  action?: { label: string; run: () => void };
}

