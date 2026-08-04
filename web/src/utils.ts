import type { FileItem, ItemFilter, SortKey } from './types';

export type FileKind = 'folder' | 'image' | 'video' | 'audio' | 'document' | 'code' | 'archive' | 'data' | 'pdf' | 'file';

const EXTENSIONS: Record<Exclude<FileKind, 'folder' | 'file'>, string[]> = {
  image: ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'ico', 'avif'],
  video: ['mp4', 'mkv', 'avi', 'mov', 'webm', 'm4v'],
  audio: ['mp3', 'ogg', 'wav', 'flac', 'aac', 'm4a', 'opus'],
  document: ['doc', 'docx', 'odt', 'rtf', 'txt', 'md'],
  code: ['js', 'jsx', 'ts', 'tsx', 'py', 'rs', 'go', 'java', 'c', 'cpp', 'h', 'html', 'css', 'scss', 'sh', 'yaml', 'yml', 'toml', 'log'],
  archive: ['zip', 'tar', 'gz', 'tgz', 'rar', '7z', 'bz2', 'xz'],
  data: ['csv', 'xls', 'xlsx', 'ods', 'json', 'xml', 'sql'],
  pdf: ['pdf'],
};

export function extensionOf(name: string) {
  const dot = name.lastIndexOf('.');
  return dot > -1 ? name.slice(dot + 1).toLowerCase() : '';
}

export function fileKind(item: FileItem): FileKind {
  if (item.dir) return 'folder';
  const extension = extensionOf(item.name);
  for (const [kind, values] of Object.entries(EXTENSIONS)) {
    if (values.includes(extension)) return kind as FileKind;
  }
  return 'file';
}

export function isTextFile(item: FileItem) {
  const extension = extensionOf(item.name);
  return ['txt', 'md', 'json', 'js', 'jsx', 'ts', 'tsx', 'py', 'rs', 'go', 'java', 'c', 'cpp', 'h', 'css', 'html', 'csv', 'log', 'yaml', 'yml', 'toml', 'xml', 'sql', 'sh'].includes(extension);
}

export function isPreviewable(item: FileItem) {
  return ['image', 'video', 'audio', 'pdf'].includes(fileKind(item)) || isTextFile(item);
}

export function fileUrl(item: FileItem) {
  return `/files/${encodeURIComponent(item.id)}/${encodeURIComponent(item.name)}`;
}

export function formatBytes(bytes: number, decimals = 1) {
  if (!Number.isFinite(bytes) || bytes <= 0) return bytes === 0 ? '0 B' : '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** index;
  return `${value.toFixed(index === 0 ? 0 : decimals)} ${units[index]}`;
}

export function formatRelativeDate(value: string | Date) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return 'Unknown';
  const seconds = Math.round((date.getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' });
  const ranges: Array<[number, Intl.RelativeTimeFormatUnit]> = [
    [60, 'second'], [60, 'minute'], [24, 'hour'], [7, 'day'], [4.345, 'week'], [12, 'month'], [Infinity, 'year'],
  ];
  let valueLeft = seconds;
  for (const [amount, unit] of ranges) {
    if (Math.abs(valueLeft) < amount) return formatter.format(Math.round(valueLeft), unit);
    valueLeft /= amount;
  }
  return date.toLocaleDateString();
}

export function formatFullDate(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Unknown' : new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(date);
}

export function formatDuration(seconds: number) {
  if (!Number.isFinite(seconds)) return '—';
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const rest = total % 60;
  return hours ? `${hours}:${String(minutes).padStart(2, '0')}:${String(rest).padStart(2, '0')}` : `${minutes}:${String(rest).padStart(2, '0')}`;
}

export function validateName(name: string) {
  const trimmed = name.trim();
  if (!trimmed) return 'Enter a name.';
  if (trimmed.length > 255) return 'Keep the name under 256 characters.';
  if (/[\/<>":|*\\]/.test(trimmed) || [...trimmed].some((char) => char.charCodeAt(0) < 32)) {
    return 'Names cannot contain /, \\, <, >, \", :, |, * or control characters.';
  }
  return '';
}

export function filterAndSort(
  items: FileItem[],
  query: string,
  filter: ItemFilter,
  sortKey: SortKey,
  descending: boolean,
) {
  const needle = query.trim().toLocaleLowerCase();
  const filtered = items.filter((item) => {
    if (needle && !item.name.toLocaleLowerCase().includes(needle)) return false;
    const kind = fileKind(item);
    if (filter === 'folders') return item.dir;
    if (filter === 'media') return ['image', 'video', 'audio'].includes(kind);
    if (filter === 'documents') return ['document', 'pdf', 'data', 'code'].includes(kind);
    if (filter === 'archives') return kind === 'archive';
    return true;
  });

  return [...filtered].sort((left, right) => {
    if (left.dir !== right.dir) return left.dir ? -1 : 1;
    let comparison = 0;
    if (sortKey === 'name') comparison = left.name.localeCompare(right.name, undefined, { numeric: true, sensitivity: 'base' });
    if (sortKey === 'size') comparison = left.size - right.size;
    if (sortKey === 'mtime') comparison = new Date(left.mtime).getTime() - new Date(right.mtime).getTime();
    return descending ? -comparison : comparison;
  });
}

export function summarizeItems(items: FileItem[]) {
  const folders = items.filter((item) => item.dir).length;
  const files = items.length - folders;
  const bytes = items.reduce((total, item) => total + (item.dir ? 0 : Math.max(0, item.size)), 0);
  return { folders, files, bytes };
}

