import { describe, expect, it } from 'vitest';
import type { FileItem } from './types';
import { fileKind, filterAndSort, formatBytes, validateName } from './utils';

const item = (name: string, options: Partial<FileItem> = {}): FileItem => ({
  id: name,
  name,
  dir: false,
  size: 100,
  parent: 'root',
  mtime: '2026-01-01T00:00:00Z',
  ...options,
});

describe('file utilities', () => {
  it('classifies common file families', () => {
    expect(fileKind(item('photo.AVIF'))).toBe('image');
    expect(fileKind(item('notes.md'))).toBe('document');
    expect(fileKind(item('bundle.tar'))).toBe('archive');
    expect(fileKind(item('Projects', { dir: true }))).toBe('folder');
  });

  it('keeps folders first while sorting naturally', () => {
    const values = [item('file10.txt'), item('Folder', { dir: true }), item('file2.txt')];
    expect(filterAndSort(values, '', 'all', 'name', false).map((value) => value.name)).toEqual(['Folder', 'file2.txt', 'file10.txt']);
  });

  it('filters by query and category', () => {
    const values = [item('sunset.jpg'), item('song.mp3'), item('brief.pdf')];
    expect(filterAndSort(values, 'sun', 'media', 'name', false).map((value) => value.name)).toEqual(['sunset.jpg']);
    expect(filterAndSort(values, '', 'documents', 'name', false).map((value) => value.name)).toEqual(['brief.pdf']);
  });

  it('validates names using the server constraints', () => {
    expect(validateName('')).toBeTruthy();
    expect(validateName('bad/name')).toBeTruthy();
    expect(validateName('Quarterly report.pdf')).toBe('');
  });

  it('formats byte counts compactly', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(1536)).toBe('1.5 KB');
  });
});
