import type { FileItem } from './types';
import { fileUrl } from './utils';

interface ManifestChunk {
  url?: string;
  download_url?: string;
  size?: number;
}

interface Manifest {
  total_chunks: number;
  chunks: ManifestChunk[];
  name?: string;
  mime?: string;
  size?: number;
}

declare global {
  interface Window {
    showSaveFilePicker?: (options: { suggestedName?: string }) => Promise<{
      createWritable: () => Promise<{
        write: (data: Uint8Array) => Promise<void>;
        close: () => Promise<void>;
        abort: () => Promise<void>;
      }>;
    }>;
  }
}

const MANIFEST_BATCH_SIZE = 5;
const RENEW_ATTEMPTS = 12;
const RENEW_DELAY = 250;
const LARGE_BLOB_LIMIT = 256 * 1024 * 1024;

const wait = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function candidates(chunk: ManifestChunk) {
  // The server's download_url is deliberately authoritative. When it points
  // at the configured CDN proxy, falling back to chunk.url would bypass the
  // proxy and trigger Discord's CORS policy in the browser.
  return chunk.download_url ? [chunk.download_url] : chunk.url ? [chunk.url] : [];
}

async function fetchChunk(chunk: ManifestChunk, signal: AbortSignal) {
  let lastError: Error | null = null;
  let notFound = false;
  for (const url of candidates(chunk)) {
    try {
      const response = await fetch(url, { mode: 'cors', referrerPolicy: 'no-referrer', signal });
      if (!response.ok) {
        notFound ||= response.status === 404;
        throw new Error(`Chunk request failed (${response.status})`);
      }
      const buffer = await response.arrayBuffer();
      if (typeof chunk.size === 'number' && buffer.byteLength !== chunk.size) {
        throw new Error(
          `Chunk integrity check failed (expected ${chunk.size.toLocaleString()} bytes, received ${buffer.byteLength.toLocaleString()}).`,
        );
      }
      return buffer;
    } catch (error) {
      if (signal.aborted) throw error;
      lastError = error instanceof Error ? error : new Error('Chunk request failed');
    }
  }
  if (notFound && lastError) Object.assign(lastError, { notFound: true });
  throw lastError ?? new Error('No valid download URL was available.');
}

async function renewChunk(fileId: string, index: number, signal: AbortSignal) {
  const response = await fetch(`/files/${encodeURIComponent(fileId)}/manifest?offset=${index}&limit=1`, { signal });
  if (!response.ok) throw new Error(`Could not renew the download link (${response.status})`);
  const manifest = await response.json() as Manifest;
  if (!manifest.chunks?.[0]) throw new Error('The renewed manifest contained no chunks.');
  return manifest.chunks[0];
}

async function fetchWithRenew(fileId: string, initial: ManifestChunk, index: number, signal: AbortSignal) {
  let chunk = initial;
  for (let attempt = 0; attempt <= RENEW_ATTEMPTS; attempt += 1) {
    try {
      return await fetchChunk(chunk, signal);
    } catch (error) {
      if (signal.aborted || !(error instanceof Error && 'notFound' in error) || attempt === RENEW_ATTEMPTS) throw error;
      chunk = await renewChunk(fileId, index, signal);
      await wait(RENEW_DELAY);
    }
  }
  throw new Error('Download links could not be renewed.');
}

function browserDownload(url: string, name: string) {
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
}

export async function downloadFile(
  file: FileItem,
  options: {
    signal: AbortSignal;
    preferDiskStream: boolean;
    onProgress: (loaded: number, total: number) => void;
  },
) : Promise<{ verified: boolean }> {
  if (file.dir) return { verified: false };

  if (!window.showSaveFilePicker && file.size > LARGE_BLOB_LIMIT) {
    browserDownload(fileUrl(file), file.name);
    options.onProgress(file.size, file.size);
    return { verified: false };
  }

  let writer: Awaited<ReturnType<Awaited<ReturnType<NonNullable<typeof window.showSaveFilePicker>>>['createWritable']>> | null = null;
  const buffers: ArrayBuffer[] = [];
  let streamed = false;

  if (options.preferDiskStream && window.showSaveFilePicker) {
    const handle = await window.showSaveFilePicker({ suggestedName: file.name });
    writer = await handle.createWritable();
    streamed = true;
  }

  let offset = 0;
  let totalChunks: number | null = null;
  let loaded = 0;
  let total = Math.max(0, file.size);
  let filename = file.name;
  let mime = 'application/octet-stream';

  try {
    do {
      const response = await fetch(`/files/${encodeURIComponent(file.id)}/manifest?offset=${offset}&limit=${MANIFEST_BATCH_SIZE}`, { signal: options.signal });
      if (!response.ok) throw new Error(`Manifest request failed (${response.status})`);
      const manifest = await response.json() as Manifest;
      if (totalChunks === null) {
        totalChunks = manifest.total_chunks;
        total = manifest.size ?? total;
        filename = manifest.name || filename;
        mime = manifest.mime || mime;
      }
      if (!manifest.chunks.length && offset < (totalChunks ?? 0)) throw new Error('The download manifest ended unexpectedly.');

      const batch = await Promise.all(manifest.chunks.map(async (chunk, batchIndex) => {
        const buffer = await fetchWithRenew(file.id, chunk, offset + batchIndex, options.signal);
        loaded += buffer.byteLength;
        options.onProgress(loaded, total || loaded);
        return buffer;
      }));

      for (const buffer of batch) {
        if (writer) await writer.write(new Uint8Array(buffer));
        else buffers.push(buffer);
      }
      offset += manifest.chunks.length;
    } while (offset < (totalChunks ?? 0));

    if (loaded !== total) {
      throw new Error(
        `Download integrity check failed (expected ${total.toLocaleString()} bytes, received ${loaded.toLocaleString()}).`,
      );
    }

    if (writer) {
      await writer.close();
      writer = null;
    } else {
      const blobUrl = URL.createObjectURL(new Blob(buffers, { type: mime }));
      browserDownload(blobUrl, filename);
      window.setTimeout(() => URL.revokeObjectURL(blobUrl), 30_000);
    }
    options.onProgress(total || loaded, total || loaded);
    return { verified: true };
  } catch (error) {
    if (writer) await writer.abort().catch(() => undefined);
    if (streamed && error instanceof DOMException && error.name === 'AbortError') throw error;
    throw error;
  }
}
