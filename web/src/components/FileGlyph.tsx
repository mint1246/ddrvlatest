import {
  Archive,
  Braces,
  File,
  FileAudio,
  FileChartColumn,
  FileImage,
  FileText,
  FileVideo,
  Folder,
} from 'lucide-react';
import type { FileItem } from '../types';
import { fileKind } from '../utils';

export function FileGlyph({ item, size = 22 }: { item: FileItem; size?: number }) {
  const kind = fileKind(item);
  const props = { size, strokeWidth: 1.8 };
  const glyph = (() => {
    if (kind === 'folder') return <Folder {...props} fill="currentColor" fillOpacity={0.13} />;
    if (kind === 'image') return <FileImage {...props} />;
    if (kind === 'video') return <FileVideo {...props} />;
    if (kind === 'audio') return <FileAudio {...props} />;
    if (kind === 'archive') return <Archive {...props} />;
    if (kind === 'code') return <Braces {...props} />;
    if (kind === 'data') return <FileChartColumn {...props} />;
    if (kind === 'document' || kind === 'pdf') return <FileText {...props} />;
    return <File {...props} />;
  })();
  return <span className={`file-glyph kind-${kind}`} aria-hidden="true">{glyph}</span>;
}
