import { useEffect } from 'react';
import { CalendarClock, Copy, Download, FileKey, FolderTree, Maximize2, Pencil, X } from 'lucide-react';
import type { Breadcrumb, FileItem } from '../types';
import { fileKind, formatBytes, formatFullDate } from '../utils';
import { Button, IconButton } from './Primitives';
import { FileGlyph } from './FileGlyph';

export function DetailsPanel({ open, item, breadcrumbs, canWrite, onClose, onOpen, onDownload, onCopy, onRename }: {
  open: boolean;
  item: FileItem | null;
  breadcrumbs: Breadcrumb[];
  canWrite: boolean;
  onClose: () => void;
  onOpen: () => void;
  onDownload: () => void;
  onCopy: () => void;
  onRename: () => void;
}) {
  useEffect(() => {
    if (!open) return;
    const close = (event: KeyboardEvent) => { if (event.key === 'Escape') onClose(); };
    document.addEventListener('keydown', close);
    return () => document.removeEventListener('keydown', close);
  }, [open, onClose]);
  if (!item) return null;
  return (
    <div className={`sheet-layer details-layer${open ? ' open' : ''}`} aria-hidden={!open}>
      <button className="sheet-scrim" aria-label="Close details" onClick={onClose} tabIndex={open ? 0 : -1} />
      <aside className="details-sheet" role="dialog" aria-modal="true" aria-labelledby="details-title">
        <header className="sheet-header">
          <div className="sheet-icon"><FileGlyph item={item} size={21} /></div>
          <div><h2 id="details-title">Details</h2></div>
          <IconButton label="Close details" onClick={onClose}><X size={20} /></IconButton>
        </header>
        <div className="sheet-scroll details-body">
          <div className={`details-hero kind-${fileKind(item)}`}><FileGlyph item={item} size={54} /></div>
          <div className="details-name"><h3>{item.name}</h3><span>{item.dir ? 'Folder' : fileKind(item)}</span></div>
          <div className="details-actions">
            <Button tone="filled" icon={<Maximize2 size={17} />} onClick={onOpen}>{item.dir ? 'Open folder' : 'Open'}</Button>
            {!item.dir && <IconButton label="Download" onClick={onDownload}><Download size={19} /></IconButton>}
            {!item.dir && <IconButton label="Copy link" onClick={onCopy}><Copy size={19} /></IconButton>}
            {canWrite && <IconButton label="Rename" onClick={onRename}><Pencil size={19} /></IconButton>}
          </div>
          <dl className="details-list">
            <div><dt><FileKey size={17} />Type</dt><dd>{item.dir ? 'Folder' : `${fileKind(item)} file`}</dd></div>
            {!item.dir && <div><dt><Maximize2 size={17} />Size</dt><dd>{formatBytes(item.size)}<small>{item.size.toLocaleString()} bytes</small></dd></div>}
            <div><dt><CalendarClock size={17} />Modified</dt><dd>{formatFullDate(item.mtime)}</dd></div>
            <div><dt><FolderTree size={17} />Location</dt><dd>{breadcrumbs.map((crumb) => crumb.name).join(' / ')}</dd></div>
          </dl>
          <div className="details-id"><span>Item ID</span><code>{item.id}</code></div>
        </div>
      </aside>
    </div>
  );
}
