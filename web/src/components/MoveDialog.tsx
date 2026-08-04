import { useEffect, useState } from 'react';
import { Check, ChevronRight, Folder, FolderInput, LoaderCircle } from 'lucide-react';
import type { DdrvApi } from '../api';
import type { Breadcrumb, Directory, FileItem } from '../types';
import { Button, Dialog, DialogHeader } from './Primitives';

export function MoveDialog({ open, api, items, busy, error, onClose, onMove }: {
  open: boolean;
  api: DdrvApi;
  items: FileItem[];
  busy: boolean;
  error: string;
  onClose: () => void;
  onMove: (destination: string) => void;
}) {
  const [directory, setDirectory] = useState<Directory | null>(null);
  const [trail, setTrail] = useState<Breadcrumb[]>([{ id: 'root', name: 'My drive' }]);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState('');
  const selectedIds = new Set(items.map((item) => item.id));
  const sameLocation = items.length > 0 && items.every((item) => (item.parent || 'root') === directory?.id);

  const load = async (id: string, knownTrail?: Breadcrumb[]) => {
    setLoading(true);
    setLoadError('');
    try {
      const result = await api.getDirectory(id);
      setDirectory(result);
      setTrail(knownTrail ?? await api.breadcrumbs(id));
    } catch (reason) {
      setLoadError(reason instanceof Error ? reason.message : 'Could not open this folder.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (open) void load('root', [{ id: 'root', name: 'My drive' }]);
  }, [open]);

  return (
    <Dialog open={open} onClose={onClose} labelledBy="move-title" className="move-dialog">
      <DialogHeader
        icon={<FolderInput size={23} />}
        title="Choose a destination"
        description="Open a folder, then move the selected items into it."
        onClose={onClose}
        titleId="move-title"
      />
      <nav className="move-breadcrumbs" aria-label="Destination path">
        {trail.map((crumb, index) => (
          <span key={crumb.id}>
            {index > 0 && <ChevronRight size={14} />}
            <button disabled={index === trail.length - 1} onClick={() => void load(crumb.id, trail.slice(0, index + 1))}>{crumb.name}</button>
          </span>
        ))}
      </nav>
      <div className="folder-picker" aria-busy={loading}>
        {loading && <div className="picker-message"><LoaderCircle className="spin" size={24} />Opening folder…</div>}
        {!loading && (loadError || error) && <div className="picker-message error">{loadError || error}</div>}
        {!loading && !loadError && directory?.files.filter((item) => item.dir && !selectedIds.has(item.id)).length === 0 && (
          <div className="picker-message"><Folder size={26} />No subfolders here</div>
        )}
        {!loading && !loadError && directory?.files.filter((item) => item.dir && !selectedIds.has(item.id)).map((folder) => (
          <button className="folder-choice" key={folder.id} onClick={() => void load(folder.id, [...trail, { id: folder.id, name: folder.name }])}>
            <span><Folder size={20} fill="currentColor" fillOpacity={0.12} /></span>
            <strong>{folder.name}</strong>
            <ChevronRight size={18} />
          </button>
        ))}
      </div>
      <footer className="dialog-actions move-actions">
        <span className="move-destination"><Check size={15} />Destination: <strong>{directory?.id === 'root' ? 'My drive' : directory?.name || '—'}</strong></span>
        <Button tone="text" onClick={onClose}>Cancel</Button>
        <Button tone="filled" disabled={busy || loading || !directory || sameLocation} onClick={() => directory && onMove(directory.id)}>{busy ? 'Moving…' : sameLocation ? 'Already here' : 'Move here'}</Button>
      </footer>
    </Dialog>
  );
}
