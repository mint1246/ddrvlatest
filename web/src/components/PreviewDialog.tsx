import { useEffect, useRef, useState } from 'react';
import { Download, ExternalLink, FileQuestion, LoaderCircle, Save, TriangleAlert, X } from 'lucide-react';
import type { FileItem } from '../types';
import { fileKind, fileUrl, formatBytes, isTextFile } from '../utils';
import { Button, Dialog, IconButton } from './Primitives';
import { FileGlyph } from './FileGlyph';

export function PreviewDialog({ open, item, canWrite, onClose, onDownload, onSave }: {
  open: boolean;
  item: FileItem | null;
  canWrite: boolean;
  onClose: () => void;
  onDownload: (item: FileItem) => void;
  onSave: (item: FileItem, content: string) => Promise<void>;
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [text, setText] = useState('');
  const [initialText, setInitialText] = useState('');
  const [saving, setSaving] = useState(false);
  const editorRef = useRef<HTMLTextAreaElement>(null);
  const kind = item ? fileKind(item) : 'file';
  const dirty = isTextFile(item ?? emptyItem) && text !== initialText;

  useEffect(() => {
    if (!open || !item || !isTextFile(item)) return;
    const controller = new AbortController();
    setLoading(true);
    setError('');
    setText('');
    setInitialText('');
    fetch(fileUrl(item), { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Preview request failed (${response.status})`);
        return response.text();
      })
      .then((content) => { setText(content); setInitialText(content); })
      .catch((reason) => { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : 'Could not load this file.'); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [open, item]);

  useEffect(() => {
    if (!open) return;
    const shortcut = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's' && item && isTextFile(item) && canWrite) {
        event.preventDefault();
        void save();
      }
    };
    document.addEventListener('keydown', shortcut);
    return () => document.removeEventListener('keydown', shortcut);
  });

  if (!item) return null;
  const url = fileUrl(item);

  const save = async () => {
    if (!dirty || saving) return;
    setSaving(true);
    setError('');
    try {
      await onSave(item, text);
      setInitialText(text);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not save this file.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} labelledBy="preview-title" className="preview-dialog">
      <header className="preview-header">
        <FileGlyph item={item} size={22} />
        <div>
          <h2 id="preview-title">{item.name}</h2>
          <span>{formatBytes(item.size)}{dirty ? ' · Unsaved changes' : ''}</span>
        </div>
        <div className="preview-actions">
          <IconButton label="Open in a new tab" onClick={() => window.open(url, '_blank', 'noopener,noreferrer')}><ExternalLink size={19} /></IconButton>
          <IconButton label="Download" onClick={() => onDownload(item)}><Download size={19} /></IconButton>
          <IconButton label="Close preview" onClick={onClose}><X size={20} /></IconButton>
        </div>
      </header>

      <div className={`preview-stage preview-${kind}`}>
        {kind === 'image' && <img src={url} alt={item.name} />}
        {kind === 'video' && <video src={url} controls autoPlay playsInline />}
        {kind === 'audio' && (
          <div className="audio-preview">
            <div className="audio-art"><FileGlyph item={item} size={58} /></div>
            <strong>{item.name}</strong>
            <audio src={url} controls autoPlay />
          </div>
        )}
        {kind === 'pdf' && <iframe src={url} title={`Preview of ${item.name}`} />}
        {isTextFile(item) && loading && <div className="preview-message"><LoaderCircle className="spin" size={28} /><span>Loading text preview…</span></div>}
        {isTextFile(item) && !loading && !error && (
          <textarea
            ref={editorRef}
            className="text-editor"
            value={text}
            readOnly={!canWrite}
            onChange={(event) => setText(event.target.value)}
            spellCheck={false}
            aria-label={`Contents of ${item.name}`}
          />
        )}
        {!['image', 'video', 'audio', 'pdf'].includes(kind) && !isTextFile(item) && (
          <div className="preview-message"><FileQuestion size={34} /><strong>No inline preview</strong><span>Download or open this file in another app.</span></div>
        )}
        {error && <div className="preview-message error" role="alert"><TriangleAlert size={29} /><strong>Preview unavailable</strong><span>{error}</span></div>}
      </div>

      <footer className="preview-footer">
        <span>{isTextFile(item) && !canWrite ? 'Guest access · read only' : isTextFile(item) ? 'Tip: Ctrl+S saves changes' : `${kind.charAt(0).toUpperCase()}${kind.slice(1)} preview`}</span>
        <div>
          <Button tone="text" onClick={onClose}>Close</Button>
          {isTextFile(item) && canWrite && (
            <Button tone="filled" icon={<Save size={17} />} disabled={!dirty || saving} onClick={() => void save()}>{saving ? 'Saving…' : 'Save changes'}</Button>
          )}
        </div>
      </footer>
    </Dialog>
  );
}

const emptyItem: FileItem = { id: '', name: '', dir: false, size: 0, parent: null, mtime: '' };
