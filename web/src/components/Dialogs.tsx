import { useEffect, useRef, useState, type FormEvent } from 'react';
import { FolderPlus, LogIn, Pencil, Trash2 } from 'lucide-react';
import type { FileItem } from '../types';
import { validateName } from '../utils';
import { Button, Dialog, DialogHeader, Field } from './Primitives';

export function LoginDialog({ open, busy, error, onClose, onSubmit, required }: {
  open: boolean;
  busy: boolean;
  error: string;
  onClose: () => void;
  onSubmit: (username: string, password: string) => void;
  required: boolean;
}) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const usernameRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) window.setTimeout(() => usernameRef.current?.focus(), 80);
  }, [open]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (username.trim() && password) onSubmit(username.trim(), password);
  };

  return (
    <Dialog open={open} onClose={required ? () => undefined : onClose} labelledBy="login-title" className="auth-dialog">
      <form onSubmit={submit}>
        <DialogHeader
          icon={<LogIn size={23} />}
          title="Welcome back"
          description="Sign in to browse private files and make changes."
          onClose={required ? () => undefined : onClose}
          titleId="login-title"
        />
        <div className="dialog-body form-stack">
          <Field label="Username">
            <input ref={usernameRef} value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" required />
          </Field>
          <Field label="Password" error={error}>
            <input value={password} onChange={(event) => setPassword(event.target.value)} type="password" autoComplete="current-password" required />
          </Field>
        </div>
        <footer className="dialog-actions">
          {!required && <Button type="button" tone="text" onClick={onClose}>Not now</Button>}
          <Button type="submit" tone="filled" disabled={busy || !username.trim() || !password}>
            {busy ? 'Signing in…' : 'Sign in'}
          </Button>
        </footer>
      </form>
    </Dialog>
  );
}

export function NameDialog({ open, mode, item, busy, serverError, onClose, onSubmit }: {
  open: boolean;
  mode: 'create' | 'rename';
  item?: FileItem | null;
  busy: boolean;
  serverError: string;
  onClose: () => void;
  onSubmit: (name: string) => void;
}) {
  const [name, setName] = useState('');
  const [touched, setTouched] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const titleId = mode === 'create' ? 'create-folder-title' : 'rename-title';
  const validation = touched ? validateName(name) : '';

  useEffect(() => {
    if (!open) return;
    setName(mode === 'rename' ? item?.name ?? '' : '');
    setTouched(false);
    window.setTimeout(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
    }, 80);
  }, [open, mode, item]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setTouched(true);
    if (!validateName(name)) onSubmit(name.trim());
  };

  return (
    <Dialog open={open} onClose={onClose} labelledBy={titleId} className="small-dialog">
      <form onSubmit={submit}>
        <DialogHeader
          icon={mode === 'create' ? <FolderPlus size={23} /> : <Pencil size={22} />}
          title={mode === 'create' ? 'Create a folder' : `Rename ${item?.name ?? 'item'}`}
          description={mode === 'create' ? 'Give it a clear name—you can always change it later.' : 'The extension can be changed too, but that may affect previews.'}
          onClose={onClose}
          titleId={titleId}
        />
        <div className="dialog-body">
          <Field label={mode === 'create' ? 'Folder name' : 'New name'} error={validation || serverError} hint="Up to 255 characters">
            <input ref={inputRef} value={name} onChange={(event) => { setName(event.target.value); if (touched) setTouched(true); }} maxLength={255} />
          </Field>
        </div>
        <footer className="dialog-actions">
          <Button type="button" tone="text" onClick={onClose}>Cancel</Button>
          <Button type="submit" tone="filled" disabled={busy || Boolean(validateName(name)) || (mode === 'rename' && name.trim() === item?.name)}>
            {busy ? (mode === 'create' ? 'Creating…' : 'Renaming…') : (mode === 'create' ? 'Create folder' : 'Rename')}
          </Button>
        </footer>
      </form>
    </Dialog>
  );
}

export function DeleteDialog({ open, items, busy, error, onClose, onConfirm }: {
  open: boolean;
  items: FileItem[];
  busy: boolean;
  error: string;
  onClose: () => void;
  onConfirm: () => void;
}) {
  const folders = items.filter((item) => item.dir).length;
  const files = items.length - folders;
  const description = [folders ? `${folders} folder${folders === 1 ? '' : 's'}` : '', files ? `${files} file${files === 1 ? '' : 's'}` : ''].filter(Boolean).join(' and ');
  return (
    <Dialog open={open} onClose={onClose} labelledBy="delete-title" className="small-dialog destructive-dialog">
      <DialogHeader
        icon={<Trash2 size={23} />}
        title={`Delete ${items.length === 1 ? items[0]?.name ?? 'item' : `${items.length} items`}?`}
        description={`This will permanently remove ${description || 'the selected item'}. This action cannot be undone.`}
        onClose={onClose}
        titleId="delete-title"
      />
      {items.length > 1 && (
        <div className="delete-preview" aria-label="Items to delete">
          {items.slice(0, 4).map((item) => <span key={item.id}>{item.name}</span>)}
          {items.length > 4 && <span>and {items.length - 4} more…</span>}
        </div>
      )}
      {error && <p className="dialog-error" role="alert">{error}</p>}
      <footer className="dialog-actions">
        <Button tone="text" onClick={onClose}>Cancel</Button>
        <Button tone="danger" icon={<Trash2 size={18} />} onClick={onConfirm} disabled={busy}>
          {busy ? 'Deleting…' : 'Delete permanently'}
        </Button>
      </footer>
    </Dialog>
  );
}
