import { useEffect } from 'react';
import { ArrowDownToLine, ArrowUpFromLine, Check, CircleAlert, RotateCcw, Trash2, X } from 'lucide-react';
import type { Transfer } from '../types';
import { formatBytes, formatDuration } from '../utils';
import { Button, IconButton, ProgressRing } from './Primitives';

export function TransferCenter({ open, transfers, onClose, onClear, onRemove }: {
  open: boolean;
  transfers: Transfer[];
  onClose: () => void;
  onClear: () => void;
  onRemove: (id: string) => void;
}) {
  useEffect(() => {
    if (!open) return;
    const close = (event: KeyboardEvent) => { if (event.key === 'Escape') onClose(); };
    document.addEventListener('keydown', close);
    return () => document.removeEventListener('keydown', close);
  }, [open, onClose]);

  const active = transfers.filter((transfer) => transfer.status === 'active' || transfer.status === 'queued');
  const completed = transfers.filter((transfer) => !active.includes(transfer));

  return (
    <div className={`sheet-layer${open ? ' open' : ''}`} aria-hidden={!open}>
      <button className="sheet-scrim" aria-label="Close transfers" onClick={onClose} tabIndex={open ? 0 : -1} />
      <aside className="transfer-sheet" role="dialog" aria-modal="true" aria-labelledby="transfers-title">
        <header className="sheet-header">
          <div className="sheet-icon transfer"><ArrowUpFromLine size={22} /></div>
          <div><h2 id="transfers-title">Transfers</h2></div>
          <IconButton label="Close transfers" onClick={onClose}><X size={20} /></IconButton>
        </header>
        <div className="transfer-summary">
          <strong>{active.length ? `${active.length} in progress` : 'All caught up'}</strong>
          <span>{active.length ? 'You can keep working while these finish.' : completed.length ? `${completed.length} recent transfer${completed.length === 1 ? '' : 's'}` : 'Uploads and downloads will appear here.'}</span>
        </div>
        <div className="sheet-scroll transfer-list">
          {transfers.length === 0 && (
            <div className="transfers-empty"><span><ArrowDownToLine size={32} /></span><strong>No transfer activity</strong><p>Start an upload or download and track it here.</p></div>
          )}
          {active.length > 0 && <span className="transfer-group-label">In progress</span>}
          {active.map((transfer) => <TransferRow key={transfer.id} transfer={transfer} onRemove={onRemove} />)}
          {completed.length > 0 && <span className="transfer-group-label">Recent</span>}
          {completed.map((transfer) => <TransferRow key={transfer.id} transfer={transfer} onRemove={onRemove} />)}
        </div>
        {completed.length > 0 && (
          <footer className="sheet-footer"><Button tone="text" icon={<Trash2 size={17} />} onClick={onClear}>Clear finished</Button></footer>
        )}
      </aside>
    </div>
  );
}

function TransferRow({ transfer, onRemove }: { transfer: Transfer; onRemove: (id: string) => void }) {
  const elapsed = Math.max(0.1, (Date.now() - transfer.startedAt) / 1000);
  const speed = transfer.speed ?? (transfer.loaded ? transfer.loaded / elapsed : 0);
  const remaining = transfer.total && speed ? Math.max(0, transfer.total - (transfer.loaded ?? 0)) / speed : Infinity;
  return (
    <article className={`transfer-row status-${transfer.status}`}>
      <div className="transfer-state" aria-hidden="true">
        {transfer.status === 'active' && <ProgressRing value={transfer.progress} />}
        {transfer.status === 'queued' && <span className="queued-dot">···</span>}
        {transfer.status === 'complete' && <Check size={19} />}
        {transfer.status === 'error' && <CircleAlert size={19} />}
        {transfer.status === 'cancelled' && <X size={19} />}
      </div>
      <div className="transfer-copy">
        <strong title={transfer.name}>{transfer.name}</strong>
        <span>
          {transfer.kind === 'upload' ? <ArrowUpFromLine size={13} /> : <ArrowDownToLine size={13} />}
          {transfer.status === 'active' && `${formatBytes(speed)}/s · ${formatDuration(remaining)} left`}
          {transfer.status === 'queued' && 'Waiting to start'}
          {transfer.status === 'complete' && 'Completed'}
          {transfer.status === 'cancelled' && 'Cancelled'}
          {transfer.status === 'error' && (transfer.error || 'Transfer failed')}
        </span>
        {transfer.status === 'active' && <span className="linear-progress"><i style={{ width: `${transfer.progress}%` }} /></span>}
      </div>
      <div className="transfer-actions">
        {(transfer.status === 'active' || transfer.status === 'queued') && transfer.cancel && <IconButton label={`Cancel ${transfer.name}`} onClick={transfer.cancel}><X size={18} /></IconButton>}
        {transfer.status === 'error' && transfer.retry && <IconButton label={`Retry ${transfer.name}`} onClick={transfer.retry}><RotateCcw size={17} /></IconButton>}
        {!['active', 'queued'].includes(transfer.status) && <IconButton label={`Remove ${transfer.name} from activity`} onClick={() => onRemove(transfer.id)}><X size={17} /></IconButton>}
      </div>
    </article>
  );
}

export function TransferToast({ transfers, onOpen }: { transfers: Transfer[]; onOpen: () => void }) {
  const active = transfers.filter((transfer) => transfer.status === 'active' || transfer.status === 'queued');
  if (!active.length) return null;
  const progress = active.reduce((total, transfer) => total + transfer.progress, 0) / active.length;
  return (
    <button className="transfer-toast" onClick={onOpen} aria-label="Open transfer activity">
      <ProgressRing value={progress} size={34} />
      <span><strong>{active.length === 1 ? active[0].name : `${active.length} active transfers`}</strong><small>{active.length === 1 ? `${Math.round(active[0].progress)}% complete` : 'Tap for details'}</small></span>
      <ArrowDownToLine size={18} />
    </button>
  );
}
