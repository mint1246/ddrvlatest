import { CheckCircle2, CircleAlert, Info, X } from 'lucide-react';
import type { ToastMessage } from '../types';
import { IconButton } from './Primitives';

export function ToastStack({ toasts, onDismiss }: { toasts: ToastMessage[]; onDismiss: (id: number) => void }) {
  return (
    <div className="toast-stack" aria-live="polite" aria-relevant="additions">
      {toasts.map((toast) => (
        <div className={`toast tone-${toast.tone || 'default'}`} role={toast.tone === 'error' ? 'alert' : 'status'} key={toast.id}>
          <span className="toast-icon" aria-hidden="true">
            {toast.tone === 'success' ? <CheckCircle2 size={19} /> : toast.tone === 'error' ? <CircleAlert size={19} /> : <Info size={19} />}
          </span>
          <span>{toast.message}</span>
          {toast.action && <button className="toast-action" onClick={toast.action.run}>{toast.action.label}</button>}
          <IconButton label="Dismiss message" onClick={() => onDismiss(toast.id)}><X size={16} /></IconButton>
        </div>
      ))}
    </div>
  );
}
