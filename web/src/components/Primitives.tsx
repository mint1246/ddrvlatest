import { useEffect, useId, useRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { Check, X } from 'lucide-react';

export function Logo({ compact = false }: { compact?: boolean }) {
  return (
    <div className={`brand-lockup${compact ? ' compact' : ''}`} aria-label="DDrv">
      <span className="brand-mark" aria-hidden="true">
        <span />
        <span />
        <span />
      </span>
      {!compact && <span className="brand-wordmark">DDrv</span>}
    </div>
  );
}

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  tone?: 'filled' | 'tonal' | 'outlined' | 'text' | 'danger';
  icon?: ReactNode;
};

export function Button({ tone = 'tonal', icon, className = '', children, ...props }: ButtonProps) {
  return (
    <button className={`button button-${tone} ${className}`} {...props}>
      {icon && <span className="button-icon" aria-hidden="true">{icon}</span>}
      {children}
    </button>
  );
}

export function IconButton({ label, selected, className = '', children, ...props }: ButtonHTMLAttributes<HTMLButtonElement> & { label: string; selected?: boolean }) {
  return (
    <button
      className={`icon-button${selected ? ' selected' : ''} ${className}`}
      aria-label={label}
      title={props.title || label}
      aria-pressed={selected === undefined ? undefined : selected}
      {...props}
    >
      {children}
    </button>
  );
}

export function Dialog({
  open,
  onClose,
  labelledBy,
  className = '',
  children,
}: {
  open: boolean;
  onClose: () => void;
  labelledBy: string;
  className?: string;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    if (!open && dialog.open) dialog.close();
  }, [open]);

  return (
    <dialog
      ref={ref}
      className={`dialog ${className}`}
      aria-labelledby={labelledBy}
      onCancel={(event) => { event.preventDefault(); onClose(); }}
      onClick={(event) => { if (event.target === event.currentTarget) onClose(); }}
    >
      {children}
    </dialog>
  );
}

export function DialogHeader({ icon, title, description, onClose, titleId }: {
  icon?: ReactNode;
  title: string;
  description?: string;
  onClose: () => void;
  titleId: string;
}) {
  return (
    <header className="dialog-header">
      {icon && <div className="dialog-icon" aria-hidden="true">{icon}</div>}
      <div className="dialog-heading">
        <h2 id={titleId}>{title}</h2>
        {description && <p>{description}</p>}
      </div>
      <IconButton label="Close" onClick={onClose}><X size={20} /></IconButton>
    </header>
  );
}

export function Field({ label, error, hint, children }: { label: string; error?: string; hint?: string; children: ReactNode }) {
  const id = useId();
  return (
    <label className={`field${error ? ' field-error' : ''}`}>
      <span className="field-label">{label}</span>
      {children}
      {(error || hint) && <span className="field-support" id={id}>{error || hint}</span>}
    </label>
  );
}

export function Segmented<T extends string>({ value, options, onChange, label }: {
  value: T;
  options: Array<{ value: T; label: string; icon?: ReactNode }>;
  onChange: (value: T) => void;
  label: string;
}) {
  return (
    <div className="segmented" role="radiogroup" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          role="radio"
          aria-checked={value === option.value}
          className={value === option.value ? 'active' : ''}
          onClick={() => onChange(option.value)}
        >
          {value === option.value ? <Check size={15} /> : option.icon}
          <span>{option.label}</span>
        </button>
      ))}
    </div>
  );
}

export function Switch({ checked, onChange, label, description }: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  description?: string;
}) {
  return (
    <label className="switch-row">
      <span>
        <strong>{label}</strong>
        {description && <small>{description}</small>}
      </span>
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
      <span className="switch-track" aria-hidden="true"><span /></span>
    </label>
  );
}

export function ProgressRing({ value, size = 38 }: { value: number; size?: number }) {
  const clamped = Math.max(0, Math.min(100, value));
  return (
    <span className="progress-ring" style={{ width: size, height: size, '--progress': `${clamped * 3.6}deg` } as React.CSSProperties}>
      <span>{Math.round(clamped)}</span>
    </span>
  );
}
