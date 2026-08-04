import { useEffect, useMemo, useRef, useState } from 'react';
import { Command, CornerDownLeft, Search, X } from 'lucide-react';
import { Dialog, IconButton } from './Primitives';

export interface CommandItem {
  id: string;
  label: string;
  detail?: string;
  shortcut?: string;
  keywords?: string;
  icon: React.ReactNode;
  run: () => void | Promise<void>;
}

export function CommandPalette({ open, commands, onClose }: { open: boolean; commands: CommandItem[]; onClose: () => void }) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const filtered = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    return commands.filter((command) => !needle || `${command.label} ${command.detail || ''} ${command.keywords || ''} ${command.shortcut || ''}`.toLocaleLowerCase().includes(needle));
  }, [commands, query]);

  useEffect(() => {
    if (!open) return;
    setQuery('');
    setActive(0);
    window.setTimeout(() => inputRef.current?.focus(), 60);
  }, [open]);

  useEffect(() => setActive((index) => Math.min(index, Math.max(0, filtered.length - 1))), [filtered.length]);

  const run = (command: CommandItem | undefined) => {
    if (!command) return;
    onClose();
    void command.run();
  };

  return (
    <Dialog open={open} onClose={onClose} labelledBy="command-title" className="command-dialog">
      <div className="command-search">
        <Command size={21} aria-hidden="true" />
        <input
          ref={inputRef}
          value={query}
          onChange={(event) => { setQuery(event.target.value); setActive(0); }}
          onKeyDown={(event) => {
            if (event.key === 'ArrowDown') { event.preventDefault(); setActive((index) => Math.min(filtered.length - 1, index + 1)); }
            if (event.key === 'ArrowUp') { event.preventDefault(); setActive((index) => Math.max(0, index - 1)); }
            if (event.key === 'Enter') { event.preventDefault(); run(filtered[active]); }
          }}
          placeholder="Type a command or search an action"
          aria-label="Search commands"
        />
        <kbd>Esc</kbd>
        <IconButton label="Close command menu" onClick={onClose}><X size={18} /></IconButton>
      </div>
      <h2 id="command-title" className="sr-only">Command menu</h2>
      <div className="command-results" role="listbox" aria-label="Available commands">
        {filtered.length === 0 && <div className="command-empty"><Search size={26} /><strong>No matching command</strong><span>Try “upload”, “theme”, or “view”.</span></div>}
        {filtered.map((command, index) => (
          <button
            type="button"
            role="option"
            aria-selected={index === active}
            className={index === active ? 'active' : ''}
            key={command.id}
            onMouseEnter={() => setActive(index)}
            onClick={() => run(command)}
          >
            <span className="command-icon">{command.icon}</span>
            <span><strong>{command.label}</strong>{command.detail && <small>{command.detail}</small>}</span>
            {command.shortcut && <kbd>{command.shortcut}</kbd>}
            {index === active && <CornerDownLeft className="command-enter" size={16} />}
          </button>
        ))}
      </div>
      <footer className="command-footer"><span><kbd>↑</kbd><kbd>↓</kbd> Navigate</span><span><kbd>↵</kbd> Run</span><span>Tip: press <kbd>?</kbd> any time</span></footer>
    </Dialog>
  );
}

