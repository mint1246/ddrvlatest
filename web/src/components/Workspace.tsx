import { useEffect, useLayoutEffect, useRef, useState, type ChangeEvent, type DragEvent, type MouseEvent, type ReactNode } from 'react';
import {
  ArrowDownAZ,
  ArrowDownUp,
  ArrowUpAZ,
  Check,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  CircleUserRound,
  Command,
  Copy,
  Download,
  FileSearch,
  FolderOpen,
  FolderPlus,
  Grid2X2,
  Info,
  LayoutList,
  LockKeyhole,
  MoreHorizontal,
  MoveRight,
  PackageOpen,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Settings2,
  Trash2,
  Upload,
  X,
} from 'lucide-react';
import type { AuthConfig, Breadcrumb, Complexity, Directory, FileItem, ItemFilter, SortKey, ViewMode } from '../types';
import { fileKind, formatBytes, formatRelativeDate, isPreviewable, summarizeItems } from '../utils';
import { Button, IconButton, Logo, Segmented } from './Primitives';
import { FileGlyph } from './FileGlyph';

type ItemAction = 'open' | 'preview' | 'download' | 'copy' | 'rename' | 'move' | 'details' | 'delete';

interface WorkspaceProps {
  directory: Directory | null;
  breadcrumbs: Breadcrumb[];
  items: FileItem[];
  loading: boolean;
  error: string;
  authRequired: boolean;
  authConfig: AuthConfig;
  authenticated: boolean;
  canWrite: boolean;
  online: boolean;
  query: string;
  onQuery: (value: string) => void;
  filter: ItemFilter;
  onFilter: (value: ItemFilter) => void;
  sortKey: SortKey;
  descending: boolean;
  onSort: (key: SortKey, descending: boolean) => void;
  view: ViewMode;
  onView: (view: ViewMode) => void;
  complexity: Complexity;
  selected: Set<string>;
  activeTransfers: number;
  onNavigate: (id: string, name?: string) => void;
  onItemAction: (action: ItemAction, item: FileItem) => void;
  onToggleSelected: (item: FileItem) => void;
  onClearSelected: () => void;
  onBulkDelete: (items: FileItem[]) => void;
  onBulkMove: (items: FileItem[]) => void;
  onUpload: (files: File[]) => void;
  onNewFolder: () => void;
  onOpenSettings: () => void;
  onOpenCommands: () => void;
  onOpenTransfers: () => void;
  onSignIn: () => void;
  onRefresh: () => void;
}

export function Workspace(props: WorkspaceProps) {
  const {
    directory, breadcrumbs, items, loading, error, authRequired, authConfig, authenticated, canWrite, online,
    query, onQuery, filter, onFilter, sortKey, descending, onSort, view, onView, complexity, selected,
    activeTransfers, onNavigate, onItemAction, onToggleSelected, onClearSelected, onBulkDelete, onBulkMove,
    onUpload, onNewFolder, onOpenSettings, onOpenCommands, onOpenTransfers, onSignIn, onRefresh,
  } = props;
  const [addOpen, setAddOpen] = useState(false);
  const [sortOpen, setSortOpen] = useState(false);
  const [itemMenu, setItemMenu] = useState<{ id: string; left: number; top: number } | null>(null);
  const [dragDepth, setDragDepth] = useState(0);
  const fileInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!addOpen && !sortOpen && !itemMenu) return;
    const close = (event: PointerEvent) => {
      if (!(event.target as Element | null)?.closest('[data-popover-root]')) {
        setAddOpen(false);
        setSortOpen(false);
        setItemMenu(null);
      }
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setAddOpen(false);
        setSortOpen(false);
        setItemMenu(null);
      }
    };
    const timer = window.setTimeout(() => document.addEventListener('pointerdown', close), 0);
    document.addEventListener('keydown', escape);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('pointerdown', close);
      document.removeEventListener('keydown', escape);
    };
  }, [addOpen, sortOpen, itemMenu]);

  const selectedItems = directory?.files.filter((item) => selected.has(item.id)) ?? [];
  const onlySelected = selectedItems.length === 1 ? selectedItems[0] : null;
  const menuItem = itemMenu ? directory?.files.find((item) => item.id === itemMenu.id) ?? null : null;
  const summary = summarizeItems(directory?.files ?? []);
  const isDragging = dragDepth > 0;

  const selectFiles = (event: ChangeEvent<HTMLInputElement>) => {
    if (event.target.files?.length) onUpload([...event.target.files]);
    event.target.value = '';
  };

  const handleDragEnter = (event: DragEvent) => {
    if (!event.dataTransfer.types.includes('Files')) return;
    event.preventDefault();
    setDragDepth((depth) => depth + 1);
  };
  const handleDragLeave = (event: DragEvent) => {
    event.preventDefault();
    setDragDepth((depth) => Math.max(0, depth - 1));
  };
  const handleDrop = (event: DragEvent) => {
    event.preventDefault();
    setDragDepth(0);
    if (canWrite && event.dataTransfer.files.length) onUpload([...event.dataTransfer.files]);
  };

  return (
    <div
      className={`workspace-shell${isDragging ? ' is-dragging' : ''}`}
      onDragEnter={handleDragEnter}
      onDragOver={(event) => event.preventDefault()}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <header className="app-bar">
        <a href="/" className="brand-link" onClick={(event) => { event.preventDefault(); onNavigate('root'); }}>
          <Logo />
        </a>
        {!online && <div className="app-bar-context offline"><span className="connection-dot offline" /><span>Offline</span></div>}
        <div className="app-bar-actions">
          {activeTransfers > 0 && (
            <button className="transfer-pill" onClick={onOpenTransfers} aria-label={`${activeTransfers} active transfers`}>
              <span className="transfer-pulse" />
              <span>{activeTransfers} active</span>
            </button>
          )}
          <IconButton label="Refresh folder" onClick={onRefresh}><RefreshCw size={20} /></IconButton>
          <IconButton label="Open command menu" onClick={onOpenCommands} className="command-trigger"><Command size={20} /></IconButton>
          <IconButton label="Preferences and account" onClick={onOpenSettings} className="account-trigger">
            {authConfig.login ? <CircleUserRound size={21} /> : <Settings2 size={20} />}
            <span className={`account-status${authenticated ? ' signed-in' : ''}`} />
          </IconButton>
        </div>
      </header>

      <main className="workspace-main">
        <section className="workspace-hero" aria-labelledby="workspace-title">
          <div className="hero-copy">
            <h1 id="workspace-title">{directory?.id === 'root' ? 'My drive' : directory?.name || 'Files'}</h1>
            <p>{loading ? 'Opening your workspace…' : `${summary.folders} folder${summary.folders === 1 ? '' : 's'} · ${summary.files} file${summary.files === 1 ? '' : 's'}${summary.bytes ? ` · ${formatBytes(summary.bytes)}` : ''}`}</p>
          </div>
        </section>

        <section className="browser-surface">
          <div className="browser-topline">
            <nav className="breadcrumbs" aria-label="Folder path">
              {breadcrumbs.map((crumb, index) => {
                const current = index === breadcrumbs.length - 1;
                return (
                  <span className="breadcrumb-part" key={crumb.id}>
                    {index > 0 && <ChevronRight size={16} aria-hidden="true" />}
                    <button disabled={current} aria-current={current ? 'page' : undefined} onClick={() => onNavigate(crumb.id)}>
                      {index === 0 && <FolderOpen size={16} />}
                      <span>{crumb.name}</span>
                    </button>
                  </span>
                );
              })}
            </nav>

            {canWrite ? (
              <div className="popover-root add-root" data-popover-root>
                <Button tone="filled" icon={<Plus size={19} />} onClick={() => setAddOpen((open) => !open)} aria-expanded={addOpen}>
                  Add <ChevronDown size={15} />
                </Button>
                {addOpen && (
                  <div className="popover action-popover" role="menu">
                    <button role="menuitem" onClick={() => { setAddOpen(false); fileInput.current?.click(); }}>
                      <span className="menu-icon upload"><Upload size={19} /></span>
                      <span><strong>Upload files</strong><small>Choose one or several files</small></span>
                    </button>
                    <button role="menuitem" onClick={() => { setAddOpen(false); onNewFolder(); }}>
                      <span className="menu-icon folder"><FolderPlus size={19} /></span>
                      <span><strong>New folder</strong><small>Organize this location</small></span>
                    </button>
                  </div>
                )}
              </div>
            ) : authConfig.login ? (
              <Button tone="filled" icon={<LockKeyhole size={18} />} onClick={onSignIn}>Sign in to add</Button>
            ) : null}
            <input ref={fileInput} type="file" multiple hidden onChange={selectFiles} />
          </div>

          {selected.size > 0 ? (
            <div className="selection-bar" role="toolbar" aria-label="Selection actions">
              <IconButton label="Clear selection" onClick={onClearSelected}><X size={20} /></IconButton>
              <strong>{selected.size} selected</strong>
              <span className="selection-spacer" />
              {onlySelected && !onlySelected.dir && (
                <Button tone="text" icon={<Download size={18} />} onClick={() => onItemAction('download', onlySelected)}>Download</Button>
              )}
              {onlySelected && canWrite && (
                <Button tone="text" icon={<Pencil size={18} />} onClick={() => onItemAction('rename', onlySelected)}>Rename</Button>
              )}
              {canWrite && <Button tone="text" icon={<MoveRight size={18} />} onClick={() => onBulkMove(selectedItems)}>Move</Button>}
              {canWrite && <Button tone="danger" icon={<Trash2 size={18} />} onClick={() => onBulkDelete(selectedItems)}>Delete</Button>}
            </div>
          ) : (
            <div className="browser-toolbar">
              <label className="search-field">
                <Search size={20} aria-hidden="true" />
                <input
                  type="search"
                  value={query}
                  onChange={(event) => onQuery(event.target.value)}
                  placeholder="Search this folder"
                  aria-label="Search this folder"
                  spellCheck={false}
                />
                {query && <IconButton label="Clear search" onClick={() => onQuery('')}><X size={17} /></IconButton>}
                <kbd>/</kbd>
              </label>

              <div className="toolbar-controls">
                {complexity !== 'calm' && (
                  <div className="filter-chips" aria-label="File type filter">
                    {([
                      ['all', 'All'], ['folders', 'Folders'], ['documents', 'Docs'], ['media', 'Media'], ['archives', 'Archives'],
                    ] as Array<[ItemFilter, string]>).map(([value, label]) => (
                      <button key={value} className={filter === value ? 'active' : ''} onClick={() => onFilter(value)} aria-pressed={filter === value}>{label}</button>
                    ))}
                  </div>
                )}

                <div className="popover-root sort-root" data-popover-root>
                  <IconButton label="Sort items" selected={sortOpen} onClick={() => setSortOpen((open) => !open)}>
                    <ArrowDownUp size={19} />
                  </IconButton>
                  {sortOpen && (
                    <div className="popover sort-popover" role="menu">
                      <span className="menu-heading">Sort by</span>
                      {([
                        ['name', 'Name'], ['mtime', 'Date modified'], ['size', 'File size'],
                      ] as Array<[SortKey, string]>).map(([value, label]) => (
                        <button key={value} role="menuitemradio" aria-checked={sortKey === value} onClick={() => onSort(value, descending)}>
                          <span>{label}</span>{sortKey === value && <Check size={17} />}
                        </button>
                      ))}
                      <span className="menu-divider" />
                      <button role="menuitemradio" aria-checked={!descending} onClick={() => onSort(sortKey, false)}><span>Ascending</span>{!descending && <ArrowDownAZ size={17} />}</button>
                      <button role="menuitemradio" aria-checked={descending} onClick={() => onSort(sortKey, true)}><span>Descending</span>{descending && <ArrowUpAZ size={17} />}</button>
                    </div>
                  )}
                </div>

                <Segmented
                  label="File view"
                  value={view}
                  onChange={onView}
                  options={[
                    { value: 'list', label: 'List', icon: <LayoutList size={16} /> },
                    { value: 'grid', label: 'Grid', icon: <Grid2X2 size={16} /> },
                  ]}
                />
              </div>
            </div>
          )}

          <div className="items-region" aria-live="polite" aria-busy={loading}>
            {loading && <LoadingState view={view} />}
            {!loading && error && (
              <StatePanel icon={<CircleAlert size={35} />} title="This folder couldn’t be opened" body={error}>
                <Button tone="filled" icon={<RefreshCw size={18} />} onClick={onRefresh}>Try again</Button>
              </StatePanel>
            )}
            {!loading && !error && authRequired && (
              <StatePanel icon={<LockKeyhole size={35} />} title="Sign in to continue" body={authConfig.anonymous ? 'Guest access is read-only. Sign in for private files and editing.' : 'This server requires an account before files can be viewed.'}>
                <Button tone="filled" onClick={onSignIn}>Sign in</Button>
              </StatePanel>
            )}
            {!loading && !error && !authRequired && items.length === 0 && (
              <StatePanel
                icon={query || filter !== 'all' ? <FileSearch size={37} /> : <PackageOpen size={38} />}
                title={query || filter !== 'all' ? 'No matching items' : 'A fresh, empty folder'}
                body={query || filter !== 'all' ? 'Try another search or clear the active filter.' : canWrite ? 'Drop files here, or use Add to upload and create folders.' : 'There are no files in this folder yet.'}
              >
                {(query || filter !== 'all') && <Button tone="tonal" onClick={() => { onQuery(''); onFilter('all'); }}>Clear filters</Button>}
              </StatePanel>
            )}
            {!loading && !error && !authRequired && items.length > 0 && (
              <div className={`items-view ${view}-view`} role={view === 'list' ? 'table' : 'list'} aria-label="Files and folders">
                {view === 'list' && (
                  <div className="list-heading" role="row">
                    <span>Name</span><span>Size</span><span>Modified</span><span aria-hidden="true" />
                  </div>
                )}
                <VirtualizedItems
                  items={items}
                  view={view}
                  role={view === 'list' ? 'rowgroup' : undefined}
                  renderItem={(item, index) => (
                    <FileTile
                      key={item.id}
                      item={item}
                      index={index}
                      view={view}
                      selected={selected.has(item.id)}
                      menuOpen={itemMenu?.id === item.id}
                      onToggle={() => onToggleSelected(item)}
                      onOpen={(event) => {
                        if (event.ctrlKey || event.metaKey) onToggleSelected(item);
                        else onItemAction(item.dir ? 'open' : isPreviewable(item) ? 'preview' : 'download', item);
                      }}
                      onMenu={(event) => {
                        if (itemMenu?.id === item.id) { setItemMenu(null); return; }
                        const rect = event.currentTarget.getBoundingClientRect();
                        const menuHeight = item.dir ? 236 : 316;
                        const left = Math.max(8, Math.min(rect.right - 204, window.innerWidth - 212));
                        const top = rect.bottom + 7 + menuHeight > window.innerHeight
                          ? Math.max(8, rect.top - menuHeight - 7)
                          : rect.bottom + 7;
                        setItemMenu({ id: item.id, left, top });
                      }}
                    />
                  )}
                />
              </div>
            )}
          </div>
        </section>
      </main>

      {menuItem && itemMenu && (
        <div
          className="popover item-popover floating-item-popover"
          role="menu"
          data-popover-root
          style={{ left: itemMenu.left, top: itemMenu.top }}
        >
          {menuItem.dir ? (
            <MenuButton icon={<FolderOpen size={18} />} label="Open" onClick={() => { setItemMenu(null); onItemAction('open', menuItem); }} />
          ) : isPreviewable(menuItem) ? (
            <MenuButton icon={<FileSearch size={18} />} label="Preview" onClick={() => { setItemMenu(null); onItemAction('preview', menuItem); }} />
          ) : null}
          {!menuItem.dir && <MenuButton icon={<Download size={18} />} label="Download" onClick={() => { setItemMenu(null); onItemAction('download', menuItem); }} />}
          {!menuItem.dir && <MenuButton icon={<Copy size={18} />} label="Copy link" onClick={() => { setItemMenu(null); onItemAction('copy', menuItem); }} />}
          <MenuButton icon={<Info size={18} />} label="Details" onClick={() => { setItemMenu(null); onItemAction('details', menuItem); }} />
          {canWrite && <span className="menu-divider" />}
          {canWrite && <MenuButton icon={<Pencil size={18} />} label="Rename" onClick={() => { setItemMenu(null); onItemAction('rename', menuItem); }} />}
          {canWrite && <MenuButton icon={<MoveRight size={18} />} label="Move" onClick={() => { setItemMenu(null); onItemAction('move', menuItem); }} />}
          {canWrite && <MenuButton danger icon={<Trash2 size={18} />} label="Delete" onClick={() => { setItemMenu(null); onItemAction('delete', menuItem); }} />}
        </div>
      )}

      {isDragging && (
        <div className="drop-overlay" aria-hidden="true">
          <div>
            {canWrite ? <Upload size={42} /> : <LockKeyhole size={42} />}
            <strong>{canWrite ? `Drop into ${directory?.id === 'root' ? 'My drive' : directory?.name}` : 'Sign in before uploading'}</strong>
            <span>{canWrite ? 'Your files will start uploading immediately' : 'Guest access is read-only'}</span>
          </div>
        </div>
      )}
    </div>
  );
}

const VIRTUALIZATION_THRESHOLD = 80;
const VIRTUAL_OVERSCAN = 4;

function VirtualizedItems({ items, view, role, renderItem }: {
  items: FileItem[];
  view: ViewMode;
  role?: 'rowgroup';
  renderItem: (item: FileItem, index: number) => ReactNode;
}) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [viewport, setViewport] = useState({ width: 0, height: 0, scrollTop: 0 });
  const [itemExtent, setItemExtent] = useState(view === 'grid' ? 160 : 64);

  useLayoutEffect(() => {
    const viewportElement = viewportRef.current;
    if (!viewportElement) return undefined;
    let frame = 0;
    const measure = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => setViewport({
        width: viewportElement.clientWidth,
        height: viewportElement.clientHeight,
        scrollTop: viewportElement.scrollTop,
      }));
    };
    const resizeObserver = new ResizeObserver(measure);
    resizeObserver.observe(viewportElement);
    viewportElement.addEventListener('scroll', measure, { passive: true });
    measure();
    return () => {
      cancelAnimationFrame(frame);
      resizeObserver.disconnect();
      viewportElement.removeEventListener('scroll', measure);
    };
  }, [view]);

  useLayoutEffect(() => {
    const firstTile = viewportRef.current?.querySelector<HTMLElement>('.file-tile');
    if (!firstTile) return undefined;
    const measure = () => setItemExtent(firstTile.getBoundingClientRect().height || (view === 'grid' ? 160 : 64));
    const resizeObserver = new ResizeObserver(measure);
    resizeObserver.observe(firstTile);
    measure();
    return () => resizeObserver.disconnect();
  }, [view, items.length]);

  if (items.length <= VIRTUALIZATION_THRESHOLD) {
    return <div className="items-container" role={role}>{items.map(renderItem)}</div>;
  }

  const compact = document.documentElement.dataset.density === 'compact';
  const gap = view === 'grid' ? (compact ? 7 : 11) : (compact ? 1 : 3);
  const minCardWidth = compact ? 154 : 180;
  const columns = view === 'grid'
    ? Math.max(1, Math.floor((viewport.width + gap) / (minCardWidth + gap)))
    : 1;
  const rowCount = view === 'grid' ? Math.ceil(items.length / columns) : items.length;
  const stride = itemExtent + gap;
  const totalHeight = Math.max(0, rowCount * stride - gap);
  const firstRow = Math.max(0, Math.floor(viewport.scrollTop / stride) - VIRTUAL_OVERSCAN);
  const lastRow = Math.min(rowCount, Math.ceil((viewport.scrollTop + viewport.height) / stride) + VIRTUAL_OVERSCAN);
  const firstIndex = view === 'grid' ? firstRow * columns : firstRow;
  const lastIndex = view === 'grid' ? Math.min(items.length, lastRow * columns) : lastRow;
  const cardWidth = view === 'grid'
    ? Math.max(0, (viewport.width - gap * (columns - 1)) / columns)
    : viewport.width;

  return (
    <div ref={viewportRef} className="virtual-viewport" aria-label={`${items.length} items`}>
      <div className="items-container virtual-items-container" role={role} style={{ height: totalHeight }}>
        {items.slice(firstIndex, lastIndex).map((item, offset) => {
          const index = firstIndex + offset;
          const row = view === 'grid' ? Math.floor(index / columns) : index;
          const column = view === 'grid' ? index % columns : 0;
          return (
            <div
              className="virtual-item"
              key={item.id}
              style={view === 'grid'
                ? { top: row * stride, left: column * (cardWidth + gap), width: cardWidth }
                : { top: row * stride, left: 0, right: 0 }}
            >
              {renderItem(item, index)}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function FileTile({ item, index, view, selected, menuOpen, onToggle, onOpen, onMenu }: {
  item: FileItem;
  index: number;
  view: ViewMode;
  selected: boolean;
  menuOpen: boolean;
  onToggle: () => void;
  onOpen: (event: MouseEvent<HTMLButtonElement>) => void;
  onMenu: (event: MouseEvent<HTMLButtonElement>) => void;
}) {
  const kind = fileKind(item);
  return (
    <article
      className={`file-tile${selected ? ' selected' : ''}${menuOpen ? ' menu-open' : ''} kind-${kind}`}
      role={view === 'list' ? 'row' : 'listitem'}
      style={{ '--reveal-delay': `${Math.min(index * 18, 220)}ms` } as React.CSSProperties}
    >
      <button className="select-control" aria-label={`${selected ? 'Deselect' : 'Select'} ${item.name}`} aria-pressed={selected} onClick={onToggle}>
        {selected && <Check size={14} />}
      </button>
      <button className="file-primary" onClick={onOpen}>
        <FileGlyph item={item} size={view === 'grid' ? 31 : 22} />
        <span className="file-copy">
          <strong title={item.name}>{item.name}</strong>
          {view === 'grid' && <small>{item.dir ? 'Folder' : `${formatBytes(item.size)} · ${formatRelativeDate(item.mtime)}`}</small>}
        </span>
      </button>
      {view === 'list' && <span className="file-size" role="cell">{item.dir ? '—' : formatBytes(item.size)}</span>}
      {view === 'list' && <time className="file-date" role="cell" dateTime={item.mtime}>{formatRelativeDate(item.mtime)}</time>}
      <div className="popover-root item-menu-root" data-popover-root>
        <IconButton label={`Actions for ${item.name}`} selected={menuOpen} onClick={onMenu}><MoreHorizontal size={19} /></IconButton>
      </div>
    </article>
  );
}

function MenuButton({ icon, label, danger, onClick }: { icon: React.ReactNode; label: string; danger?: boolean; onClick: () => void }) {
  return <button role="menuitem" className={danger ? 'danger' : ''} onClick={onClick}>{icon}<span>{label}</span></button>;
}

function LoadingState({ view }: { view: ViewMode }) {
  return (
    <div className={`loading-layout ${view}`} aria-label="Loading folder">
      {Array.from({ length: view === 'grid' ? 8 : 6 }).map((_, index) => (
        <span className="skeleton-item" key={index}><i /><b /><em /></span>
      ))}
    </div>
  );
}

function StatePanel({ icon, title, body, children }: { icon: React.ReactNode; title: string; body: string; children?: React.ReactNode }) {
  return (
    <div className="state-panel">
      <div className="state-illustration" aria-hidden="true">{icon}</div>
      <h2>{title}</h2>
      <p>{body}</p>
      {children && <div className="state-actions">{children}</div>}
    </div>
  );
}
