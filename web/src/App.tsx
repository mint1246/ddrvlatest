import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  Command,
  FolderPlus,
  Grid2X2,
  LayoutList,
  LogIn,
  LogOut,
  MoonStar,
  RefreshCw,
  Search,
  Settings2,
  Sun,
  Upload,
} from 'lucide-react';
import { ApiError, DdrvApi } from './api';
import { downloadFile } from './download';
import { loadSettings, loadSort, loadToken, loadViewMode, saveSettings, storeToken } from './storage';
import type {
  AppSettings,
  AuthConfig,
  Breadcrumb,
  Directory,
  FileItem,
  ItemFilter,
  SortKey,
  ToastMessage,
  Transfer,
  ViewMode,
} from './types';
import { fileUrl, filterAndSort, isPreviewable } from './utils';
import { Workspace } from './components/Workspace';
import { LoginDialog, NameDialog, DeleteDialog } from './components/Dialogs';
import { PreviewDialog } from './components/PreviewDialog';
import { SettingsPanel } from './components/SettingsPanel';
import { TransferCenter, TransferToast } from './components/TransferCenter';
import { ToastStack } from './components/Toasts';
import { DetailsPanel } from './components/DetailsPanel';
import { CommandPalette, type CommandItem } from './components/CommandPalette';
import { MoveDialog } from './components/MoveDialog';

type NameOperation = { mode: 'create' } | { mode: 'rename'; item: FileItem };

function messageOf(reason: unknown) {
  return reason instanceof Error ? reason.message : 'Something went wrong.';
}

function isTypingTarget(target: EventTarget | null) {
  const element = target as HTMLElement | null;
  return Boolean(element?.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(element?.tagName || ''));
}

export default function App() {
  const tokenRef = useRef<string | null>(loadToken());
  const configRef = useRef<AuthConfig>({ login: false, anonymous: true });
  const initializedRef = useRef(false);
  const api = useMemo(() => new DdrvApi(() => tokenRef.current), []);
  const requestRef = useRef(0);
  const directoryRef = useRef<Directory | null>(null);
  const toastId = useRef(0);

  const [settings, setSettings] = useState<AppSettings>(loadSettings);
  const [config, setConfig] = useState<AuthConfig>({ login: false, anonymous: true });
  const [authenticated, setAuthenticated] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [loginOpen, setLoginOpen] = useState(false);
  const [loginBusy, setLoginBusy] = useState(false);
  const [loginError, setLoginError] = useState('');
  const [directory, setDirectory] = useState<Directory | null>(null);
  const [breadcrumbs, setBreadcrumbs] = useState<Breadcrumb[]>([{ id: 'root', name: 'My drive' }]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState('');
  const [online, setOnline] = useState(navigator.onLine);
  const [query, setQuery] = useState('');
  const [filter, setFilter] = useState<ItemFilter>('all');
  const initialSort = useMemo(loadSort, []);
  const [sortKey, setSortKey] = useState<SortKey>(initialSort.key);
  const [descending, setDescending] = useState(initialSort.descending);
  const [view, setView] = useState<ViewMode>(loadViewMode);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [nameOperation, setNameOperation] = useState<NameOperation | null>(null);
  const [nameBusy, setNameBusy] = useState(false);
  const [nameError, setNameError] = useState('');
  const [deleteItems, setDeleteItems] = useState<FileItem[]>([]);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteError, setDeleteError] = useState('');
  const [moveItems, setMoveItems] = useState<FileItem[]>([]);
  const [moveBusy, setMoveBusy] = useState(false);
  const [moveError, setMoveError] = useState('');
  const [previewItem, setPreviewItem] = useState<FileItem | null>(null);
  const [detailsItem, setDetailsItem] = useState<FileItem | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [commandsOpen, setCommandsOpen] = useState(false);
  const [transfersOpen, setTransfersOpen] = useState(false);
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  const canWrite = !config.login || authenticated;
  const visibleItems = useMemo(
    () => filterAndSort(directory?.files ?? [], query, filter, sortKey, descending),
    [directory?.files, query, filter, sortKey, descending],
  );
  const activeTransfers = transfers.filter((transfer) => transfer.status === 'active' || transfer.status === 'queued').length;

  const notify = useCallback((message: string, tone: ToastMessage['tone'] = 'default', action?: ToastMessage['action']) => {
    const id = ++toastId.current;
    setToasts((current) => [...current.slice(-3), { id, message, tone, action }]);
    window.setTimeout(() => setToasts((current) => current.filter((toast) => toast.id !== id)), tone === 'error' ? 7000 : 4500);
  }, []);

  const handleUnauthorized = useCallback(() => {
    tokenRef.current = null;
    storeToken(null);
    setAuthenticated(false);
    setAuthRequired(!configRef.current.anonymous);
    if (configRef.current.login) setLoginOpen(true);
  }, []);

  const loadDirectory = useCallback(async (
    id: string,
    options: { history?: 'push' | 'replace' | 'none'; trail?: Breadcrumb[]; fallbackRoot?: boolean } = {},
  ) => {
    const request = ++requestRef.current;
    setLoading(true);
    setLoadError('');
    setAuthRequired(false);
    setSelected(new Set());
    try {
      const result = await api.getDirectory(id);
      if (request !== requestRef.current) return;
      const trail = options.trail ?? await api.breadcrumbs(result.id);
      if (request !== requestRef.current) return;
      setDirectory(result);
      directoryRef.current = result;
      setBreadcrumbs(trail);
      setQuery('');
      setFilter('all');
      localStorage.setItem('last_dir_id', result.id);
      if (options.history !== 'none') {
        const url = new URL(location.href);
        if (result.id === 'root') url.searchParams.delete('dir');
        else url.searchParams.set('dir', result.id);
        history[options.history === 'replace' ? 'replaceState' : 'pushState']({ dir: result.id }, '', url);
      }
    } catch (reason) {
      if (request !== requestRef.current) return;
      if (reason instanceof ApiError && reason.status === 401) {
        handleUnauthorized();
      } else if (options.fallbackRoot && id !== 'root') {
        await loadDirectory('root', { history: 'replace', trail: [{ id: 'root', name: 'My drive' }] });
        notify('The previous folder no longer exists. Opened My drive instead.');
      } else {
        setLoadError(messageOf(reason));
      }
    } finally {
      if (request === requestRef.current) setLoading(false);
    }
  }, [api, handleUnauthorized, notify]);

  const navigateTo = useCallback((id: string, name?: string) => {
    let trail: Breadcrumb[] | undefined;
    const existing = breadcrumbs.findIndex((crumb) => crumb.id === id);
    if (existing >= 0) trail = breadcrumbs.slice(0, existing + 1);
    else if (name) trail = [...breadcrumbs, { id, name }];
    void loadDirectory(id, { history: 'push', trail });
  }, [breadcrumbs, loadDirectory]);

  const refresh = useCallback(() => {
    const current = directoryRef.current?.id || 'root';
    void loadDirectory(current, { history: 'none', trail: breadcrumbs });
  }, [breadcrumbs, loadDirectory]);

  useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;
    const initialize = async () => {
      let authConfig: AuthConfig = { login: false, anonymous: true };
      try { authConfig = await api.config(); } catch { /* keep open-access fallback for older servers */ }
      setConfig(authConfig);
      configRef.current = authConfig;
      if (tokenRef.current) {
        try {
          await api.checkToken();
          setAuthenticated(true);
          storeToken(tokenRef.current);
        } catch {
          tokenRef.current = null;
          storeToken(null);
        }
      }
      if (authConfig.login && !authConfig.anonymous && !tokenRef.current) {
        setAuthRequired(true);
        setLoginOpen(true);
      }
      const params = new URLSearchParams(location.search);
      const initial = params.get('dir') || localStorage.getItem('last_dir_id') || 'root';
      await loadDirectory(initial, { history: 'replace', fallbackRoot: true });
      document.documentElement.classList.add('app-ready');
    };
    void initialize();
  }, [api, loadDirectory]);

  useEffect(() => {
    directoryRef.current = directory;
  }, [directory]);

  useEffect(() => {
    saveSettings(settings);
    const root = document.documentElement;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const apply = () => {
      const dark = settings.theme === 'dark' || (settings.theme === 'system' && media.matches);
      root.dataset.theme = dark ? 'dark' : 'light';
      root.dataset.accent = settings.accent;
      root.dataset.complexity = settings.complexity;
      root.dataset.density = settings.density;
      root.dataset.motion = settings.reducedMotion ? 'reduced' : 'full';
      root.style.colorScheme = dark ? 'dark' : 'light';
      const meta = document.querySelector<HTMLMetaElement>('#meta-theme-color');
      if (meta) meta.content = dark ? '#101412' : '#f7f9f7';
    };
    apply();
    media.addEventListener('change', apply);
    return () => media.removeEventListener('change', apply);
  }, [settings]);

  useEffect(() => {
    localStorage.setItem('view_mode', view);
    localStorage.setItem('sort_by', sortKey);
    localStorage.setItem('sort_desc', descending ? '1' : '0');
  }, [view, sortKey, descending]);

  useEffect(() => {
    const onlineListener = () => setOnline(true);
    const offlineListener = () => setOnline(false);
    window.addEventListener('online', onlineListener);
    window.addEventListener('offline', offlineListener);
    return () => { window.removeEventListener('online', onlineListener); window.removeEventListener('offline', offlineListener); };
  }, []);

  useEffect(() => {
    const pop = () => {
      const id = new URLSearchParams(location.search).get('dir') || 'root';
      void loadDirectory(id, { history: 'none', fallbackRoot: true });
    };
    window.addEventListener('popstate', pop);
    return () => window.removeEventListener('popstate', pop);
  }, [loadDirectory]);

  useEffect(() => {
    if (!activeTransfers) return;
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [activeTransfers]);

  useEffect(() => {
    if ('serviceWorker' in navigator && import.meta.env.PROD) navigator.serviceWorker.register('/sw.js').catch(() => undefined);
  }, []);

  const toggleSelected = (item: FileItem) => setSelected((current) => {
    const next = new Set(current);
    if (next.has(item.id)) next.delete(item.id); else next.add(item.id);
    return next;
  });

  const copyLink = async (item: FileItem) => {
    const url = `${location.origin}${fileUrl(item)}`;
    try {
      await navigator.clipboard.writeText(url);
    } catch {
      const input = document.createElement('textarea');
      input.value = url;
      input.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(input);
      input.select();
      document.execCommand('copy');
      input.remove();
    }
    notify('Download link copied', 'success');
  };

  const beginDownload = useCallback((item: FileItem) => {
    const id = `download-${item.id}-${Date.now()}`;
    const controller = new AbortController();
    const retry = () => beginDownload(item);
    setTransfers((current) => [{
      id, name: item.name, kind: 'download', status: 'active', progress: 0, loaded: 0, total: item.size,
      startedAt: Date.now(), cancel: () => controller.abort(), retry,
    }, ...current]);
    void downloadFile(item, {
      signal: controller.signal,
      preferDiskStream: settings.streamDownloads,
      onProgress: (loaded, total) => setTransfers((current) => current.map((transfer) => transfer.id === id ? {
        ...transfer, loaded, total, progress: total > 0 ? Math.min(100, loaded / total * 100) : 0,
      } : transfer)),
    }).then(({ verified }) => {
      setTransfers((current) => current.map((transfer) => transfer.id === id ? { ...transfer, status: 'complete', progress: 100, cancel: undefined } : transfer));
      notify(verified ? `${item.name} downloaded and verified` : `${item.name} download started`, 'success');
    }).catch((reason) => {
      const cancelled = reason instanceof DOMException && reason.name === 'AbortError';
      setTransfers((current) => current.map((transfer) => transfer.id === id ? {
        ...transfer, status: cancelled ? 'cancelled' : 'error', error: cancelled ? undefined : messageOf(reason), cancel: undefined,
      } : transfer));
      if (!cancelled) notify(`Download failed: ${messageOf(reason)}`, 'error', { label: 'Retry', run: retry });
    });
  }, [notify, settings.streamDownloads]);

  const beginUploads = useCallback((files: File[]) => {
    if (!files.length || !directoryRef.current) return;
    if (!canWrite) { setLoginOpen(true); return; }
    const destination = directoryRef.current.id;
    const jobs = files.map((file, index) => ({ file, id: `upload-${Date.now()}-${index}-${crypto.randomUUID?.() || index}`, cancelled: false, cancel: undefined as (() => void) | undefined }));
    setTransfers((current) => [
      ...jobs.map((job): Transfer => ({
        id: job.id, name: job.file.name, kind: 'upload', status: 'queued', progress: 0, loaded: 0, total: job.file.size, startedAt: Date.now(),
        cancel: () => { job.cancelled = true; job.cancel?.(); setTransfers((items) => items.map((item) => item.id === job.id ? { ...item, status: 'cancelled', cancel: undefined } : item)); },
        retry: () => beginUploads([job.file]),
      })),
      ...current,
    ]);
    let cursor = 0;
    const worker = async () => {
      while (cursor < jobs.length) {
        const job = jobs[cursor++];
        if (job.cancelled) continue;
        const upload = api.uploadFile(destination, job.file, (loaded, total) => {
          setTransfers((current) => current.map((transfer) => transfer.id === job.id ? {
            ...transfer, loaded, total, progress: total > 0 ? Math.min(100, loaded / total * 100) : 0,
          } : transfer));
        });
        job.cancel = upload.cancel;
        setTransfers((current) => current.map((transfer) => transfer.id === job.id ? { ...transfer, status: 'active', cancel: upload.cancel } : transfer));
        try {
          await upload.promise;
          setTransfers((current) => current.map((transfer) => transfer.id === job.id ? { ...transfer, status: 'complete', progress: 100, cancel: undefined } : transfer));
          notify(`${job.file.name} uploaded`, 'success');
        } catch (reason) {
          const cancelled = job.cancelled || (reason instanceof DOMException && reason.name === 'AbortError');
          setTransfers((current) => current.map((transfer) => transfer.id === job.id ? {
            ...transfer, status: cancelled ? 'cancelled' : 'error', error: cancelled ? undefined : messageOf(reason), cancel: undefined,
          } : transfer));
          if (reason instanceof ApiError && reason.status === 401) handleUnauthorized();
          else if (!cancelled) notify(`Upload failed: ${messageOf(reason)}`, 'error', { label: 'Retry', run: () => beginUploads([job.file]) });
        }
      }
    };
    void Promise.all(Array.from({ length: Math.min(settings.uploadConcurrency, jobs.length) }, worker)).then(() => {
      if (directoryRef.current?.id === destination) refresh();
    });
  }, [api, canWrite, handleUnauthorized, notify, refresh, settings.uploadConcurrency]);

  const submitName = async (name: string) => {
    if (!nameOperation || !directory) return;
    setNameBusy(true);
    setNameError('');
    try {
      if (nameOperation.mode === 'create') {
        await api.createDirectory(name, directory.id);
        notify(`Folder “${name}” created`, 'success');
      } else {
        await api.updateItem(nameOperation.item, nameOperation.item.parent || directory.id, { name });
        notify(`Renamed to “${name}”`, 'success');
      }
      setNameOperation(null);
      refresh();
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) handleUnauthorized();
      else setNameError(messageOf(reason));
    } finally {
      setNameBusy(false);
    }
  };

  const confirmDelete = async () => {
    if (!directory || !deleteItems.length) return;
    setDeleteBusy(true);
    setDeleteError('');
    const failures: string[] = [];
    for (const item of deleteItems) {
      try { await api.deleteItem(item, item.parent || directory.id); }
      catch (reason) {
        if (reason instanceof ApiError && reason.status === 401) { handleUnauthorized(); break; }
        failures.push(`${item.name}: ${messageOf(reason)}`);
      }
    }
    setDeleteBusy(false);
    refresh();
    if (failures.length) setDeleteError(`${failures.length} item${failures.length === 1 ? '' : 's'} could not be deleted. ${failures[0]}`);
    else {
      notify(`${deleteItems.length} item${deleteItems.length === 1 ? '' : 's'} deleted`, 'success');
      setDeleteItems([]);
      setSelected(new Set());
    }
  };

  const confirmMove = async (destination: string) => {
    if (!directory || !moveItems.length) return;
    setMoveBusy(true);
    setMoveError('');
    try {
      for (const item of moveItems) await api.updateItem(item, item.parent || directory.id, { parent: destination });
      notify(`${moveItems.length} item${moveItems.length === 1 ? '' : 's'} moved`, 'success');
      setMoveItems([]);
      setSelected(new Set());
      refresh();
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) handleUnauthorized();
      else setMoveError(messageOf(reason));
    } finally {
      setMoveBusy(false);
    }
  };

  const itemAction = (action: 'open' | 'preview' | 'download' | 'copy' | 'rename' | 'move' | 'details' | 'delete', item: FileItem) => {
    if (action === 'open') item.dir ? navigateTo(item.id, item.name) : isPreviewable(item) ? setPreviewItem(item) : beginDownload(item);
    if (action === 'preview') setPreviewItem(item);
    if (action === 'download') beginDownload(item);
    if (action === 'copy') void copyLink(item);
    if (action === 'rename') { setNameError(''); setNameOperation({ mode: 'rename', item }); }
    if (action === 'move') { setMoveError(''); setMoveItems([item]); }
    if (action === 'details') setDetailsItem(item);
    if (action === 'delete') { setDeleteError(''); setDeleteItems([item]); }
  };

  const login = async (username: string, password: string) => {
    setLoginBusy(true);
    setLoginError('');
    try {
      const token = await api.login(username, password);
      tokenRef.current = token;
      storeToken(token);
      setAuthenticated(true);
      setAuthRequired(false);
      setLoginOpen(false);
      notify('Signed in successfully', 'success');
      await loadDirectory(directoryRef.current?.id || 'root', { history: 'none' });
    } catch (reason) {
      setLoginError(messageOf(reason));
    } finally {
      setLoginBusy(false);
    }
  };

  const logout = () => {
    tokenRef.current = null;
    storeToken(null);
    setAuthenticated(false);
    setSettingsOpen(false);
    notify('Signed out');
    if (config.anonymous) void loadDirectory('root', { history: 'replace', trail: [{ id: 'root', name: 'My drive' }] });
    else { setAuthRequired(true); setLoginOpen(true); }
  };

  const savePreview = async (item: FileItem, content: string) => {
    if (!directory) return;
    await api.overwriteFile(item, item.parent || directory.id, content);
    notify(`${item.name} saved`, 'success');
    refresh();
  };

  const commands = useMemo<CommandItem[]>(() => {
    const values: CommandItem[] = [
      { id: 'search', label: 'Focus search', detail: 'Find an item in this folder', shortcut: '/', keywords: 'filter find', icon: <Search size={19} />, run: () => document.querySelector<HTMLInputElement>('[aria-label="Search this folder"]')?.focus() },
      { id: 'refresh', label: 'Refresh this folder', detail: 'Fetch the latest file list', shortcut: 'R', keywords: 'reload sync', icon: <RefreshCw size={19} />, run: refresh },
      { id: 'view', label: view === 'grid' ? 'Switch to list view' : 'Switch to grid view', detail: 'Change how items are arranged', shortcut: 'V', keywords: 'layout cards rows', icon: view === 'grid' ? <LayoutList size={19} /> : <Grid2X2 size={19} />, run: () => setView(view === 'grid' ? 'list' : 'grid') },
      { id: 'preferences', label: 'Open settings', detail: 'Theme, density and interface depth', shortcut: ',', keywords: 'settings appearance account', icon: <Settings2 size={19} />, run: () => setSettingsOpen(true) },
      { id: 'transfers', label: 'View transfer activity', detail: 'Uploads, downloads and errors', keywords: 'progress activity', icon: <Activity size={19} />, run: () => setTransfersOpen(true) },
      { id: 'theme', label: settings.theme === 'dark' ? 'Use light theme' : 'Use dark theme', detail: 'Quickly switch color mode', shortcut: 'T', keywords: 'appearance night day', icon: settings.theme === 'dark' ? <Sun size={19} /> : <MoonStar size={19} />, run: () => setSettings((current) => ({ ...current, theme: current.theme === 'dark' ? 'light' : 'dark' })) },
    ];
    if (canWrite) {
      values.splice(1, 0,
        { id: 'upload', label: 'Upload files', detail: 'Add files to this folder', shortcut: 'U', keywords: 'send add import', icon: <Upload size={19} />, run: () => document.querySelector<HTMLInputElement>('input[type="file"][multiple]')?.click() },
        { id: 'folder', label: 'Create a folder', detail: 'Organize this location', shortcut: 'N', keywords: 'directory new mkdir', icon: <FolderPlus size={19} />, run: () => { setNameError(''); setNameOperation({ mode: 'create' }); } },
      );
    }
    if (config.login) values.push(authenticated
      ? { id: 'logout', label: 'Sign out', detail: 'Return to guest access', keywords: 'account auth', icon: <LogOut size={19} />, run: logout }
      : { id: 'login', label: 'Sign in', detail: 'Unlock editing and private files', keywords: 'account auth', icon: <LogIn size={19} />, run: () => setLoginOpen(true) });
    return values;
  }, [authenticated, canWrite, config.login, refresh, settings.theme, view]);

  useEffect(() => {
    const shortcuts = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); setCommandsOpen(true); return; }
      if (isTypingTarget(event.target) || document.querySelector('dialog[open]')) return;
      const key = event.key.toLowerCase();
      if (event.key === '?' || ((event.ctrlKey || event.metaKey) && key === '/')) { event.preventDefault(); setCommandsOpen(true); }
      else if (key === '/') { event.preventDefault(); document.querySelector<HTMLInputElement>('[aria-label="Search this folder"]')?.focus(); }
      else if (key === 'r') refresh();
      else if (key === 'v') setView((current) => current === 'grid' ? 'list' : 'grid');
      else if (key === 'n' && canWrite) { setNameError(''); setNameOperation({ mode: 'create' }); }
      else if (key === 'u' && canWrite) document.querySelector<HTMLInputElement>('input[type="file"][multiple]')?.click();
      else if (event.key === 'Escape' && selected.size) setSelected(new Set());
    };
    document.addEventListener('keydown', shortcuts);
    return () => document.removeEventListener('keydown', shortcuts);
  }, [canWrite, refresh, selected.size]);

  return (
    <>
      <Workspace
        directory={directory}
        breadcrumbs={breadcrumbs}
        items={visibleItems}
        loading={loading}
        error={loadError}
        authRequired={authRequired}
        authConfig={config}
        authenticated={authenticated}
        canWrite={canWrite}
        online={online}
        query={query}
        onQuery={setQuery}
        filter={filter}
        onFilter={setFilter}
        sortKey={sortKey}
        descending={descending}
        onSort={(key, desc) => { setSortKey(key); setDescending(desc); }}
        view={view}
        onView={setView}
        complexity={settings.complexity}
        selected={selected}
        activeTransfers={activeTransfers}
        onNavigate={navigateTo}
        onItemAction={itemAction}
        onToggleSelected={toggleSelected}
        onClearSelected={() => setSelected(new Set())}
        onBulkDelete={(items) => { setDeleteError(''); setDeleteItems(items); }}
        onBulkMove={(items) => { setMoveError(''); setMoveItems(items); }}
        onUpload={beginUploads}
        onNewFolder={() => { setNameError(''); setNameOperation({ mode: 'create' }); }}
        onOpenSettings={() => setSettingsOpen(true)}
        onOpenCommands={() => setCommandsOpen(true)}
        onOpenTransfers={() => setTransfersOpen(true)}
        onSignIn={() => { setLoginError(''); setLoginOpen(true); }}
        onRefresh={refresh}
      />

      <LoginDialog open={loginOpen} busy={loginBusy} error={loginError} required={authRequired && !config.anonymous} onClose={() => setLoginOpen(false)} onSubmit={(username, password) => void login(username, password)} />
      <NameDialog open={Boolean(nameOperation)} mode={nameOperation?.mode || 'create'} item={nameOperation?.mode === 'rename' ? nameOperation.item : null} busy={nameBusy} serverError={nameError} onClose={() => setNameOperation(null)} onSubmit={(name) => void submitName(name)} />
      <DeleteDialog open={deleteItems.length > 0} items={deleteItems} busy={deleteBusy} error={deleteError} onClose={() => setDeleteItems([])} onConfirm={() => void confirmDelete()} />
      <MoveDialog open={moveItems.length > 0} api={api} items={moveItems} busy={moveBusy} error={moveError} onClose={() => setMoveItems([])} onMove={(destination) => void confirmMove(destination)} />
      <PreviewDialog open={Boolean(previewItem)} item={previewItem} canWrite={canWrite} onClose={() => setPreviewItem(null)} onDownload={beginDownload} onSave={savePreview} />
      <DetailsPanel
        open={Boolean(detailsItem)} item={detailsItem} breadcrumbs={breadcrumbs} canWrite={canWrite} onClose={() => setDetailsItem(null)}
        onOpen={() => { if (detailsItem) itemAction('open', detailsItem); setDetailsItem(null); }}
        onDownload={() => detailsItem && beginDownload(detailsItem)} onCopy={() => detailsItem && void copyLink(detailsItem)}
        onRename={() => { if (detailsItem) { setNameOperation({ mode: 'rename', item: detailsItem }); setDetailsItem(null); } }}
      />
      <SettingsPanel
        open={settingsOpen} settings={settings} config={config} authenticated={authenticated} onChange={setSettings} onClose={() => setSettingsOpen(false)}
        onLogin={() => { setSettingsOpen(false); setLoginOpen(true); }} onLogout={logout}
        onOpenTransfers={() => { setSettingsOpen(false); setTransfersOpen(true); }}
      />
      <TransferCenter open={transfersOpen} transfers={transfers} onClose={() => setTransfersOpen(false)} onClear={() => setTransfers((current) => current.filter((transfer) => transfer.status === 'active' || transfer.status === 'queued'))} onRemove={(id) => setTransfers((current) => current.filter((transfer) => transfer.id !== id))} />
      <CommandPalette open={commandsOpen} commands={commands} onClose={() => setCommandsOpen(false)} />
      <TransferToast transfers={transfers} onOpen={() => setTransfersOpen(true)} />
      <ToastStack toasts={toasts} onDismiss={(id) => setToasts((current) => current.filter((toast) => toast.id !== id))} />
    </>
  );
}
