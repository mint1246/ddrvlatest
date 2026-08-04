import { useEffect } from 'react';
import {
  Activity,
  Check,
  ChevronRight,
  Cloud,
  Gauge,
  LogIn,
  LogOut,
  Monitor,
  Moon,
  Palette,
  Settings2,
  Sparkles,
  Sun,
  X,
} from 'lucide-react';
import type { Accent, AppSettings, AuthConfig, Complexity, Density, ThemeMode } from '../types';
import { Button, IconButton, Segmented, Switch } from './Primitives';

export function SettingsPanel({ open, settings, config, authenticated, onChange, onClose, onLogin, onLogout, onOpenTransfers }: {
  open: boolean;
  settings: AppSettings;
  config: AuthConfig;
  authenticated: boolean;
  onChange: (settings: AppSettings) => void;
  onClose: () => void;
  onLogin: () => void;
  onLogout: () => void;
  onOpenTransfers: () => void;
}) {
  useEffect(() => {
    if (!open) return;
    const close = (event: KeyboardEvent) => { if (event.key === 'Escape') onClose(); };
    document.addEventListener('keydown', close);
    return () => document.removeEventListener('keydown', close);
  }, [open, onClose]);

  const update = <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => onChange({ ...settings, [key]: value });

  return (
    <div className={`sheet-layer${open ? ' open' : ''}`} aria-hidden={!open}>
      <button className="sheet-scrim" aria-label="Close preferences" onClick={onClose} tabIndex={open ? 0 : -1} />
      <aside className="settings-sheet" role="dialog" aria-modal="true" aria-labelledby="settings-title">
        <header className="sheet-header">
          <div className="sheet-icon"><Settings2 size={22} /></div>
          <div><h2 id="settings-title">Settings</h2></div>
          <IconButton label="Close preferences" onClick={onClose}><X size={20} /></IconButton>
        </header>

        <div className="sheet-scroll">
          <section className="account-card">
            <div className={`account-avatar${authenticated ? ' authenticated' : ''}`}>
              {authenticated ? <Check size={23} /> : <Cloud size={23} />}
            </div>
            <div>
              <strong>{authenticated ? 'Signed in' : config.login ? (config.anonymous ? 'Guest workspace' : 'Sign-in required') : 'Open workspace'}</strong>
              <span>{authenticated ? 'Private files and editing enabled' : config.login ? (config.anonymous ? 'Browsing in read-only mode' : 'Authenticate to browse files') : 'No authentication configured'}</span>
            </div>
            {config.login && (
              authenticated
                ? <Button tone="text" icon={<LogOut size={17} />} onClick={onLogout}>Sign out</Button>
                : <Button tone="filled" icon={<LogIn size={17} />} onClick={onLogin}>Sign in</Button>
            )}
          </section>

          <button className="settings-link" onClick={onOpenTransfers}>
            <span className="settings-link-icon"><Activity size={20} /></span>
            <span><strong>Transfer activity</strong><small>Uploads, downloads, progress and errors</small></span>
            <ChevronRight size={19} />
          </button>

          <section className="settings-section">
            <div className="settings-section-title"><Palette size={18} /><span><strong>Appearance</strong><small>Theme and color personality</small></span></div>
            <Segmented<ThemeMode>
              label="Theme"
              value={settings.theme}
              onChange={(value) => update('theme', value)}
              options={[
                { value: 'system', label: 'System', icon: <Monitor size={15} /> },
                { value: 'light', label: 'Light', icon: <Sun size={15} /> },
                { value: 'dark', label: 'Dark', icon: <Moon size={15} /> },
              ]}
            />
            <div className="accent-picker" role="radiogroup" aria-label="Accent color">
              {([
                ['lagoon', 'Lagoon', '#006b61', '#65dbc9'],
                ['violet', 'Violet', '#6750a4', '#d0bcff'],
                ['sunset', 'Sunset', '#925322', '#ffb68a'],
              ] as Array<[Accent, string, string, string]>).map(([value, label, first, second]) => (
                <button key={value} role="radio" aria-checked={settings.accent === value} className={settings.accent === value ? 'active' : ''} onClick={() => update('accent', value)}>
                  <span className="accent-swatch" style={{ '--swatch-a': first, '--swatch-b': second } as React.CSSProperties} />
                  <span>{label}</span>
                  {settings.accent === value && <Check size={16} />}
                </button>
              ))}
            </div>
          </section>

          <section className="settings-section">
            <div className="settings-section-title"><Sparkles size={18} /><span><strong>Interface depth</strong><small>Choose how much information is visible</small></span></div>
            <div className="complexity-picker" role="radiogroup" aria-label="Interface depth">
              {([
                ['calm', 'Calm', 'Essentials only', 'A distraction-free file list with simplified controls.'],
                ['balanced', 'Balanced', 'Recommended', 'Helpful filters and metadata without visual noise.'],
                ['power', 'Power', 'Everything at hand', 'Denser information, shortcuts and advanced controls.'],
              ] as Array<[Complexity, string, string, string]>).map(([value, label, badge, description]) => (
                <button key={value} role="radio" aria-checked={settings.complexity === value} className={settings.complexity === value ? 'active' : ''} onClick={() => update('complexity', value)}>
                  <span className="complexity-check">{settings.complexity === value && <Check size={15} />}</span>
                  <span><strong>{label}<em>{badge}</em></strong><small>{description}</small></span>
                </button>
              ))}
            </div>
            <Segmented<Density>
              label="Item density"
              value={settings.density}
              onChange={(value) => update('density', value)}
              options={[
                { value: 'comfortable', label: 'Comfortable' },
                { value: 'compact', label: 'Compact' },
              ]}
            />
          </section>

          <section className="settings-section">
            <div className="settings-section-title"><Gauge size={18} /><span><strong>Behavior</strong><small>Performance and motion</small></span></div>
            <label className="slider-row">
              <span><strong>Parallel uploads</strong><small>{settings.uploadConcurrency} at a time</small></span>
              <input type="range" min="1" max="4" step="1" value={settings.uploadConcurrency} onChange={(event) => update('uploadConcurrency', Number(event.target.value) as AppSettings['uploadConcurrency'])} />
            </label>
            <Switch checked={settings.streamDownloads} onChange={(checked) => update('streamDownloads', checked)} label="Stream downloads to disk" description="Uses less memory when your browser supports it." />
            <Switch checked={settings.reducedMotion} onChange={(checked) => update('reducedMotion', checked)} label="Reduce interface motion" description="Turns off expressive transitions and reveal effects." />
          </section>
        </div>
      </aside>
    </div>
  );
}
