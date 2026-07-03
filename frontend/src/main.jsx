import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  CirclePause,
  CirclePlay,
  Database,
  LogOut,
  RefreshCcw,
  Search,
  ShieldCheck,
  User,
  Wifi
} from 'lucide-react';
import './styles.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';
const POLL_INTERVAL_MS = 5000;
const AUTH_STORAGE_KEY = 'replicator.auth';

function authHeader(auth) {
  if (!auth) return {};
  return { Authorization: `Basic ${auth.token}` };
}

async function api(path, options = {}, auth) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      Accept: 'application/json',
      ...authHeader(auth),
      ...options.headers
    },
    ...options
  });

  if (response.status === 401) {
    throw new Error('Unauthorized');
  }

  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `${response.status} ${response.statusText}`);
  }

  return response.json();
}

function formatDate(value) {
  if (!value) return 'Never';
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium'
  }).format(new Date(value));
}

function formatLag(value) {
  if (value === null || value === undefined) return 'Unknown';
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(1)} s`;
}

function statusLabel(status) {
  if (!status) return { label: 'Offline', tone: 'danger' };
  if (status.error) return { label: 'Error', tone: 'danger' };
  if (status.paused) return { label: 'Paused', tone: 'warning' };
  if (status.running) return { label: 'Running', tone: 'success' };
  return { label: 'Stopped', tone: 'neutral' };
}

const buttonClass =
  'inline-flex min-h-10 items-center justify-center gap-2 rounded-md border border-blue-600 bg-blue-600 px-4 text-sm font-bold text-white shadow-sm transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50';
const secondaryButtonClass =
  'inline-flex min-h-10 items-center justify-center gap-2 rounded-md border border-blue-200 bg-white px-4 text-sm font-bold text-blue-700 shadow-sm transition hover:border-blue-300 hover:bg-blue-50 disabled:cursor-not-allowed disabled:opacity-50';
const panelClass = 'rounded-lg border border-blue-100 bg-white p-5 shadow-[0_18px_45px_rgba(29,78,216,0.08)]';
const eyebrowClass = 'mb-1.5 text-xs font-extrabold uppercase tracking-normal text-blue-500';
const inputClass =
  'min-h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-500 focus:ring-4 focus:ring-blue-100';
const labelClass = 'grid gap-1.5 text-sm font-extrabold text-slate-700';

const statusToneClasses = {
  success: 'border-emerald-200 bg-emerald-50 text-emerald-700 [&>span]:bg-emerald-500',
  warning: 'border-amber-200 bg-amber-50 text-amber-700 [&>span]:bg-amber-500',
  danger: 'border-rose-200 bg-rose-50 text-rose-700 [&>span]:bg-rose-500',
  neutral: 'border-slate-200 bg-white text-slate-600 [&>span]:bg-slate-400'
};

function StatCard({ icon: Icon, label, value, detail }) {
  return (
    <section className="flex min-h-32 gap-3 rounded-lg border border-blue-100 bg-white p-4 shadow-[0_18px_45px_rgba(29,78,216,0.08)]">
      <div className="flex h-10 w-10 flex-none items-center justify-center rounded-md bg-blue-50 text-blue-600">
        <Icon size={20} />
      </div>
      <div className="min-w-0">
        <p className="mb-2 text-sm text-slate-500">{label}</p>
        <strong className="block text-lg leading-tight text-slate-950 [overflow-wrap:anywhere]">{value}</strong>
        {detail ? <span className="mt-2 block text-sm text-slate-500">{detail}</span> : null}
      </div>
    </section>
  );
}

function KeyValue({ label, value }) {
  return (
    <div className="grid gap-2 border-b border-blue-50 pb-2.5 sm:grid-cols-[150px_minmax(0,1fr)]">
      <span className="text-slate-500">{label}</span>
      <strong className="text-slate-900 [overflow-wrap:anywhere]">{value || '-'}</strong>
    </div>
  );
}

function App() {
  const [health, setHealth] = useState(null);
  const [status, setStatus] = useState(null);
  const [verifyResult, setVerifyResult] = useState(null);
  const [verifySummary, setVerifySummary] = useState(null);
  const [auth, setAuth] = useState(() => {
    const stored = window.sessionStorage.getItem(AUTH_STORAGE_KEY);
    return stored ? JSON.parse(stored) : null;
  });
  const [loginForm, setLoginForm] = useState({ username: '', password: '' });
  const [tableName, setTableName] = useState('');
  const [limit, setLimit] = useState('');
  const [loading, setLoading] = useState(true);
  const [loginLoading, setLoginLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [verifyLoading, setVerifyLoading] = useState(false);
  const [verifyAllLoading, setVerifyAllLoading] = useState(false);
  const [error, setError] = useState('');

  const currentStatus = useMemo(() => statusLabel(status), [status]);

  const logout = useCallback(() => {
    window.sessionStorage.removeItem(AUTH_STORAGE_KEY);
    setAuth(null);
    setStatus(null);
    setVerifyResult(null);
    setVerifySummary(null);
  }, []);

  const loadStatus = useCallback(async ({ quiet = false } = {}) => {
    if (!quiet) setLoading(true);
    try {
      const healthResponse = await api('/health');
      setHealth(healthResponse);
      if (!auth) {
        setError('');
        return;
      }
      const statusResponse = await api('/replication/status', {}, auth);
      setStatus(statusResponse);
      setError('');
    } catch (err) {
      if (err.message === 'Unauthorized') {
        logout();
      }
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [auth, logout]);

  useEffect(() => {
    loadStatus();
    const timer = window.setInterval(() => loadStatus({ quiet: true }), POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [loadStatus]);

  async function login(event) {
    event.preventDefault();
    setLoginLoading(true);
    try {
      const nextAuth = {
        username: loginForm.username.trim(),
        token: window.btoa(`${loginForm.username.trim()}:${loginForm.password}`)
      };
      const me = await api('/auth/me', {}, nextAuth);
      const authenticated = { ...nextAuth, username: me.username };
      window.sessionStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(authenticated));
      setAuth(authenticated);
      setLoginForm({ username: '', password: '' });
      setError('');
    } catch (err) {
      setError(err.message === 'Unauthorized' ? 'Invalid username or password' : err.message);
    } finally {
      setLoginLoading(false);
    }
  }

  async function changeReplicationState(action) {
    setActionLoading(true);
    try {
      await api(`/replication/${action}`, { method: 'POST' }, auth);
      await loadStatus({ quiet: true });
    } catch (err) {
      if (err.message === 'Unauthorized') {
        logout();
      }
      setError(err.message);
    } finally {
      setActionLoading(false);
    }
  }

  async function verifyTable(event) {
    event.preventDefault();
    if (!tableName.trim()) return;

    const params = new URLSearchParams({ table: tableName.trim() });
    if (limit) params.set('limit', limit);

    setVerifyLoading(true);
    try {
      const result = await api(`/replication/verify?${params.toString()}`, {}, auth);
      setVerifyResult(result);
      setError('');
    } catch (err) {
      if (err.message === 'Unauthorized') {
        logout();
      }
      setError(err.message);
    } finally {
      setVerifyLoading(false);
    }
  }

  async function verifyAllTables() {
    const params = new URLSearchParams();
    if (limit) params.set('limit', limit);

    setVerifyAllLoading(true);
    try {
      const query = params.toString();
      const result = await api(`/replication/verify/all${query ? `?${query}` : ''}`, {}, auth);
      setVerifySummary(result);
      setError('');
    } catch (err) {
      if (err.message === 'Unauthorized') {
        logout();
      }
      setError(err.message);
    } finally {
      setVerifyAllLoading(false);
    }
  }

  return (
    <main className="mx-auto max-w-[1180px] px-5 py-6 text-slate-900 sm:px-7">
      <header className="mb-5 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <p className={eyebrowClass}>ThinkVitals</p>
          <h1 className="m-0 text-4xl font-black leading-none text-slate-950 sm:text-5xl">MySQL Replicator</h1>
        </div>
        <div
          className={`inline-flex min-h-10 items-center gap-2 self-start rounded-full border px-4 text-sm font-extrabold sm:self-auto ${statusToneClasses[currentStatus.tone]}`}
        >
          <span className="block h-2.5 w-2.5 rounded-full" />
          {currentStatus.label}
        </div>
      </header>

      {error ? (
        <section
          className="mb-4 flex items-center gap-2.5 rounded-md border border-rose-200 bg-rose-50 px-3.5 py-3 text-rose-800"
          role="alert"
        >
          <AlertTriangle size={18} />
          <span>{error}</span>
        </section>
      ) : null}

      {!auth ? (
        <section className="grid min-h-[54vh] place-items-center">
          <article className={`${panelClass} w-full max-w-[430px]`}>
            <div className="mb-5 flex items-center justify-between">
              <div>
                <p className={eyebrowClass}>Authorized Access</p>
                <h2 className="m-0 text-lg font-extrabold text-slate-950">Login to control replication</h2>
              </div>
              <div className="flex h-10 w-10 flex-none items-center justify-center rounded-md bg-blue-50 text-blue-600">
                <User size={20} />
              </div>
            </div>
            <form className="grid gap-3.5" onSubmit={login}>
              <label className={labelClass}>
                Username
                <input
                  className={inputClass}
                  autoComplete="username"
                  value={loginForm.username}
                  onChange={(event) =>
                    setLoginForm((current) => ({ ...current, username: event.target.value }))
                  }
                  placeholder="admin"
                />
              </label>
              <label className={labelClass}>
                Password
                <input
                  className={inputClass}
                  autoComplete="current-password"
                  type="password"
                  value={loginForm.password}
                  onChange={(event) =>
                    setLoginForm((current) => ({ ...current, password: event.target.value }))
                  }
                  placeholder="admin123"
                />
              </label>
              <button
                className={buttonClass}
                type="submit"
                disabled={loginLoading || !loginForm.username.trim() || !loginForm.password}
              >
                <CirclePlay size={18} />
                Login
              </button>
            </form>
          </article>
        </section>
      ) : (
        <>
          <section className="mb-3.5 flex flex-col gap-4 rounded-lg border border-blue-100 bg-white px-4 py-3 shadow-sm sm:flex-row sm:items-center sm:justify-between">
            <div>
              <span className="mb-0.5 block text-sm text-slate-500">Signed in as</span>
              <strong className="block text-slate-950">{auth.username}</strong>
            </div>
            <button type="button" className={secondaryButtonClass} onClick={logout}>
              <LogOut size={18} />
              Logout
            </button>
          </section>

          <section className="mb-5 flex flex-wrap gap-2.5">
            <button className={buttonClass} type="button" onClick={() => loadStatus()} disabled={loading}>
              <RefreshCcw size={18} />
              Refresh
            </button>
            <button
              type="button"
              className={secondaryButtonClass}
              onClick={() => changeReplicationState('pause')}
              disabled={actionLoading || status?.paused}
            >
              <CirclePause size={18} />
              Pause
            </button>
            <button
              className={buttonClass}
              type="button"
              onClick={() => changeReplicationState('resume')}
              disabled={actionLoading || status?.running}
            >
              <CirclePlay size={18} />
              Run Sync
            </button>
          </section>

          <section className="mb-3.5 grid gap-3.5 lg:grid-cols-4" aria-label="Replication summary">
            <StatCard
              icon={Wifi}
              label="Backend Health"
              value={health?.status ?? 'Unknown'}
              detail={loading ? 'Refreshing' : 'Live polling'}
            />
            <StatCard
              icon={Database}
              label="Databases"
              value={`${status?.sourceDatabase ?? '-'} -> ${status?.sinkDatabase ?? '-'}`}
              detail="Source to sink"
            />
            <StatCard
              icon={Activity}
              label="Lag"
              value={formatLag(status?.lagMs)}
              detail={`Last applied ${formatDate(status?.lastAppliedTime)}`}
            />
            <StatCard
              icon={ShieldCheck}
              label="Checkpoint"
              value={status?.checkpointPresent ? 'Found' : 'Missing'}
              detail={
                status?.checkpointSourceDatabase
                  ? `Source ${status.checkpointSourceDatabase}`
                  : 'No checkpoint row'
              }
            />
          </section>

          <section className="grid gap-3.5 lg:grid-cols-[minmax(0,1fr)_minmax(340px,0.9fr)]">
            <article className={panelClass}>
              <div className="mb-5 flex items-center justify-between">
                <div>
                  <p className={eyebrowClass}>Position</p>
                  <h2 className="m-0 text-lg font-extrabold text-slate-950">Binlog Checkpoint</h2>
                </div>
              </div>
              <div className="grid gap-2.5">
                <KeyValue label="File" value={status?.binlogFile} />
                <KeyValue label="Position" value={status?.binlogPosition?.toLocaleString()} />
                <KeyValue label="GTID" value={status?.gtidSet} />
                <KeyValue label="Last source event" value={formatDate(status?.lastEventTime)} />
                <KeyValue label="Error" value={status?.error || 'None'} />
              </div>
            </article>

            <article className={panelClass}>
              <div className="mb-5 flex items-center justify-between">
                <div>
                  <p className={eyebrowClass}>Verification</p>
                  <h2 className="m-0 text-lg font-extrabold text-slate-950">Compare Table</h2>
                </div>
              </div>
              <form className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_120px_auto]" onSubmit={verifyTable}>
                <label className={labelClass}>
                  Table
                  <input
                    className={inputClass}
                    value={tableName}
                    onChange={(event) => setTableName(event.target.value)}
                    placeholder="user_model"
                  />
                </label>
                <label className={labelClass}>
                  Limit
                  <input
                    className={inputClass}
                    type="number"
                    min="1"
                    value={limit}
                    onChange={(event) => setLimit(event.target.value)}
                    placeholder="Optional"
                  />
                </label>
                <button className={`${buttonClass} self-end`} type="submit" disabled={verifyLoading || !tableName.trim()}>
                  <Search size={18} />
                  Verify
                </button>
              </form>
              <div className="mt-3 flex justify-stretch lg:justify-end">
                <button
                  type="button"
                  className={`${secondaryButtonClass} w-full lg:w-auto`}
                  onClick={verifyAllTables}
                  disabled={verifyAllLoading}
                >
                  <ShieldCheck size={18} />
                  Verify All
                </button>
              </div>

              {verifyResult ? (
                <div
                  className={`mt-4 rounded-lg border p-3.5 ${
                    verifyResult.matched
                      ? 'border-emerald-200 bg-emerald-50/50'
                      : 'border-rose-200 bg-rose-50/50'
                  }`}
                >
                  <div
                    className={`mb-3.5 flex items-center gap-2 ${
                      verifyResult.matched ? 'text-emerald-700' : 'text-rose-700'
                    }`}
                  >
                    {verifyResult.matched ? (
                      <CheckCircle2 size={20} />
                    ) : (
                      <AlertTriangle size={20} />
                    )}
                    <strong>{verifyResult.matched ? 'Matched' : 'Mismatch'}</strong>
                  </div>
                  <div className="grid gap-2">
                    <KeyValue label="Table" value={verifyResult.table} />
                    <KeyValue label="Comparable" value={verifyResult.comparable ? 'Yes' : 'No'} />
                    <KeyValue label="Source rows" value={verifyResult.sourceRows?.toLocaleString()} />
                    <KeyValue label="Sink rows" value={verifyResult.sinkRows?.toLocaleString()} />
                    <KeyValue label="Verified at" value={formatDate(verifyResult.verifiedAt)} />
                  </div>
                  {verifyResult.note ? <p className="mt-3 text-slate-500">{verifyResult.note}</p> : null}
                </div>
              ) : null}

              {verifySummary ? (
                <div
                  className={`mt-4 rounded-lg border p-3.5 ${
                    verifySummary.matched
                      ? 'border-emerald-200 bg-emerald-50/50'
                      : 'border-rose-200 bg-rose-50/50'
                  }`}
                >
                  <div
                    className={`mb-3.5 flex items-center gap-2 ${
                      verifySummary.matched ? 'text-emerald-700' : 'text-rose-700'
                    }`}
                  >
                    {verifySummary.matched ? (
                      <CheckCircle2 size={20} />
                    ) : (
                      <AlertTriangle size={20} />
                    )}
                    <strong>{verifySummary.matched ? 'All tables matched' : 'Integrity issue found'}</strong>
                  </div>
                  <div className="grid gap-2">
                    <KeyValue label="Total tables" value={verifySummary.totalTables?.toLocaleString()} />
                    <KeyValue label="Matched" value={verifySummary.matchedTables?.toLocaleString()} />
                    <KeyValue label="Mismatched" value={verifySummary.mismatchedTables?.toLocaleString()} />
                    <KeyValue
                      label="Not comparable"
                      value={verifySummary.notComparableTables?.toLocaleString()}
                    />
                    <KeyValue label="Verified at" value={formatDate(verifySummary.verifiedAt)} />
                  </div>
                  {verifySummary.tables?.some((table) => !table.matched || !table.comparable) ? (
                    <div className="mt-3.5 grid gap-2.5 border-t border-blue-100 pt-3">
                      {verifySummary.tables
                        .filter((table) => !table.matched || !table.comparable)
                        .slice(0, 8)
                        .map((table) => (
                          <div className="grid gap-1" key={table.table}>
                            <strong className="text-slate-950">{table.table}</strong>
                            <span className="text-sm text-slate-500 [overflow-wrap:anywhere]">{table.note}</span>
                          </div>
                        ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </article>
          </section>
        </>
      )}
    </main>
  );
}

createRoot(document.getElementById('root')).render(<App />);
