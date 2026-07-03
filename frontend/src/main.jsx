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

function StatCard({ icon: Icon, label, value, detail }) {
  return (
    <section className="stat-card">
      <div className="stat-icon">
        <Icon size={20} />
      </div>
      <div>
        <p>{label}</p>
        <strong>{value}</strong>
        {detail ? <span>{detail}</span> : null}
      </div>
    </section>
  );
}

function KeyValue({ label, value }) {
  return (
    <div className="kv-row">
      <span>{label}</span>
      <strong>{value || '-'}</strong>
    </div>
  );
}

function App() {
  const [health, setHealth] = useState(null);
  const [status, setStatus] = useState(null);
  const [verifyResult, setVerifyResult] = useState(null);
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
  const [error, setError] = useState('');

  const currentStatus = useMemo(() => statusLabel(status), [status]);

  const logout = useCallback(() => {
    window.sessionStorage.removeItem(AUTH_STORAGE_KEY);
    setAuth(null);
    setStatus(null);
    setVerifyResult(null);
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

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">ThinkVitals</p>
          <h1>MySQL Replicator</h1>
        </div>
        <div className={`status-pill ${currentStatus.tone}`}>
          <span />
          {currentStatus.label}
        </div>
      </header>

      {error ? (
        <section className="alert" role="alert">
          <AlertTriangle size={18} />
          <span>{error}</span>
        </section>
      ) : null}

      {!auth ? (
        <section className="login-layout">
          <article className="panel login-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Authorized Access</p>
                <h2>Login to control replication</h2>
              </div>
              <div className="stat-icon">
                <User size={20} />
              </div>
            </div>
            <form className="login-form" onSubmit={login}>
              <label>
                Username
                <input
                  autoComplete="username"
                  value={loginForm.username}
                  onChange={(event) =>
                    setLoginForm((current) => ({ ...current, username: event.target.value }))
                  }
                  placeholder="admin"
                />
              </label>
              <label>
                Password
                <input
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
          <section className="session-bar">
            <div>
              <span>Signed in as</span>
              <strong>{auth.username}</strong>
            </div>
            <button type="button" className="secondary" onClick={logout}>
              <LogOut size={18} />
              Logout
            </button>
          </section>

          <section className="control-bar">
            <button type="button" onClick={() => loadStatus()} disabled={loading}>
              <RefreshCcw size={18} />
              Refresh
            </button>
            <button
              type="button"
              className="secondary"
              onClick={() => changeReplicationState('pause')}
              disabled={actionLoading || status?.paused}
            >
              <CirclePause size={18} />
              Pause
            </button>
            <button
              type="button"
              onClick={() => changeReplicationState('resume')}
              disabled={actionLoading || status?.running}
            >
              <CirclePlay size={18} />
              Run Sync
            </button>
          </section>

          <section className="stats-grid" aria-label="Replication summary">
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
              label="Last Event"
              value={status?.lastEventType ?? 'None'}
              detail={status?.lastTableName ?? 'No table yet'}
            />
          </section>

          <section className="content-grid">
            <article className="panel">
              <div className="panel-heading">
                <div>
                  <p className="eyebrow">Position</p>
                  <h2>Binlog Checkpoint</h2>
                </div>
              </div>
              <div className="kv-list">
                <KeyValue label="File" value={status?.binlogFile} />
                <KeyValue label="Position" value={status?.binlogPosition?.toLocaleString()} />
                <KeyValue label="GTID" value={status?.gtidSet} />
                <KeyValue label="Last source event" value={formatDate(status?.lastEventTime)} />
                <KeyValue label="Error" value={status?.error || 'None'} />
              </div>
            </article>

            <article className="panel">
              <div className="panel-heading">
                <div>
                  <p className="eyebrow">Verification</p>
                  <h2>Compare Table</h2>
                </div>
              </div>
              <form className="verify-form" onSubmit={verifyTable}>
                <label>
                  Table
                  <input
                    value={tableName}
                    onChange={(event) => setTableName(event.target.value)}
                    placeholder="user_model"
                  />
                </label>
                <label>
                  Limit
                  <input
                    type="number"
                    min="1"
                    value={limit}
                    onChange={(event) => setLimit(event.target.value)}
                    placeholder="Optional"
                  />
                </label>
                <button type="submit" disabled={verifyLoading || !tableName.trim()}>
                  <Search size={18} />
                  Verify
                </button>
              </form>

              {verifyResult ? (
                <div className={`verification ${verifyResult.matched ? 'success' : 'danger'}`}>
                  <div>
                    {verifyResult.matched ? (
                      <CheckCircle2 size={20} />
                    ) : (
                      <AlertTriangle size={20} />
                    )}
                    <strong>{verifyResult.matched ? 'Matched' : 'Mismatch'}</strong>
                  </div>
                  <div className="kv-list compact">
                    <KeyValue label="Table" value={verifyResult.table} />
                    <KeyValue label="Comparable" value={verifyResult.comparable ? 'Yes' : 'No'} />
                    <KeyValue label="Source rows" value={verifyResult.sourceRows?.toLocaleString()} />
                    <KeyValue label="Sink rows" value={verifyResult.sinkRows?.toLocaleString()} />
                    <KeyValue label="Verified at" value={formatDate(verifyResult.verifiedAt)} />
                  </div>
                  {verifyResult.note ? <p className="note">{verifyResult.note}</p> : null}
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
