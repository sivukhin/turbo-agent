import React, { useState, useEffect, useRef, useCallback } from 'react';
import Markdown from 'react-markdown';

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function ExecList({ executions, currentId, onSelect }) {
  return (
    <div className="space-y-1">
      {executions.map(e => {
        const active = e.execution_id === currentId;
        const statusCls = e.finished ? 'bg-gray-100 text-gray-500' : 'bg-emerald-50 text-emerald-700';
        const statusText = e.finished ? 'done' : 'running';
        return (
          <div key={e.execution_id}
            className={`px-3 py-2 rounded-md cursor-pointer transition border ${active ? 'bg-blue-50 border-blue-200' : 'hover:bg-gray-100 border-transparent'}`}
            onClick={() => onSelect(e.execution_id)}>
            <div className="flex items-center justify-between">
              <span className="font-medium text-gray-800 text-sm">{e.workflow}</span>
              <span className={`text-xs px-1.5 py-0.5 rounded ${statusCls}`}>{statusText}</span>
            </div>
            <div className="text-xs text-gray-400 mt-0.5 font-mono">{e.execution_id.substring(0, 12)}</div>
          </div>
        );
      })}
    </div>
  );
}

function hasLabel(m, label) {
  const labels = (m.meta && m.meta.labels) || '';
  return labels.split(',').includes(label);
}

function renderContent(content) {
  if (typeof content === 'string') {
    try {
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        return parsed.map((block, i) => {
          if (block.type === 'text') return <div key={i} className="whitespace-pre-wrap break-words">{block.text}</div>;
          if (block.type === 'tool_use') return (
            <div key={i} className="mt-1 bg-gray-50 border border-gray-200 rounded p-2 text-xs font-mono">
              <span className="text-purple-600 font-medium">{block.name}</span>
              <span className="text-gray-400 ml-2">{block.id}</span>
              <JsonTree data={block.input} />
            </div>
          );
          if (block.type === 'tool_result') return (
            <div key={i} className="mt-1 bg-gray-50 border border-gray-200 rounded p-2 text-xs font-mono">
              <span className="text-teal-600 font-medium">result</span>
              <span className="text-gray-400 ml-2">{block.tool_use_id}</span>
              <div className="whitespace-pre-wrap mt-1">{typeof block.content === 'string' ? block.content : JSON.stringify(block.content)}</div>
            </div>
          );
          return <div key={i} className="text-xs text-gray-500">{JSON.stringify(block)}</div>;
        });
      }
    } catch {}
    return content;
  }
  return String(content);
}

function ToolUseMessage({ content }) {
  try {
    const data = JSON.parse(content);
    return (
      <div className="bg-purple-50 border border-purple-200 rounded-md p-2 text-xs font-mono">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-purple-700 font-semibold">{data.name}</span>
          <span className="text-purple-300">{data.id}</span>
        </div>
        <div className="bg-white rounded p-1.5 border border-purple-100">
          <JsonTree data={data.input} />
        </div>
      </div>
    );
  } catch { return <div className="whitespace-pre-wrap text-xs">{content}</div>; }
}

function ToolResultMessage({ content }) {
  try {
    const data = JSON.parse(content);
    return (
      <div className="bg-teal-50 border border-teal-200 rounded-md p-2 text-xs font-mono">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-teal-700 font-semibold">result</span>
          <span className="text-teal-300">{data.tool_use_id}</span>
        </div>
        <pre className="bg-white rounded p-1.5 border border-teal-100 whitespace-pre-wrap break-words">{data.output}</pre>
      </div>
    );
  } catch { return <div className="whitespace-pre-wrap text-xs">{content}</div>; }
}

function ChatMessage({ m, showAll }) {
  const isMain = m.role === 'user' || m.role === 'assistant';
  const hidden = hasLabel(m, 'hidden');
  if (!showAll && (hidden || !isMain)) return null;

  const configs = {
    user: { bg: 'bg-emerald-100', text: 'text-emerald-700', label: 'You', letter: 'U' },
    assistant: { bg: 'bg-blue-100', text: 'text-blue-700', label: 'Assistant', letter: 'A' },
    tool_use: { bg: 'bg-purple-100', text: 'text-purple-700', label: 'Tool Call', letter: 'T' },
    tool_result: { bg: 'bg-teal-100', text: 'text-teal-700', label: 'Tool Result', letter: 'R' },
  };
  const c = configs[m.role];
  const dim = hidden || (!isMain && m.role !== 'tool_use' && m.role !== 'tool_result');

  if (c) {
    const renderBody = () => {
      if (m.role === 'tool_use') return <ToolUseMessage content={m.content} />;
      if (m.role === 'tool_result') return <ToolResultMessage content={m.content} />;
      if (m.role === 'assistant') return <div className="text-gray-800 prose prose-sm max-w-none"><Markdown>{m.content}</Markdown></div>;
      return <div className="text-gray-800 whitespace-pre-wrap break-words">{m.content}</div>;
    };
    return (
      <div className={dim ? 'opacity-60' : ''}>
        <div className="flex gap-3">
          <div className={`w-7 h-7 rounded-full ${c.bg} ${c.text} flex items-center justify-center text-xs font-bold shrink-0 mt-0.5`}>{c.letter}</div>
          <div className="flex-1 min-w-0">
            <div className={`text-xs font-medium ${c.text} mb-0.5`}>{c.label}</div>
            {renderBody()}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="opacity-60">
      <div className="flex gap-3">
        <div className="w-7 h-7 rounded-full bg-gray-100 text-gray-500 flex items-center justify-center text-xs font-bold shrink-0 mt-0.5">{m.role[0].toUpperCase()}</div>
        <div className="flex-1 min-w-0">
          <div className="text-xs font-medium text-gray-500 mb-0.5">{m.role}</div>
          <div className="text-gray-600 text-xs">{renderContent(m.content)}</div>
        </div>
      </div>
    </div>
  );
}

function ChatView({ execId, exec, showAll }) {
  const [messages, setMessages] = useState([]);
  const areaRef = useRef(null);
  const prevLenRef = useRef(0);

  useEffect(() => {
    if (!execId || !exec) { setMessages([]); prevLenRef.current = 0; return; }
    const rootWf = exec.workflows[exec.root_workflow_id];
    if (!rootWf || !rootWf.conversation_id) { setMessages([]); return; }
    api(`/api/executions/${execId}/conversation/${rootWf.conversation_id}`)
      .then(setMessages)
      .catch(() => {});
  }, [execId, exec]);

  useEffect(() => {
    const area = areaRef.current;
    if (!area) return;
    if (messages.length > prevLenRef.current) {
      const wasAtBottom = area.scrollHeight - area.scrollTop - area.clientHeight < 40;
      if (wasAtBottom) requestAnimationFrame(() => { area.scrollTop = area.scrollHeight; });
    }
    prevLenRef.current = messages.length;
  }, [messages]);

  const rootWf = exec ? exec.workflows[exec.root_workflow_id] : null;

  return (
    <div ref={areaRef} className="chat-area bg-white border border-gray-200 rounded-lg overflow-y-auto p-4 space-y-3" style={{ maxHeight: '60vh' }}>
      {messages.length === 0 && (
        <div className="text-gray-400 text-center py-8">No conversation yet</div>
      )}
      {messages.map((m, i) => (
        <ChatMessage key={m.message_id || i} m={m} showAll={showAll} />
      ))}
      {exec && exec.finished && (
        <div className="text-center py-3 text-emerald-600 text-xs font-medium border-t border-gray-100 mt-2 pt-3">
          Done{rootWf && rootWf.result ? ': ' + rootWf.result : ''}
        </div>
      )}
    </div>
  );
}

function JsonTree({ data, depth = 0 }) {
  const [expanded, setExpanded] = useState(depth < 1);

  if (data === null || data === undefined) return <span className="text-gray-400">null</span>;
  if (typeof data === 'boolean') return <span className="text-orange-600">{String(data)}</span>;
  if (typeof data === 'number') return <span className="text-blue-600">{data}</span>;
  if (typeof data === 'string') {
    if (data.length > 200 && depth > 0) {
      return <span className="text-green-700">"{data.substring(0, 200)}..."</span>;
    }
    return <span className="text-green-700 whitespace-pre-wrap break-all">"{data}"</span>;
  }
  if (Array.isArray(data)) {
    if (data.length === 0) return <span className="text-gray-400">[]</span>;
    return (
      <span>
        <span className="cursor-pointer text-gray-400 hover:text-gray-600 select-none" onClick={() => setExpanded(!expanded)}>
          {expanded ? '\u25BC' : '\u25B6'} [{data.length}]
        </span>
        {expanded && (
          <div className="ml-4 border-l border-gray-200 pl-2">
            {data.map((item, i) => (
              <div key={i}><span className="text-gray-400">{i}: </span><JsonTree data={item} depth={depth + 1} /></div>
            ))}
          </div>
        )}
      </span>
    );
  }
  if (typeof data === 'object') {
    const keys = Object.keys(data);
    if (keys.length === 0) return <span className="text-gray-400">{'{}'}</span>;
    return (
      <span>
        <span className="cursor-pointer text-gray-400 hover:text-gray-600 select-none" onClick={() => setExpanded(!expanded)}>
          {expanded ? '\u25BC' : '\u25B6'} {'{' + keys.length + '}'}
        </span>
        {expanded && (
          <div className="ml-4 border-l border-gray-200 pl-2">
            {keys.map(k => (
              <div key={k}><span className="text-purple-600">{k}</span>: <JsonTree data={data[k]} depth={depth + 1} /></div>
            ))}
          </div>
        )}
      </span>
    );
  }
  return <span>{String(data)}</span>;
}

function EventRow({ e }) {
  const [expanded, setExpanded] = useState(false);
  const payloadObj = typeof e.payload === 'string' ? (() => { try { return JSON.parse(e.payload); } catch { return e.payload; } })() : e.payload;
  const payloadStr = typeof payloadObj === 'string' ? payloadObj : JSON.stringify(payloadObj);
  const short = payloadStr.length > 120 ? payloadStr.substring(0, 120) + '...' : payloadStr;
  const catCls = e.category === 'inbox' ? 'text-teal-600' : 'text-purple-600';
  return (
    <div className="border-b border-gray-50">
      <div className="flex gap-2 px-3 py-1 items-baseline hover:bg-gray-50 cursor-pointer" onClick={() => setExpanded(!expanded)}>
        <span className="w-4 text-gray-400 select-none">{expanded ? '\u25BC' : '\u25B6'}</span>
        <span className="w-8 text-right text-gray-400">{e.event_id}</span>
        <span className={`w-12 ${catCls} font-medium`}>{e.category}</span>
        <span className="w-16 text-gray-400 truncate">{(e.workflow_id || '-').substring(0, 8)}</span>
        <span className="w-44 text-gray-700 font-medium truncate">{e.type}</span>
        <span className="flex-1 text-gray-500 truncate">{short}</span>
      </div>
      {expanded && (
        <div className="px-3 py-2 bg-gray-50 border-t border-gray-100 text-xs">
          <JsonTree data={payloadObj} />
        </div>
      )}
    </div>
  );
}

function EventsView({ execId, exec }) {
  const [events, setEvents] = useState([]);
  const areaRef = useRef(null);
  const prevLenRef = useRef(0);

  useEffect(() => {
    if (!execId) { setEvents([]); prevLenRef.current = 0; return; }
    api(`/api/executions/${execId}/events`).then(setEvents).catch(() => {});
  }, [execId, exec]);

  useEffect(() => {
    const area = areaRef.current;
    if (!area) return;
    if (events.length > prevLenRef.current) {
      const wasAtBottom = area.scrollHeight - area.scrollTop - area.clientHeight < 40;
      if (wasAtBottom) requestAnimationFrame(() => { area.scrollTop = area.scrollHeight; });
    }
    prevLenRef.current = events.length;
  }, [events]);

  return (
    <div ref={areaRef} className="bg-white border border-gray-200 rounded-lg overflow-y-auto font-mono text-xs" style={{ maxHeight: '70vh' }}>
      {events.map(e => <EventRow key={e.event_id} e={e} />)}
    </div>
  );
}

function StatusView({ exec }) {
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 text-xs text-gray-600 overflow-auto font-mono" style={{ maxHeight: '70vh' }}>
      {exec ? <JsonTree data={exec} /> : null}
    </div>
  );
}

function PromptInput({ execId, onSent }) {
  const [prompts, setPrompts] = useState([]);
  const [value, setValue] = useState('');
  const [sending, setSending] = useState(false);
  const inputRef = useRef(null);

  useEffect(() => {
    if (!execId) { setPrompts([]); return; }
    api(`/api/executions/${execId}/prompts`).then(setPrompts).catch(() => {});
  }, [execId]);

  const pending = prompts.length > 0 ? prompts[0] : null;

  useEffect(() => {
    if (pending && inputRef.current && document.activeElement !== inputRef.current) {
      inputRef.current.focus();
    }
  }, [pending]);

  const send = async () => {
    if (!pending || !value.trim() || sending) return;
    setSending(true);
    try {
      await api(`/api/executions/${execId}/answer`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: pending.request_id, response: value }),
      });
      setValue('');
      setPrompts([]);
      onSent();
    } finally { setSending(false); }
  };

  if (!pending) return null;

  return (
    <div className="mt-2 flex gap-2">
      <input ref={inputRef} value={value} onChange={e => setValue(e.target.value)}
        onKeyDown={e => { if (e.key === 'Enter') send(); }}
        className="flex-1 border border-gray-300 rounded-lg px-4 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
        placeholder="Type your response..." />
      <button onClick={send} disabled={sending}
        className="bg-blue-600 hover:bg-blue-700 text-white px-5 py-2 rounded-lg text-sm font-medium transition disabled:opacity-50">
        {sending ? '...' : 'Send'}
      </button>
    </div>
  );
}

function StatusBadge({ exec }) {
  if (!exec) return null;
  if (exec.finished) return <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-500">Finished</span>;
  if (exec.has_pending_prompts) return <span className="text-xs px-2 py-0.5 rounded bg-amber-50 text-amber-700 animate-pulse">Waiting for input</span>;
  if (exec.running_bg) return <span className="text-xs px-2 py-0.5 rounded bg-blue-50 text-blue-600">Running...</span>;
  return <span className="text-xs px-2 py-0.5 rounded bg-emerald-50 text-emerald-700">Active</span>;
}

function Toggle({ checked, onChange, label }) {
  return (
    <label className="flex items-center gap-2 cursor-pointer select-none">
      <span className="text-xs text-gray-500">{label}</span>
      <button type="button" role="switch" aria-checked={checked}
        onClick={() => onChange(!checked)}
        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${checked ? 'bg-blue-500' : 'bg-gray-200'}`}>
        <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${checked ? 'translate-x-4' : 'translate-x-0.5'}`} />
      </button>
    </label>
  );
}

export default function App() {
  const [executions, setExecutions] = useState([]);
  const [currentId, setCurrentId] = useState(null);
  const [exec, setExec] = useState(null);
  const [tab, setTab] = useState('chat');
  const [showAll, setShowAll] = useState(false);
  const [target, setTarget] = useState('');
  const [args, setArgs] = useState('');

  const refresh = useCallback(async () => {
    try {
      const execs = await api('/api/executions');
      execs.sort((a, b) => b.execution_id.localeCompare(a.execution_id));
      setExecutions(execs);
    } catch {}
  }, []);

  const refreshExec = useCallback(async () => {
    if (!currentId) { setExec(null); return; }
    try {
      const ex = await api(`/api/executions/${currentId}`);
      setExec(ex);
    } catch {}
  }, [currentId]);

  // Poll executions list
  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  // Poll current execution
  useEffect(() => {
    refreshExec();
    const id = setInterval(refreshExec, 1000);
    return () => clearInterval(id);
  }, [refreshExec]);

  const selectExec = useCallback((id) => {
    setCurrentId(id);
    setExec(null);
  }, []);

  const startExec = async () => {
    if (!target) return;
    const argsList = args ? JSON.parse(`[${args}]`) : [];
    const result = await api('/api/executions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target, args: argsList }),
    });
    setCurrentId(result.execution_id);
    refresh();
  };

  const tabCls = (t) => `px-4 py-1.5 text-sm font-medium ${t === tab ? 'bg-white text-gray-700' : 'bg-gray-50 text-gray-500'}`;

  return (
    <div className="max-w-6xl mx-auto px-4 py-4 bg-gray-50 text-gray-900 font-sans text-sm min-h-screen">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold text-gray-800 tracking-tight">turbo-agent</h1>
        <div className="flex gap-2">
          <input value={target} onChange={e => setTarget(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent w-72"
            placeholder="examples/agent_demo.py:chat" />
          <input value={args} onChange={e => setArgs(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 w-28"
            placeholder="args" />
          <button onClick={startExec}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-1.5 rounded-md text-sm font-medium transition">Start</button>
        </div>
      </div>

      <div className="flex gap-4" style={{ minHeight: 'calc(100vh - 100px)' }}>
        {/* Sidebar */}
        <div className="w-64 shrink-0">
          <div className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Executions</div>
          <ExecList executions={executions} currentId={currentId} onSelect={selectExec} />
        </div>

        {/* Main */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between mb-3">
            <div className="flex gap-0 border border-gray-200 rounded-md overflow-hidden">
              <button className={`${tabCls('chat')} border-r border-gray-200`} onClick={() => setTab('chat')}>Chat</button>
              <button className={`${tabCls('events')} border-r border-gray-200`} onClick={() => setTab('events')}>Events</button>
              <button className={tabCls('status')} onClick={() => setTab('status')}>Status</button>
            </div>
            <div className="flex items-center gap-3">
              {tab === 'chat' && <Toggle checked={showAll} onChange={setShowAll} label="Details" />}
              <StatusBadge exec={exec} />
            </div>
          </div>

          {tab === 'chat' && (
            <div>
              <ChatView execId={currentId} exec={exec} showAll={showAll} />
              <PromptInput execId={currentId} onSent={() => { refresh(); refreshExec(); }} />
            </div>
          )}
          {tab === 'events' && <EventsView execId={currentId} exec={exec} />}
          {tab === 'status' && <StatusView exec={exec} />}
        </div>
      </div>
    </div>
  );
}
