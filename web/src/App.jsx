import React, { useState, useEffect, useRef, useCallback } from 'react';
import Markdown from 'react-markdown';
import { BrowserRouter, Routes, Route, useNavigate, useParams, useLocation, Link } from 'react-router-dom';

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function formatTime(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function ExecList({ executions, currentId, onSelect, onUpdateDescription }) {
  const [editingId, setEditingId] = useState(null);
  const [editValue, setEditValue] = useState('');
  const inputRef = useRef(null);

  useEffect(() => { if (editingId && inputRef.current) inputRef.current.focus(); }, [editingId]);

  const startEdit = (e, exec) => {
    e.stopPropagation();
    setEditingId(exec.execution_id);
    setEditValue(exec.description || '');
  };
  const saveEdit = (execId) => {
    setEditingId(null);
    onUpdateDescription(execId, editValue);
  };

  return (
    <div className="space-y-1">
      {executions.map(e => {
        const active = e.execution_id === currentId;
        const statusCls = e.finished ? 'bg-gray-100 text-gray-500' : e.has_pending_prompts ? 'bg-amber-50 text-amber-700' : 'bg-emerald-50 text-emerald-700';
        const statusText = e.finished ? 'done' : e.has_pending_prompts ? 'input' : 'running';
        const isEditing = editingId === e.execution_id;
        return (
          <div key={e.execution_id} className="flex items-start gap-1">
            <div className="w-5 shrink-0 pt-2.5 text-center">
              {active && !isEditing && (
                <span className="cursor-pointer text-gray-300 hover:text-gray-600 text-xs" onClick={ev => startEdit(ev, e)}>&#9998;</span>
              )}
            </div>
            <div
              className={`flex-1 min-w-0 px-3 py-2 rounded-md cursor-pointer transition border ${active ? 'bg-blue-50 border-blue-200' : 'hover:bg-gray-100 border-transparent'}`}
              onClick={() => onSelect(e.execution_id)}>
              <div className="flex items-start gap-1.5">
                <div className="flex-1">
                  {isEditing ? (
                    <input ref={inputRef} value={editValue} onChange={ev => setEditValue(ev.target.value)}
                      onClick={ev => ev.stopPropagation()}
                      onBlur={() => saveEdit(e.execution_id)}
                      onKeyDown={ev => { if (ev.key === 'Enter') saveEdit(e.execution_id); if (ev.key === 'Escape') setEditingId(null); }}
                      className="text-xs text-gray-600 border border-gray-300 rounded px-1.5 py-0.5 w-full focus:outline-none focus:ring-1 focus:ring-blue-400" />
                  ) : (
                    <div className="text-sm font-medium text-gray-800">{e.description || '???'}</div>
                  )}
                  <div className="text-xs text-gray-400">{e.workflow}</div>
                </div>
                <span className={`text-xs px-1.5 py-0.5 rounded shrink-0 ${statusCls}`}>{statusText}</span>
              </div>
              <div className="text-[10px] text-gray-300 mt-0.5 font-mono flex items-center gap-1.5 truncate">
                <span>{e.execution_id.substring(0, 8)}</span>
                {e.created_at ? <span>{formatTime(e.created_at)}</span> : null}
                {e.total_cost > 0 && <span className="text-emerald-500">{formatCost(e.total_cost)}</span>}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function WorkflowTree({ exec, selectedWfId, onSelectWf }) {
  if (!exec || !exec.workflows) return null;

  const wfs = exec.workflows;
  const rootId = exec.root_workflow_id;
  const promptWfIds = new Set(exec.prompt_workflow_ids || []);

  // Build children map
  const children = {};
  for (const [id, wf] of Object.entries(wfs)) {
    const parent = wf.parent || null;
    if (!children[parent]) children[parent] = [];
    children[parent].push(id);
  }

  const renderNode = (id, depth = 0) => {
    const wf = wfs[id];
    if (!wf) return null;
    const active = id === selectedWfId;
    const awaiting = promptWfIds.has(id);
    const statusColors = { running: 'text-emerald-600', waiting: 'text-amber-600', finished: 'text-gray-400' };
    const statusDot = awaiting ? 'text-amber-500' : (statusColors[wf.status] || 'text-gray-400');
    return (
      <div key={id}>
        <div
          className={`flex items-center gap-1.5 px-2 py-1 rounded cursor-pointer text-xs ${active ? 'bg-blue-50 text-gray-700' : 'hover:bg-gray-100 text-gray-700'}`}
          style={{ paddingLeft: `${depth * 12 + 8}px` }}
          onClick={() => onSelectWf(id)}>
          <span className={`inline-block w-1.5 h-1.5 rounded-full ${statusDot} bg-current shrink-0 ${awaiting ? 'animate-pulse' : ''}`} />
          <span className="font-medium truncate">{wf.name}</span>
          {awaiting && <span className="text-amber-500 text-[10px] shrink-0">input</span>}
          <span className="text-gray-400 font-mono ml-auto shrink-0">{id.substring(0, 6)}</span>
        </div>
        {wf.description && <div className="text-xs text-gray-400 truncate" style={{ paddingLeft: `${depth * 12 + 22}px` }}>{wf.description}</div>}
        {(children[id] || []).map(cid => renderNode(cid, depth + 1))}
      </div>
    );
  };

  return (
    <div className="mt-3">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Workflows</div>
      {renderNode(rootId)}
      {/* Render orphans (parent not in current state) */}
      {Object.keys(wfs).filter(id => id !== rootId && !wfs[wfs[id].parent]).map(id => !children[wfs[id].parent]?.includes(id) ? null : null)}
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
    } catch { }
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

function formatCost(cost) {
  if (cost >= 0.01) return '$' + cost.toFixed(2);
  if (cost >= 0.001) return '$' + cost.toFixed(3);
  return '$' + cost.toFixed(4);
}

function formatDuration(s) {
  if (s < 1) return (s * 1000).toFixed(0) + 'ms';
  if (s < 60) return s.toFixed(1) + 's';
  return (s / 60).toFixed(1) + 'm';
}

function UsageSummary({ usage }) {
  const t = usage.total;
  const hasCache = t.cache_creation_input_tokens > 0 || t.cache_read_input_tokens > 0;
  const b = 'bg-gray-100 text-gray-500 rounded px-1 py-0.5';
  return (
    <>
      <span className={b}>in {t.input_tokens.toLocaleString()}</span>
      <span className={b}>out {t.output_tokens.toLocaleString()}</span>
      {hasCache && <span className={b}>cache +{t.cache_creation_input_tokens.toLocaleString()} hit {t.cache_read_input_tokens.toLocaleString()}</span>}
      {usage.cost > 0 && <span className={b}>{formatCost(usage.cost)}</span>}
      {usage.turn_time > 0 && <span className={b}>{formatDuration(usage.turn_time)}</span>}
    </>
  );
}

function UsageSteps({ usage }) {
  const [expanded, setExpanded] = useState(false);
  if (!usage || usage.steps.length <= 1) return null;
  const b = 'bg-gray-100 text-gray-500 rounded px-1 py-0.5';
  return (
    <div className="mt-1 text-xs" style={{ color: '#9ca3af' }}>
      <span className="cursor-pointer select-none" onClick={() => setExpanded(!expanded)}>
        {expanded ? '\u25BE' : '\u25B8'} {usage.steps.length} steps {usage.llm_time > 0 && `(${formatDuration(usage.llm_time)} llm)`}
      </span>
      {expanded && (
        <div className="mt-1 ml-2 space-y-0.5 border-l-2 border-gray-100 pl-2">
          {usage.steps.map((s, i) => (
            <div key={i} className="flex flex-wrap items-center gap-1">
              <span className="text-gray-500">{s.model}</span>
              <span className={b}>in {s.input_tokens.toLocaleString()}</span>
              <span className={b}>out {s.output_tokens.toLocaleString()}</span>
              {s.cost > 0 && <span className={b}>{formatCost(s.cost)}</span>}
              {s.duration > 0 && <span className={b}>{formatDuration(s.duration)}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
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
            <div className={`text-xs font-medium ${c.text} mb-0.5 flex flex-wrap items-center gap-1.5`}>
              {c.label}
              {m.meta && m.meta.model && <span className="text-gray-400 font-normal">{m.meta.model}</span>}
              {m.created_at ? <span className="text-gray-400 font-normal">{formatTime(m.created_at)}</span> : null}
              {m.usage && <UsageSummary usage={m.usage} />}
            </div>
            {renderBody()}
            {m.usage && <UsageSteps usage={m.usage} />}
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
          <div className="text-xs font-medium text-gray-500 mb-0.5">{m.role}{m.created_at ? <span className="ml-2 text-gray-400 font-normal">{formatTime(m.created_at)}</span> : null}</div>
          <div className="text-gray-600 text-xs">{renderContent(m.content)}</div>
        </div>
      </div>
    </div>
  );
}

function ChatView({ execId, exec, showAll, selectedWfId, apiBase }) {
  const [messages, setMessages] = useState([]);
  const areaRef = useRef(null);
  const prevLenRef = useRef(0);

  const wfId = selectedWfId || (exec && exec.root_workflow_id);
  const wf = exec && wfId ? exec.workflows[wfId] : null;

  const convId = wf && wf.conversation_id;
  const prevConvId = useRef(null);

  useEffect(() => {
    if (!execId || !convId) { setMessages([]); prevLenRef.current = 0; prevConvId.current = null; return; }
    if (convId !== prevConvId.current) {
      setMessages([]);
      prevLenRef.current = 0;
      prevConvId.current = convId;
    }
    api(`${apiBase}/executions/${execId}/conversation/${convId}`)
      .then(setMessages)
      .catch(() => { });
  }, [execId, exec, convId]);

  useEffect(() => {
    const area = areaRef.current;
    if (!area) return;
    if (messages.length > prevLenRef.current) {
      requestAnimationFrame(() => { area.scrollTop = area.scrollHeight; });
    }
    prevLenRef.current = messages.length;
  }, [messages]);

  const displayWf = wf;

  return (
    <div ref={areaRef} className="chat-area bg-white border border-gray-200 rounded-lg overflow-y-auto p-4 space-y-3" style={{ maxHeight: '60vh' }}>
      {messages.length === 0 && (
        <div className="text-gray-400 text-center py-8">No conversation yet</div>
      )}
      {messages.map((m, i) => (
        <ChatMessage key={m.message_id || i} m={m} showAll={showAll} />
      ))}
      {displayWf && displayWf.status === 'finished' && (
        <div className="text-center py-3 text-emerald-600 text-xs font-medium border-t border-gray-100 mt-2 pt-3">
          Done{displayWf.result ? ': ' + displayWf.result : ''}
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
          {expanded ? '\u25BE' : '\u25B8'} [{data.length}]
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
          {expanded ? '\u25BE' : '\u25B8'} {'{' + keys.length + '}'}
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
        <span className="w-4 text-gray-400 select-none">{expanded ? '\u25BE' : '\u25B8'}</span>
        <span className="w-8 text-right text-gray-400">{e.event_id}</span>
        <span className="w-16 text-gray-400 tabular-nums">{formatTime(e.created_at)}</span>
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

function EventsView({ execId, exec, apiBase }) {
  const [events, setEvents] = useState([]);
  const areaRef = useRef(null);
  const prevLenRef = useRef(0);

  useEffect(() => {
    if (!execId) { setEvents([]); prevLenRef.current = 0; return; }
    api(`${apiBase}/executions/${execId}/events`).then(setEvents).catch(() => { });
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

function PromptInput({ execId, exec, onSent, apiBase }) {
  const [prompts, setPrompts] = useState([]);
  const [value, setValue] = useState('');
  const [sending, setSending] = useState(false);
  const inputRef = useRef(null);

  useEffect(() => {
    if (!execId) { setPrompts([]); return; }
    api(`${apiBase}/executions/${execId}/prompts`).then(setPrompts).catch(() => { });
  }, [execId, exec]);

  const pending = prompts.length > 0 ? prompts[0] : null;
  const prevPendingId = useRef(null);
  const disabled = !pending || sending;

  useEffect(() => {
    if (pending && pending.request_id !== prevPendingId.current) {
      prevPendingId.current = pending.request_id;
      if (inputRef.current) inputRef.current.focus();
    }
    if (!pending) prevPendingId.current = null;
  }, [pending]);

  const send = async () => {
    if (!pending || !value.trim() || sending) return;
    setSending(true);
    try {
      await api(`${apiBase}/executions/${execId}/answer`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: pending.request_id, response: value }),
      });
      setValue('');
      setPrompts([]);
      onSent();
    } finally { setSending(false); }
  };

  return (
    <div className="mt-2 flex gap-2">
      <input ref={inputRef} value={value} onChange={e => setValue(e.target.value)}
        disabled={disabled}
        onKeyDown={e => { if (e.key === 'Enter') send(); }}
        className={`flex-1 border rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${disabled ? 'bg-gray-50 border-gray-200 text-gray-400' : 'bg-white border-gray-300'}`}
        placeholder={pending ? 'Type your response...' : 'Waiting...'} />
      <button onClick={send} disabled={disabled}
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

function TaskCard({ task }) {
  const isPending = task.status === 'pending';
  const borderColor = task.color ? { borderLeftColor: task.color } : {};
  return (
    <div className={`border rounded-lg p-3 bg-white border-l-4 ${isPending ? 'border-l-blue-400' : 'border-l-gray-300'}`} style={borderColor}>
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="font-medium text-gray-800 text-sm">{task.name}</div>
          {task.description && <div className="text-xs text-gray-400 mt-0.5 line-clamp-2">{task.description}</div>}
        </div>
        <div className="flex items-center gap-1 shrink-0">
          {task.needs_input && <span className="text-xs px-1.5 py-0.5 rounded bg-amber-50 text-amber-600 animate-pulse">input</span>}
          <span className={`text-xs px-1.5 py-0.5 rounded ${isPending ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-500'}`}>
            {task.status}
          </span>
        </div>
      </div>
      {Object.entries(task.labels).filter(([k]) => k !== 'project').length > 0 && (
        <div className="flex flex-wrap gap-1 mt-1.5">
          {Object.entries(task.labels).filter(([k]) => k !== 'project').map(([k, v]) => (
            <span key={k} className="text-[10px] bg-gray-100 text-gray-500 rounded px-1.5 py-0.5">
              {v || k}
            </span>
          ))}
        </div>
      )}
      <div className="text-[10px] text-gray-300 mt-1.5 font-mono">{task.task_id.substring(0, 8)}</div>
    </div>
  );
}

function CreateProjectModal({ onClose, onCreated }) {
  const [name, setName] = useState('');
  const inputRef = useRef(null);
  useEffect(() => { if (inputRef.current) inputRef.current.focus(); }, []);
  const submit = async () => {
    if (!name.trim()) return;
    await api('/api/projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    onCreated();
  };
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-lg shadow-lg w-80 p-4" onClick={e => e.stopPropagation()}>
        <div className="text-sm font-semibold text-gray-800 mb-3">New project</div>
        <input ref={inputRef} value={name} onChange={e => setName(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') submit(); if (e.key === 'Escape') onClose(); }}
          className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 mb-3"
          placeholder="Project name" />
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700 px-3 py-1 rounded text-xs">Cancel</button>
          <button onClick={submit} disabled={!name.trim()}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-1 rounded text-xs font-medium transition disabled:opacity-50">Create</button>
        </div>
      </div>
    </div>
  );
}

function CreateTaskModal({ project, onClose, onCreated }) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const nameRef = useRef(null);

  useEffect(() => { if (nameRef.current) nameRef.current.focus(); }, []);

  const submit = async () => {
    if (!name.trim()) return;
    const labels = project ? { project } : {};
    const task = await api('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, description, labels }),
    });
    onCreated(task);
  };

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-lg shadow-lg w-96 p-4" onClick={e => e.stopPropagation()}>
        <div className="text-sm font-semibold text-gray-800 mb-3">
          New task{project ? <span className="text-gray-400 font-normal"> in {project}</span> : ''}
        </div>
        <input ref={nameRef} value={name} onChange={e => setName(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter' && name.trim()) submit(); if (e.key === 'Escape') onClose(); }}
          className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 mb-2"
          placeholder="Task name" />
        <textarea value={description} onChange={e => setDescription(e.target.value)}
          rows={3}
          onKeyDown={e => { if (e.key === 'Escape') onClose(); }}
          className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 mb-3 resize-y"
          placeholder="Description (optional)" />
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700 px-3 py-1 rounded text-xs">Cancel</button>
          <button onClick={submit} disabled={!name.trim()}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-1 rounded text-xs font-medium transition disabled:opacity-50">Create</button>
        </div>
      </div>
    </div>
  );
}

function TasksView({ setModalProject, refreshKey }) {
  const [tasks, setTasks] = useState([]);
  const [projects, setProjects] = useState([]);
  const [editingProject, setEditingProject] = useState(null);
  const [editProjectValue, setEditProjectValue] = useState('');
  const editProjectRef = useRef(null);

  const refresh = useCallback(async () => {
    try { setTasks(await api('/api/tasks')); } catch { }
    try { setProjects(await api('/api/projects')); } catch { }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 3000);
    return () => clearInterval(id);
  }, [refresh, refreshKey]);

  useEffect(() => { if (editingProject !== null && editProjectRef.current) editProjectRef.current.focus(); }, [editingProject]);

  const renameProject = async (oldName) => {
    const newName = editProjectValue.trim();
    setEditingProject(null);
    if (!newName || newName === oldName) return;
    // Update all tasks in this project
    const projectTasks = tasks.filter(t => (t.labels && t.labels.project) === oldName || (!t.labels?.project && oldName === ''));
    for (const t of projectTasks) {
      const newLabels = { ...t.labels, project: newName };
      await api(`/api/tasks/${t.task_id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ labels: newLabels }),
      });
    }
    refresh();
  };

  // Group by project label, sort: pending first, then by created_at desc
  const sorted = [...tasks].sort((a, b) => {
    if (a.status !== b.status) return a.status === 'pending' ? -1 : 1;
    return b.created_at - a.created_at;
  });
  const groups = {};
  // Include all known projects (even empty)
  for (const p of projects) { if (!groups[p]) groups[p] = []; }
  for (const t of sorted) {
    const project = (t.labels && t.labels.project) || '';
    if (!groups[project]) groups[project] = [];
    groups[project].push(t);
  }
  const projectKeys = Object.keys(groups).sort((a, b) => {
    if (a === '') return 1;
    if (b === '') return -1;
    return a.localeCompare(b);
  });

  return (
    <div>
      {projectKeys.map(project => (
        <div key={project} className="mb-4">
          <div className="flex items-center gap-2 mb-2">
            {project && editingProject !== project && (
              <span className="text-gray-300 hover:text-gray-600 cursor-pointer" onClick={() => { setEditingProject(project); setEditProjectValue(project); }}>&#9998;</span>
            )}
            {editingProject === project ? (
              <input ref={editProjectRef} value={editProjectValue} onChange={e => setEditProjectValue(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') renameProject(project); if (e.key === 'Escape') setEditingProject(null); }}
                onBlur={() => renameProject(project)}
                className="border border-gray-300 rounded px-2 py-0.5 text-xs font-medium w-40 focus:outline-none focus:ring-1 focus:ring-blue-400" />
            ) : (
              <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">{project || 'No project'}</span>
            )}
            <span className="text-[10px] text-gray-400">{groups[project].filter(t => t.status === 'finished').length}/{groups[project].length}</span>
            <button className="text-gray-400 hover:text-gray-600 text-xs px-2 py-0.5 rounded-md border border-gray-200 hover:bg-white transition" onClick={() => setModalProject(project || '')}>+ Add task</button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {groups[project].map(t => (
              <Link key={t.task_id} to={`/tasks/${t.task_id}`} className="no-underline">
                <TaskCard task={t} />
              </Link>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function TaskDetailView() {
  const { taskId } = useParams();
  const navigate = useNavigate();
  const [task, setTask] = useState(null);
  const [executions, setExecutions] = useState([]);
  const [editingDesc, setEditingDesc] = useState(false);
  const [descValue, setDescValue] = useState('');
  const [editingName, setEditingName] = useState(false);
  const [nameValue, setNameValue] = useState('');
  const [startTarget, setStartTarget] = useState('');
  const textareaRef = useRef(null);
  const nameRef = useRef(null);

  const refresh = useCallback(() => {
    api(`/api/tasks/${taskId}`).then(t => {
      setTask(t);
      if (!editingDesc) setDescValue(t.description || '');
    }).catch(() => {});
    api(`/api/tasks/${taskId}/executions`).then(setExecutions).catch(() => {});
  }, [taskId, editingDesc]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);
  useEffect(() => { if (editingDesc && textareaRef.current) textareaRef.current.focus(); }, [editingDesc]);
  useEffect(() => { if (editingName && nameRef.current) nameRef.current.focus(); }, [editingName]);

  const saveDesc = async () => {
    setEditingDesc(false);
    await api(`/api/tasks/${taskId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: descValue }),
    });
    refresh();
  };

  const startExecution = async () => {
    if (!startTarget.trim()) return;
    const result = await api(`/api/tasks/${taskId}/executions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: startTarget }),
    });
    setStartTarget('');
    refresh();
  };

  const saveName = async () => {
    setEditingName(false);
    if (nameValue.trim() && nameValue !== task.name) {
      await api(`/api/tasks/${taskId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: nameValue }),
      });
      refresh();
    }
  };

  if (!task) return <div className="text-gray-400 py-8 text-center">Loading...</div>;

  const hasLabels = Object.keys(task.labels).length > 0;

  return (
    <div>
      {/* Breadcrumb */}
      <div className="mb-3">
        <Link to="/" className="text-xs text-gray-400 hover:text-gray-600 no-underline">Tasks</Link>
        <span className="text-xs text-gray-300 mx-1">/</span>
        <span className="text-xs text-gray-600">{task.name}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        {!editingName && (
          <span className="text-gray-300 hover:text-gray-600 cursor-pointer" onClick={() => { setEditingName(true); setNameValue(task.name); }}>&#9998;</span>
        )}
        {editingName ? (
          <input ref={nameRef} value={nameValue} onChange={e => setNameValue(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') saveName(); if (e.key === 'Escape') setEditingName(false); }}
            onBlur={saveName}
            className="text-lg font-semibold text-gray-800 flex-1 border border-gray-300 rounded px-2 py-0.5 focus:outline-none focus:ring-1 focus:ring-blue-400" />
        ) : (
          <h2 className="text-lg font-semibold text-gray-800 flex-1">{task.name}</h2>
        )}
        {task.needs_input && <span className="text-xs px-2 py-0.5 rounded bg-amber-50 text-amber-600 animate-pulse">input</span>}
        <span className={`text-xs px-2 py-0.5 rounded ${task.status === 'pending' ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-500'}`}>
          {task.status}
        </span>
      </div>

      <div className="flex gap-4">
        {/* Main: description */}
        <div className="flex-1 min-w-0">
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              {!editingDesc && (
                <span className="text-gray-400 hover:text-gray-600 cursor-pointer" onClick={() => setEditingDesc(true)}>&#9998;</span>
              )}
              <span className="text-xs font-medium text-gray-400 uppercase tracking-wider">Description</span>
            </div>
            {editingDesc ? (
              <div>
                <textarea ref={textareaRef} value={descValue} onChange={e => setDescValue(e.target.value)}
                  rows={12}
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 resize-y font-mono"
                  placeholder="Write task description (markdown supported)..." />
                <div className="flex gap-2 mt-2">
                  <button onClick={saveDesc} className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-xs font-medium transition">Save</button>
                  <button onClick={() => { setEditingDesc(false); setDescValue(task.description || ''); }} className="text-gray-500 hover:text-gray-700 px-3 py-1 rounded text-xs">Cancel</button>
                </div>
              </div>
            ) : (
              task.description
                ? <div className="prose prose-sm max-w-none text-gray-700"><Markdown>{task.description}</Markdown></div>
                : <div className="text-gray-300 text-sm italic cursor-pointer" onClick={() => setEditingDesc(true)}>Click to add description...</div>
            )}
          </div>
          {/* Executions */}
          <div className="bg-white border border-gray-200 rounded-lg p-4 mt-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-gray-400 uppercase tracking-wider">Executions</span>
              <div className="flex gap-1">
                <input value={startTarget} onChange={e => setStartTarget(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') startExecution(); }}
                  className="border border-gray-300 rounded px-2 py-0.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-blue-400 w-52"
                  placeholder="file.py:workflow" />
                <button onClick={startExecution}
                  className="text-gray-400 hover:text-gray-600 text-xs px-2 py-0.5 rounded-md border border-gray-200 hover:bg-white transition">Start</button>
              </div>
            </div>
            {executions.length === 0 && <div className="text-gray-300 text-xs text-center py-4">No executions yet</div>}
            <div className="space-y-1">
              {executions.map(e => {
                const statusCls = e.finished ? 'bg-gray-100 text-gray-500' : e.has_pending_prompts ? 'bg-amber-50 text-amber-600' : 'bg-emerald-50 text-emerald-700';
                const statusText = e.finished ? 'done' : e.has_pending_prompts ? 'input' : 'running';
                return (
                  <div key={e.execution_id} className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-gray-50 cursor-pointer"
                    onClick={() => navigate(`/tasks/${taskId}/executions/${e.execution_id}`)}>
                    <span className="text-sm text-gray-700 flex-1">{e.description || e.workflow}</span>
                    {e.total_cost > 0 && <span className="text-[10px] text-gray-400 font-mono">{formatCost(e.total_cost)}</span>}
                    <span className={`text-xs px-1.5 py-0.5 rounded ${statusCls}`}>{statusText}</span>
                    <span className="text-[10px] text-gray-300 font-mono">{e.execution_id.substring(0, 8)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Sidebar: labels & meta */}
        <div className="w-56 shrink-0">
          {hasLabels && (
            <div className="bg-white border border-gray-200 rounded-lg p-3 mb-3">
              <div className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">Labels</div>
              <div className="space-y-1">
                {Object.entries(task.labels).map(([k, v]) => (
                  <div key={k} className="flex items-center justify-between text-xs">
                    <span className="text-gray-400">{k}</span>
                    <span className="text-gray-700 font-medium">{v || '-'}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          <div className="bg-white border border-gray-200 rounded-lg p-3">
            <div className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">Details</div>
            <div className="space-y-1 text-xs">
              <div className="flex items-center justify-between">
                <span className="text-gray-400">ID</span>
                <span className="text-gray-600 font-mono">{task.task_id.substring(0, 12)}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-gray-400">Created</span>
                <span className="text-gray-600">{task.created_at ? new Date(task.created_at * 1000).toLocaleString() : '-'}</span>
              </div>
              {task.color && (
                <div className="flex items-center justify-between">
                  <span className="text-gray-400">Color</span>
                  <span className="flex items-center gap-1 text-gray-600"><span className="inline-block w-3 h-3 rounded" style={{ background: task.color }} /> {task.color}</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ExecutionsView() {
  const { taskId, executionId: currentId, workflowId: selectedWfId } = useParams();
  const navigate = useNavigate();
  const apiBase = `/api/tasks/${taskId}`;
  const [executions, setExecutions] = useState([]);
  const [exec, setExec] = useState(null);
  const [tab, setTab] = useState('chat');
  const [showAll, setShowAll] = useState(false);
  const [target, setTarget] = useState('');

  const refresh = useCallback(async () => {
    if (!taskId) return;
    try {
      const execs = await api(`${apiBase}/executions`);
      execs.sort((a, b) => b.execution_id.localeCompare(a.execution_id));
      setExecutions(execs);
    } catch { }
  }, [taskId, apiBase]);

  const refreshExec = useCallback(async () => {
    if (!currentId) { setExec(null); return; }
    try {
      const ex = await api(`${apiBase}/executions/${currentId}`);
      setExec(ex);
    } catch { }
  }, [currentId, apiBase]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    refreshExec();
    const id = setInterval(refreshExec, 1000);
    return () => clearInterval(id);
  }, [refreshExec]);

  const selectExec = useCallback((id) => {
    navigate(`/tasks/${taskId}/executions/${id}`);
  }, [navigate, taskId]);

  const selectWf = useCallback((wfId) => {
    navigate(`/tasks/${taskId}/executions/${currentId}/workflows/${wfId}`);
  }, [navigate, taskId, currentId]);

  const startExec = async () => {
    if (!target) return;
    const result = await api(`${apiBase}/executions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target }),
    });
    refresh();
    navigate(`/tasks/${taskId}/executions/${result.execution_id}`);
  };

  const tabCls = (t) => `px-4 py-1.5 text-sm font-medium ${t === tab ? 'bg-white text-gray-700' : 'bg-gray-50 text-gray-500'}`;

  return (
    <div className="flex gap-4" style={{ minHeight: 'calc(100vh - 100px)' }}>
      <div className="w-64 shrink-0">
        <div className="flex gap-2 mb-3">
          <input value={target} onChange={e => setTarget(e.target.value)}
            className="border border-gray-300 rounded-md px-2 py-1 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-blue-500 flex-1 min-w-0"
            placeholder="file.py:workflow" />
          <button onClick={startExec}
            className="bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded-md text-xs font-medium transition shrink-0">Start</button>
        </div>
        <ExecList executions={executions} currentId={currentId} onSelect={selectExec}
          onUpdateDescription={async (eid, desc) => {
            await api(`${apiBase}/executions/${eid}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ description: desc }) });
            refresh(); refreshExec();
          }} />
        {exec && currentId && (
          <WorkflowTree exec={exec} selectedWfId={selectedWfId || exec.root_workflow_id} onSelectWf={selectWf} />
        )}
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div className="flex gap-0 border border-gray-200 rounded-md overflow-hidden">
              <button className={`${tabCls('chat')} border-r border-gray-200`} onClick={() => setTab('chat')}>Chat</button>
              <button className={`${tabCls('events')} border-r border-gray-200`} onClick={() => setTab('events')}>Events</button>
              <button className={tabCls('status')} onClick={() => setTab('status')}>Status</button>
            </div>
            {tab === 'chat' && <Toggle checked={showAll} onChange={setShowAll} label="Details" />}
          </div>
          <div className="flex items-center gap-3">
            {exec && exec.total_cost > 0 && (
              <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-500 font-mono">{formatCost(exec.total_cost)}</span>
            )}
            <StatusBadge exec={exec} />
          </div>
        </div>

        {tab === 'chat' && (
          <div>
            <ChatView execId={currentId} exec={exec} showAll={showAll} selectedWfId={selectedWfId} apiBase={apiBase} />
            <PromptInput execId={currentId} exec={exec} onSent={() => { refresh(); refreshExec(); }} apiBase={apiBase} />
          </div>
        )}
        {tab === 'events' && <EventsView execId={currentId} exec={exec} apiBase={apiBase} />}
        {tab === 'status' && <StatusView exec={exec} />}
      </div>
    </div>
  );
}

function Layout({ children, toolbar }) {
  return (
    <div className="max-w-6xl mx-auto px-4 py-4 bg-gray-50 text-gray-900 font-sans text-sm min-h-screen">
      <div className="flex items-center gap-4 mb-4">
        <Link to="/" className="text-lg font-semibold text-gray-800 tracking-tight no-underline">turbo-agent</Link>
        <Link to="/" className="px-3 py-1 text-xs font-medium bg-gray-50 text-gray-400 no-underline hover:bg-white hover:text-gray-700 rounded-md border border-gray-200">Tasks</Link>
        {toolbar}
      </div>
      {children}
    </div>
  );
}

function TasksPage() {
  const [showProjectModal, setShowProjectModal] = useState(false);
  const [modalProject, setModalProject] = useState(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const navigate = useNavigate();

  const toolbar = (
    <button onClick={() => setShowProjectModal(true)}
      className="text-gray-400 hover:text-gray-600 text-xs px-2 py-1 rounded-md border border-gray-200 hover:bg-white transition">+ Add project</button>
  );

  return (
    <Layout toolbar={toolbar}>
      {showProjectModal && (
        <CreateProjectModal
          onClose={() => setShowProjectModal(false)}
          onCreated={() => { setShowProjectModal(false); setRefreshKey(k => k + 1); }}
        />
      )}
      {modalProject !== null && (
        <CreateTaskModal
          project={modalProject || null}
          onClose={() => setModalProject(null)}
          onCreated={(task) => { setModalProject(null); setRefreshKey(k => k + 1); navigate(`/tasks/${task.task_id}`); }}
        />
      )}
      <TasksView setModalProject={setModalProject} refreshKey={refreshKey} />
    </Layout>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<TasksPage />} />
        <Route path="/tasks/:taskId" element={<Layout><TaskDetailView /></Layout>} />
        <Route path="/tasks/:taskId/executions/:executionId" element={<Layout><ExecutionsView /></Layout>} />
        <Route path="/tasks/:taskId/executions/:executionId/workflows/:workflowId" element={<Layout><ExecutionsView /></Layout>} />
      </Routes>
    </BrowserRouter>
  );
}
