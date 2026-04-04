import json
import pickle
import turso
from workflows.ids import new_id
from workflows.ops import ExecutionState, Event
from workflows.events import serialize_payload, deserialize_payload, payload_type_name
from workflows.conversation import MessageRef, ConversationRef, Message


class Store:
    """Persists execution state and events in a Turso database."""

    def __init__(self, db_path: str):
        self.conn = turso.connect(db_path)
        self._migrate()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                state BLOB NOT NULL,
                last_processed_event_id INTEGER NOT NULL DEFAULT 0,
                finished INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                workflow_id TEXT,
                category TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_events_inbox
                ON events (execution_id, category, event_id)
                WHERE category = 'inbox';

            CREATE INDEX IF NOT EXISTS idx_events_outbox
                ON events (execution_id, category, event_id)
                WHERE category = 'outbox';

            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                layer INTEGER NOT NULL DEFAULT 0,
                tombstone INTEGER NOT NULL DEFAULT 0,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                meta TEXT NOT NULL DEFAULT '{}',
                event_time INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (conversation_id, message_id, layer)
            );

            CREATE TABLE IF NOT EXISTS conversation_refs (
                conversation_id TEXT PRIMARY KEY,
                parent_conversation_id TEXT,
                parent_message_id TEXT,
                parent_layer INTEGER
            );
        """)
        self.conn.commit()

    def save_state(self, execution_id: str, state: ExecutionState,
                   last_processed_event_id: int | None = None):
        import time as _time
        cur = self.conn.cursor()
        cur.execute(
            "SELECT last_processed_event_id, created_at FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = cur.fetchone()
        lp = last_processed_event_id if last_processed_event_id is not None else (row[0] if row else 0)
        created_at = row[1] if row else _time.time()

        cur.execute(
            """INSERT OR REPLACE INTO executions
               (execution_id, state, last_processed_event_id, finished, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (execution_id, pickle.dumps(state), lp, int(state.finished), created_at),
        )
        self.conn.commit()

    def load_state(self, execution_id: str) -> tuple[ExecutionState, int]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT state, last_processed_event_id FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"Execution {execution_id} not found")
        return pickle.loads(row[0]), row[1]

    def get_created_at(self, execution_id: str) -> float:
        cur = self.conn.cursor()
        cur.execute("SELECT created_at FROM executions WHERE execution_id = ?", (execution_id,))
        row = cur.fetchone()
        return row[0] if row else 0.0

    def append_event(self, execution_id: str, workflow_id: str | None,
                     category: str, payload) -> None:
        """Append a single event with immediate commit."""
        self.append_events([(execution_id, workflow_id, category, payload)])

    def append_events(self, events: list[tuple]) -> None:
        """Batch-append events. Each tuple: (execution_id, workflow_id, category, payload)."""
        import time as _time
        now = _time.time()
        cur = self.conn.cursor()
        for execution_id, workflow_id, category, payload in events:
            cur.execute(
                """INSERT INTO events (execution_id, workflow_id, category, type, payload, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (execution_id, workflow_id, category,
                 payload_type_name(payload), serialize_payload(payload), now),
            )
        self.conn.commit()

    def _read_events(self, execution_id: str, category: str,
                     after_event_id: int = 0) -> list[Event]:
        cur = self.conn.cursor()
        cur.execute(
            """SELECT event_id, execution_id, workflow_id, category, payload, created_at
               FROM events
               WHERE execution_id = ? AND category = ? AND event_id > ?
               ORDER BY event_id""",
            (execution_id, category, after_event_id),
        )
        return [
            Event(
                event_id=row[0],
                execution_id=row[1],
                workflow_id=row[2],
                category=row[3],
                payload=deserialize_payload(row[4]),
                created_at=row[5] or 0.0,
            )
            for row in cur.fetchall()
        ]

    def read_all_events(self, execution_id: str, after_event_id: int = 0) -> list[Event]:
        """Read all events (inbox + outbox) after a given event_id."""
        cur = self.conn.cursor()
        cur.execute(
            """SELECT event_id, execution_id, workflow_id, category, payload, created_at
               FROM events
               WHERE execution_id = ? AND event_id > ?
               ORDER BY event_id""",
            (execution_id, after_event_id),
        )
        return [
            Event(
                event_id=row[0],
                execution_id=row[1],
                workflow_id=row[2],
                category=row[3],
                payload=deserialize_payload(row[4]),
                created_at=row[5] or 0.0,
            )
            for row in cur.fetchall()
        ]

    def read_inbox(self, execution_id: str, after_event_id: int = 0) -> list[Event]:
        return self._read_events(execution_id, 'inbox', after_event_id)

    def read_outbox(self, execution_id: str, after_event_id: int = 0) -> list[Event]:
        return self._read_events(execution_id, 'outbox', after_event_id)

    def list_executions(self) -> list[tuple[str, ExecutionState, float]]:
        cur = self.conn.cursor()
        cur.execute("SELECT execution_id, state, created_at FROM executions ORDER BY execution_id")
        return [(row[0], pickle.loads(row[1]), row[2] or 0.0) for row in cur.fetchall()]

    # ---- Conversation methods ----

    def create_conversation(self, conversation_id: str,
                            parent_conversation_id: str | None = None,
                            parent_message_id: str | None = None,
                            parent_layer: int | None = None):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO conversation_refs
               (conversation_id, parent_conversation_id, parent_message_id, parent_layer)
               VALUES (?, ?, ?, ?)""",
            (conversation_id, parent_conversation_id, parent_message_id, parent_layer),
        )
        self.conn.commit()

    def conv_append_message(self, conversation_id: str, role: str,
                            content, meta: dict | None = None,
                            event_time: int = 0) -> MessageRef:
        import time as _time
        if not isinstance(content, str):
            content = json.dumps(content)
        message_id = new_id()
        meta = meta or {}
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO conversations
               (conversation_id, message_id, layer, tombstone, role, content, meta, event_time, created_at)
               VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?)""",
            (conversation_id, message_id, role, content, json.dumps(meta), event_time, _time.time()),
        )
        self.conn.commit()
        return MessageRef(conversation_id=conversation_id, message_id=message_id,
                          layer=0, role=role, meta=meta, event_time=event_time)

    def conv_list_messages(self, conversation_id: str,
                           end_message_id: str | None = None,
                           max_layer: int | None = None,
                           start_message_id: str | None = None,
                           role_filter: str | None = None,
                           pattern: str | None = None) -> list[MessageRef]:
        """List message refs, following parent chain. Returns [MessageRef] (no content)."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT parent_conversation_id, parent_message_id, parent_layer "
            "FROM conversation_refs WHERE conversation_id = ?",
            (conversation_id,),
        )
        row = cur.fetchone()
        parent_refs = []
        if row and row[0]:
            parent_refs = self.conv_list_messages(
                row[0], row[1], row[2],
                start_message_id=start_message_id,
                role_filter=role_filter, pattern=pattern,
            )

        own_refs = self._list_layer_messages(
            conversation_id, end_message_id, max_layer,
            start_message_id=start_message_id,
            role_filter=role_filter, pattern=pattern,
        )
        return parent_refs + own_refs

    def _list_layer_messages(self, conversation_id, end_message_id, max_layer,
                             start_message_id=None, role_filter=None,
                             pattern=None) -> list[MessageRef]:
        """List refs from a single conversation layer (no parent chain)."""
        cur = self.conn.cursor()
        conditions = ["conversation_id = ?"]
        params = [conversation_id]
        if end_message_id:
            conditions.append("message_id <= ?")
            params.append(end_message_id)
        if start_message_id:
            conditions.append("message_id >= ?")
            params.append(start_message_id)
        if max_layer is not None:
            conditions.append("layer <= ?")
            params.append(max_layer)

        where = " AND ".join(conditions)
        cur.execute(
            f"SELECT message_id, layer, tombstone, role, meta, event_time, created_at "
            f"FROM conversations WHERE {where} ORDER BY message_id, layer DESC",
            params,
        )

        refs = []
        seen = set()
        for msg_id, layer, tombstone, role, meta_str, event_time, created_at in cur.fetchall():
            if msg_id in seen:
                continue
            seen.add(msg_id)
            if tombstone:
                continue
            if role_filter and role != role_filter:
                continue
            if pattern:
                # Need to check content for pattern — read it
                cur2 = self.conn.cursor()
                cur2.execute(
                    "SELECT content FROM conversations "
                    "WHERE conversation_id = ? AND message_id = ? AND layer = ?",
                    (conversation_id, msg_id, layer),
                )
                row = cur2.fetchone()
                if row and pattern.replace('%', '') not in row[0]:
                    continue
            meta = json.loads(meta_str) if meta_str else {}
            refs.append(MessageRef(conversation_id, msg_id, layer, role, meta, event_time or 0))

        refs.sort(key=lambda r: r.message_id)
        return refs

    def conv_read_messages(self, refs: list[MessageRef]) -> list[Message]:
        """Read message content by refs. Returns [Message]."""
        results = []
        cur = self.conn.cursor()
        for ref in refs:
            cur.execute(
                "SELECT content, meta, event_time, created_at FROM conversations "
                "WHERE conversation_id = ? AND message_id = ? AND layer = ?",
                (ref.conversation_id, ref.message_id, ref.layer),
            )
            row = cur.fetchone()
            if row:
                meta = json.loads(row[1]) if row[1] else {}
                enriched_ref = MessageRef(
                    ref.conversation_id, ref.message_id, ref.layer,
                    ref.role, meta, row[2] or 0,
                )
                msg = Message(ref=enriched_ref, content=row[0])
                msg.created_at = row[3] or 0.0
                results.append(msg)
        return results

    def conv_replace_with(self, conversation_id: str,
                          new_messages: list[dict],
                          start_message_id: str | None = None,
                          end_message_id: str | None = None,
                          event_time: int = 0) -> list[MessageRef]:
        """Replace a range of messages with new ones via layer compaction."""
        cur = self.conn.cursor()

        conditions = ["conversation_id = ?"]
        params = [conversation_id]
        if start_message_id:
            conditions.append("message_id >= ?")
            params.append(start_message_id)
        if end_message_id:
            conditions.append("message_id <= ?")
            params.append(end_message_id)

        where = " AND ".join(conditions)
        cur.execute(
            f"SELECT DISTINCT message_id, MAX(layer) as max_layer "
            f"FROM conversations WHERE {where} GROUP BY message_id",
            params,
        )
        existing = cur.fetchall()
        max_layer = max((row[1] for row in existing), default=0) if existing else 0
        new_layer = max_layer + 1

        for msg_id, _ in existing:
            cur.execute(
                """INSERT OR IGNORE INTO conversations
                   (conversation_id, message_id, layer, tombstone, role, content)
                   VALUES (?, ?, ?, 1, '', '')""",
                (conversation_id, msg_id, new_layer),
            )

        # Use start_message_id as base so replacements sort in the right position
        base_id = start_message_id or (existing[0][0] if existing else new_id())
        new_refs = []
        for i, msg in enumerate(new_messages):
            msg_id = f'{base_id}_r{i}'
            meta = msg.get('meta', {})
            cur.execute(
                """INSERT INTO conversations
                   (conversation_id, message_id, layer, tombstone, role, content, meta, event_time)
                   VALUES (?, ?, ?, 0, ?, ?, ?, ?)""",
                (conversation_id, msg_id, new_layer, msg['role'], msg['content'], json.dumps(meta), event_time),
            )
            new_refs.append(MessageRef(conversation_id, msg_id, new_layer, msg['role'], meta, event_time))

        self.conn.commit()
        return new_refs

    def conv_update_message(self, ref: MessageRef, content) -> None:
        """Update message content in place. Only for special cases like task context."""
        if not isinstance(content, str):
            content = json.dumps(content)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE conversations SET content = ? WHERE conversation_id = ? AND message_id = ? AND layer = ?",
            (content, ref.conversation_id, ref.message_id, ref.layer),
        )
        self.conn.commit()

    def conv_resolve_ref(self, conversation_id: str) -> ConversationRef:
        """Resolve to a concrete ConversationRef."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT MAX(message_id), MAX(layer) FROM conversations WHERE conversation_id = ?",
            (conversation_id,),
        )
        row = cur.fetchone()
        msg_id = row[0] if row and row[0] else ''
        layer = row[1] if row and row[1] is not None else 0
        return ConversationRef(conversation_id, msg_id, layer)

    def close(self):
        self.conn.close()
