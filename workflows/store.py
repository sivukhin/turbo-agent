import pickle
import uuid
import turso
from workflows.engine import ExecutionState, Event
from workflows.events import serialize_payload, deserialize_payload, payload_type_name
from workflows.conversation import (
    MessageRef, ConversationRef, ConversationMessage, _sortable_uuid,
)


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
                finished INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                workflow_id TEXT,
                category TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL
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
        cur = self.conn.cursor()
        cur.execute(
            "SELECT last_processed_event_id FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = cur.fetchone()
        lp = last_processed_event_id if last_processed_event_id is not None else (row[0] if row else 0)

        cur.execute(
            """INSERT OR REPLACE INTO executions
               (execution_id, state, last_processed_event_id, finished)
               VALUES (?, ?, ?, ?)""",
            (execution_id, pickle.dumps(state), lp, int(state.finished)),
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

    def append_event(self, execution_id: str, workflow_id: str | None,
                     category: str, payload) -> None:
        """Append a single event with immediate commit."""
        self.append_events([(execution_id, workflow_id, category, payload)])

    def append_events(self, events: list[tuple]) -> None:
        """Batch-append events. Each tuple: (execution_id, workflow_id, category, payload)."""
        cur = self.conn.cursor()
        for execution_id, workflow_id, category, payload in events:
            cur.execute(
                """INSERT INTO events (execution_id, workflow_id, category, type, payload)
                   VALUES (?, ?, ?, ?, ?)""",
                (execution_id, workflow_id, category,
                 payload_type_name(payload), serialize_payload(payload)),
            )
        self.conn.commit()

    def _read_events(self, execution_id: str, category: str,
                     after_event_id: int = 0) -> list[Event]:
        cur = self.conn.cursor()
        cur.execute(
            """SELECT event_id, execution_id, workflow_id, category, payload
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
            )
            for row in cur.fetchall()
        ]

    def read_inbox(self, execution_id: str, after_event_id: int = 0) -> list[Event]:
        return self._read_events(execution_id, 'inbox', after_event_id)

    def read_outbox(self, execution_id: str, after_event_id: int = 0) -> list[Event]:
        return self._read_events(execution_id, 'outbox', after_event_id)

    def list_executions(self) -> list[tuple[str, ExecutionState]]:
        cur = self.conn.cursor()
        cur.execute("SELECT execution_id, state FROM executions ORDER BY execution_id")
        return [(row[0], pickle.loads(row[1])) for row in cur.fetchall()]

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
                            content: str) -> MessageRef:
        message_id = _sortable_uuid()
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO conversations
               (conversation_id, message_id, layer, tombstone, role, content)
               VALUES (?, ?, 0, 0, ?, ?)""",
            (conversation_id, message_id, role, content),
        )
        self.conn.commit()
        return MessageRef(conversation_id=conversation_id, message_id=message_id, layer=0)

    def conv_read_messages(self, conversation_id: str,
                           end_message_id: str | None = None,
                           max_layer: int | None = None) -> list[ConversationMessage]:
        """Read resolved conversation, following parent chain."""
        # Get parent ref
        cur = self.conn.cursor()
        cur.execute(
            "SELECT parent_conversation_id, parent_message_id, parent_layer "
            "FROM conversation_refs WHERE conversation_id = ?",
            (conversation_id,),
        )
        row = cur.fetchone()
        parent_messages = []
        if row and row[0]:
            parent_messages = self.conv_read_messages(row[0], row[1], row[2])

        # Read own messages
        own_messages = self._read_conversation_layer(
            conversation_id, end_message_id, max_layer,
        )

        return parent_messages + own_messages

    def _read_conversation_layer(self, conversation_id: str,
                                 end_message_id: str | None,
                                 max_layer: int | None) -> list[ConversationMessage]:
        """Read messages from a single conversation (no parent chain)."""
        cur = self.conn.cursor()

        if end_message_id and max_layer is not None:
            cur.execute(
                """SELECT message_id, layer, tombstone, role, content
                   FROM conversations
                   WHERE conversation_id = ? AND message_id <= ? AND layer <= ?
                   ORDER BY message_id, layer DESC""",
                (conversation_id, end_message_id, max_layer),
            )
        elif end_message_id:
            cur.execute(
                """SELECT message_id, layer, tombstone, role, content
                   FROM conversations
                   WHERE conversation_id = ? AND message_id <= ?
                   ORDER BY message_id, layer DESC""",
                (conversation_id, end_message_id),
            )
        elif max_layer is not None:
            cur.execute(
                """SELECT message_id, layer, tombstone, role, content
                   FROM conversations
                   WHERE conversation_id = ? AND layer <= ?
                   ORDER BY message_id, layer DESC""",
                (conversation_id, max_layer),
            )
        else:
            cur.execute(
                """SELECT message_id, layer, tombstone, role, content
                   FROM conversations
                   WHERE conversation_id = ?
                   ORDER BY message_id, layer DESC""",
                (conversation_id,),
            )

        # For each message_id, take the highest layer entry
        messages = []
        seen = set()
        for msg_id, layer, tombstone, role, content in cur.fetchall():
            if msg_id in seen:
                continue
            seen.add(msg_id)
            if not tombstone:
                messages.append(ConversationMessage(
                    ref=MessageRef(conversation_id, msg_id, layer),
                    role=role,
                    content=content,
                ))

        # Sort by message_id (they're sortable UUIDs)
        messages.sort(key=lambda m: m.ref.message_id)
        return messages

    def conv_search_messages(self, conversation_id: str,
                             pattern: str) -> list[ConversationMessage]:
        """Search conversation messages by LIKE pattern on content."""
        all_messages = self.conv_read_messages(conversation_id)
        return [m for m in all_messages if pattern.replace('%', '') in m.content]

    def conv_get_messages(self, refs: list[MessageRef]) -> list[ConversationMessage]:
        """Batch read messages by refs."""
        results = []
        cur = self.conn.cursor()
        for ref in refs:
            cur.execute(
                """SELECT role, content FROM conversations
                   WHERE conversation_id = ? AND message_id = ? AND layer = ?""",
                (ref.conversation_id, ref.message_id, ref.layer),
            )
            row = cur.fetchone()
            if row:
                results.append(ConversationMessage(ref=ref, role=row[0], content=row[1]))
        return results

    def conv_replace_with(self, conversation_id: str,
                          new_messages: list[dict],
                          start_message_id: str | None = None,
                          end_message_id: str | None = None) -> list[MessageRef]:
        """Replace a range of messages with new ones via layer compaction."""
        cur = self.conn.cursor()

        # Find messages in range and max layer
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

        # Tombstone existing messages at new layer
        for msg_id, _ in existing:
            cur.execute(
                """INSERT OR IGNORE INTO conversations
                   (conversation_id, message_id, layer, tombstone, role, content)
                   VALUES (?, ?, ?, 1, '', '')""",
                (conversation_id, msg_id, new_layer),
            )

        # Insert new messages at new layer.
        # Use the first replaced message_id's timestamp prefix so new messages
        # sort in the same position as the replaced range.
        base_id = existing[0][0] if existing else (start_message_id or _sortable_uuid())
        # Parse: "{timestamp}-{seq}-{rand}" and increment seq
        parts = base_id.split('-', 2)
        base_ts = parts[0]
        base_seq = int(parts[1], 16) if len(parts) > 1 else 0
        new_refs = []
        for i, msg in enumerate(new_messages):
            seq = base_seq + i
            rand = uuid.uuid4().hex[:8]
            msg_id = f'{base_ts}-{seq:08x}-{rand}'
            cur.execute(
                """INSERT INTO conversations
                   (conversation_id, message_id, layer, tombstone, role, content)
                   VALUES (?, ?, ?, 0, ?, ?)""",
                (conversation_id, msg_id, new_layer, msg['role'], msg['content']),
            )
            new_refs.append(MessageRef(conversation_id, msg_id, new_layer))

        self.conn.commit()
        return new_refs

    def conv_resolve_ref(self, conversation_id: str) -> ConversationRef:
        """Resolve Latest to a concrete ConversationRef."""
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
