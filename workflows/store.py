import pickle
import turso
from workflows.engine import ExecutionState, Event
from workflows.events import serialize_payload, deserialize_payload, payload_type_name


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
        """Append an event. payload is a typed dataclass from workflows.events."""
        cur = self.conn.cursor()
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

    def close(self):
        self.conn.close()
