import pickle
import turso
from workflows.engine import ExecutionState, Message


class Store:
    """Persists execution state and messages in a Turso database."""

    def __init__(self, db_path: str):
        self.conn = turso.connect(db_path)
        self._migrate()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                state BLOB NOT NULL,
                last_processed_msg_id INTEGER NOT NULL DEFAULT 0,
                finished INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS messages (
                msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                workflow_id TEXT,
                category TEXT NOT NULL,
                type TEXT NOT NULL,
                payload BLOB
            );

            CREATE INDEX IF NOT EXISTS idx_messages_inbox
                ON messages (execution_id, category, msg_id)
                WHERE category = 'inbox';

            CREATE INDEX IF NOT EXISTS idx_messages_outbox
                ON messages (execution_id, category, msg_id)
                WHERE category = 'outbox';
        """)
        self.conn.commit()

    def save_state(self, execution_id: str, state: ExecutionState,
                   last_processed_msg_id: int | None = None):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT last_processed_msg_id FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = cur.fetchone()
        lp = last_processed_msg_id if last_processed_msg_id is not None else (row[0] if row else 0)

        cur.execute(
            """INSERT OR REPLACE INTO executions
               (execution_id, state, last_processed_msg_id, finished)
               VALUES (?, ?, ?, ?)""",
            (execution_id, pickle.dumps(state), lp, int(state.finished)),
        )
        self.conn.commit()

    def load_state(self, execution_id: str) -> tuple[ExecutionState, int]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT state, last_processed_msg_id FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"Execution {execution_id} not found")
        return pickle.loads(row[0]), row[1]

    def append_message(self, execution_id: str, workflow_id: str | None,
                       category: str, msg_type: str, payload: dict):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO messages (execution_id, workflow_id, category, type, payload)
               VALUES (?, ?, ?, ?, ?)""",
            (execution_id, workflow_id, category, msg_type, pickle.dumps(payload)),
        )
        self.conn.commit()

    def read_inbox(self, execution_id: str, after_msg_id: int = 0) -> list[Message]:
        cur = self.conn.cursor()
        cur.execute(
            """SELECT msg_id, execution_id, workflow_id, category, type, payload
               FROM messages
               WHERE execution_id = ? AND category = 'inbox' AND msg_id > ?
               ORDER BY msg_id""",
            (execution_id, after_msg_id),
        )
        return [
            Message(
                msg_id=row[0],
                execution_id=row[1],
                workflow_id=row[2],
                category=row[3],
                type=row[4],
                payload=pickle.loads(row[5]),
            )
            for row in cur.fetchall()
        ]

    def read_outbox(self, execution_id: str, after_msg_id: int = 0) -> list[Message]:
        cur = self.conn.cursor()
        cur.execute(
            """SELECT msg_id, execution_id, workflow_id, category, type, payload
               FROM messages
               WHERE execution_id = ? AND category = 'outbox' AND msg_id > ?
               ORDER BY msg_id""",
            (execution_id, after_msg_id),
        )
        return [
            Message(
                msg_id=row[0],
                execution_id=row[1],
                workflow_id=row[2],
                category=row[3],
                type=row[4],
                payload=pickle.loads(row[5]),
            )
            for row in cur.fetchall()
        ]

    def list_executions(self) -> list[tuple[str, ExecutionState]]:
        cur = self.conn.cursor()
        cur.execute("SELECT execution_id, state FROM executions ORDER BY execution_id")
        return [(row[0], pickle.loads(row[1])) for row in cur.fetchall()]

    def close(self):
        self.conn.close()
