import pickle
import turso
from workflows.engine import ExecutionState, WorkflowState


def _pickle_or_none(val):
    return pickle.dumps(val) if val is not None else None


def _unpickle_or_none(data):
    return pickle.loads(data) if data is not None else None


class Store:
    """Persists execution state in a Turso (Limbo) database."""

    def __init__(self, db_path: str):
        self.conn = turso.connect(db_path)
        self._migrate()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                root TEXT NOT NULL,
                next_id INTEGER NOT NULL DEFAULT 1,
                step INTEGER NOT NULL DEFAULT 0,
                finished INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS workflows (
                exec_id TEXT NOT NULL,
                wf_id TEXT NOT NULL,
                name TEXT NOT NULL,
                args BLOB NOT NULL,
                checkpoint BLOB,
                status TEXT NOT NULL DEFAULT 'running',
                wait_deps BLOB,
                wait_mode TEXT,
                result BLOB,
                send_val BLOB,
                PRIMARY KEY (exec_id, wf_id),
                FOREIGN KEY (exec_id) REFERENCES executions(id)
            );
        """)
        self.conn.commit()

    def save(self, exec_id: str, state: ExecutionState):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO executions (id, root, next_id, step, finished)
               VALUES (?, ?, ?, ?, ?)""",
            (exec_id, state.root, state.next_id, state.step, int(state.finished)),
        )
        cur.execute("DELETE FROM workflows WHERE exec_id = ?", (exec_id,))
        for wf_id, wf in state.workflows.items():
            cur.execute(
                """INSERT INTO workflows
                   (exec_id, wf_id, name, args, checkpoint, status,
                    wait_deps, wait_mode, result, send_val)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    exec_id,
                    wf_id,
                    wf.name,
                    pickle.dumps(wf.args),
                    _pickle_or_none(wf.checkpoint),
                    wf.status,
                    pickle.dumps(wf.wait_deps) if wf.wait_deps else None,
                    wf.wait_mode,
                    _pickle_or_none(wf.result),
                    _pickle_or_none(wf._send_val),
                ),
            )
        self.conn.commit()

    def load(self, exec_id: str) -> ExecutionState:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT root, next_id, step, finished FROM executions WHERE id = ?",
            (exec_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"Execution {exec_id} not found")
        root, next_id, step, finished = row

        cur.execute(
            """SELECT wf_id, name, args, checkpoint, status,
                      wait_deps, wait_mode, result, send_val
               FROM workflows WHERE exec_id = ?""",
            (exec_id,),
        )
        workflows = {}
        for row in cur.fetchall():
            wf_id, name, args_b, cp_b, status, deps_b, wait_mode, result_b, sv_b = row
            wf = WorkflowState(
                name=name,
                args=pickle.loads(args_b),
                checkpoint=_unpickle_or_none(cp_b),
                status=status,
                wait_deps=pickle.loads(deps_b) if deps_b is not None else [],
                wait_mode=wait_mode,
                result=_unpickle_or_none(result_b),
            )
            wf._send_val = _unpickle_or_none(sv_b)
            workflows[wf_id] = wf

        return ExecutionState(
            workflows=workflows,
            root=root,
            next_id=next_id,
            step=step,
            finished=bool(finished),
        )

    def list_all(self) -> list[tuple[str, ExecutionState]]:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM executions ORDER BY id")
        results = []
        for (exec_id,) in cur.fetchall():
            results.append((exec_id, self.load(exec_id)))
        return results

    def close(self):
        self.conn.close()
