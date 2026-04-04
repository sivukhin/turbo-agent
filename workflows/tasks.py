"""Task management: top-level organizational unit for executions.

Each task has its own turso database for executions/events/conversations.
Tasks are stored in a separate task registry database.
The task's DB path is derived from a configured tasks directory + task_id.

Each task gets a "context conversation" with two system messages:
- title message (task name)
- description message (task description)
All executions fork their root conversation from this context conversation.
Updating task name/description updates these messages in place.
"""

import json
import os
import time
import turso
from workflows.ids import new_id


def task_db_path(tasks_dir: str, task_id: str) -> str:
    return os.path.join(tasks_dir, task_id, 'executions.db')


def task_workdir(tasks_dir: str, task_id: str) -> str:
    return os.path.join(tasks_dir, task_id, 'workspace')


class TaskStore:
    """Manages tasks in a registry database."""

    def __init__(self, db_path: str, tasks_dir: str = '.tasks'):
        self.tasks_dir = tasks_dir
        self.conn = turso.connect(db_path)
        self._migrate()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                labels TEXT NOT NULL DEFAULT '{}',
                color TEXT NOT NULL DEFAULT '',
                needs_input INTEGER NOT NULL DEFAULT 0,
                context_conversation_id TEXT NOT NULL DEFAULT '',
                title_message_id TEXT NOT NULL DEFAULT '',
                description_message_id TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS projects (
                name TEXT PRIMARY KEY,
                created_at REAL NOT NULL DEFAULT 0
            );
        """)
        self.conn.commit()

    def _get_task_store(self, task_id: str):
        """Get the execution Store for a task."""
        from workflows.store import Store
        db = task_db_path(self.tasks_dir, task_id)
        os.makedirs(os.path.dirname(db), exist_ok=True)
        return Store(db)

    def create(self, name, description='', status='pending', labels=None,
               color='') -> dict:
        task_id = new_id()
        labels = labels or {}
        now = time.time()

        # Create context conversation in the task's execution DB
        store = self._get_task_store(task_id)
        conv_id = new_id()
        store.create_conversation(conv_id)
        title_ref = store.conv_append_message(conv_id, 'user', f'Task: {name}')
        desc_ref = store.conv_append_message(conv_id, 'user', description or '(no description)')
        store.close()

        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO tasks (task_id, name, description, status, labels, color, needs_input,
               context_conversation_id, title_message_id, description_message_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)""",
            (task_id, name, description, status, json.dumps(labels), color,
             conv_id, title_ref.message_id, desc_ref.message_id, now),
        )
        self.conn.commit()
        return self.get(task_id)

    _TASK_COLS = ('task_id', 'name', 'description', 'status', 'labels', 'color',
                  'needs_input', 'context_conversation_id', 'title_message_id',
                  'description_message_id', 'created_at')
    _TASK_SELECT = ', '.join(_TASK_COLS)

    def get(self, task_id: str) -> dict:
        cur = self.conn.cursor()
        cur.execute(f"SELECT {self._TASK_SELECT} FROM tasks WHERE task_id = ?", (task_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f'Task {task_id} not found')
        return self._row_to_dict(row)

    def list(self) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(f"SELECT {self._TASK_SELECT} FROM tasks ORDER BY created_at DESC")
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def update(self, task_id: str, **kwargs) -> dict:
        task = self.get(task_id)
        allowed = {'name', 'description', 'status', 'labels', 'color', 'needs_input'}
        sets = []
        params = []
        for k, v in kwargs.items():
            if k not in allowed:
                raise ValueError(f'Cannot update field: {k}')
            if k == 'labels':
                v = json.dumps(v)
            if k == 'needs_input':
                v = int(v)
            sets.append(f'{k} = ?')
            params.append(v)
        if not sets:
            return task

        params.append(task_id)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?", params)
        self.conn.commit()

        # Update context conversation messages if name/description changed
        if 'name' in kwargs or 'description' in kwargs:
            conv_id = task['context_conversation_id']
            if conv_id:
                from workflows.conversation import MessageRef
                store = self._get_task_store(task_id)
                if 'name' in kwargs and task['title_message_id']:
                    store.conv_update_message(
                        MessageRef(conv_id, task['title_message_id'], 0, 'user'),
                        f'Task: {kwargs["name"]}',
                    )
                if 'description' in kwargs and task['description_message_id']:
                    store.conv_update_message(
                        MessageRef(conv_id, task['description_message_id'], 0, 'user'),
                        kwargs['description'] or '(no description)',
                    )
                store.close()

        return self.get(task_id)

    def delete(self, task_id: str):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        self.conn.commit()

    def find_by_prefix(self, prefix: str) -> dict:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE task_id LIKE ?", (prefix + '%',))
        rows = cur.fetchall()
        if len(rows) == 0:
            raise KeyError(f'No task matching prefix: {prefix}')
        if len(rows) > 1:
            raise KeyError(f'Ambiguous prefix: {prefix} matches {len(rows)} tasks')
        return self._row_to_dict(rows[0])

    def create_project(self, name: str) -> dict:
        now = time.time()
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO projects (name, created_at) VALUES (?, ?)",
            (name, now),
        )
        self.conn.commit()
        return {'name': name, 'created_at': now}

    def list_projects(self) -> list[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM projects ORDER BY name")
        explicit = {row[0] for row in cur.fetchall()}
        # Also include projects from task labels
        tasks = self.list()
        for t in tasks:
            p = t['labels'].get('project', '')
            if p:
                explicit.add(p)
        return sorted(explicit)

    def close(self):
        self.conn.close()

    @staticmethod
    def _row_to_dict(row):
        return {
            'task_id': row[0],
            'name': row[1],
            'description': row[2],
            'status': row[3],
            'labels': json.loads(row[4]) if row[4] else {},
            'color': row[5],
            'needs_input': bool(row[6]),
            'context_conversation_id': row[7],
            'title_message_id': row[8],
            'description_message_id': row[9],
            'created_at': row[10],
        }
