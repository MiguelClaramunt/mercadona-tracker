from dataclasses import dataclass, field
from typing import Optional, Any
from mercatracker import db

@dataclass
class SetHandler:
    conn: Optional[Any] = None
    ymd: int = 0
    supermarket_id: int = 0
    sets: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.conn and self.ymd and self.supermarket_id:
            self.load_cache()

    def init_set(self, *set_names):
        for name in set_names:
            if name not in self.sets:
                self.sets[name] = set()

    def update_set(self, name_or_dict, elements=None, reset=False):
        if isinstance(name_or_dict, dict):
            for set_name, set_elements in name_or_dict.items():
                if set_name in self.sets:
                    if reset:
                        self.reset_set(set_name)
                    self.sets[set_name].update(set_elements)
                else:
                    self.sets[set_name] = set(set_elements)
        else:
            set_name = name_or_dict
            if set_name in self.sets:
                if reset:
                    self.reset_set(set_name)
                self.sets[set_name].update(elements)
            else:
                self.sets[set_name] = set(elements) if elements is not None else set()

    def reset_set(self, *set_names):
        if set_names:
            for name in set_names:
                if name in self.sets:
                    self.sets[name] = set()
        else:
            self.sets = {}
        self.save_cache()

    def __getitem__(self, name: str):
        if name not in self.sets:
            self.sets[name] = set()
        return self.sets[name]

    def __getattr__(self, name: str):
        if name in self.sets:
            return self.sets[name]
        raise AttributeError(f"'SetHandler' has no attribute '{name}'")

    def load_cache(self):
        if not (self.conn and self.ymd and self.supermarket_id):
            return
        loaded = db.load_set_cache(self.conn, self.ymd, self.supermarket_id)
        for set_name, set_data in loaded.items():
            self.sets[set_name] = set_data

    def save_cache(self):
        if not (self.conn and self.ymd and self.supermarket_id):
            return
        for set_name, set_data in self.sets.items():
            if set_data:  # Only save non-empty sets
                db.write_set_cache(self.conn, self.ymd, self.supermarket_id, set_name, set_data)