import os
import pickle
from dataclasses import dataclass, field


class NamedSet:
    def __init__(self, name: str, set_handler):
        self.name = name
        self.set_handler = set_handler

    def __sub__(self, other):
        if isinstance(other, NamedSet):
            return self.set_handler.sets[self.name] - self.set_handler.sets[other.name]
        elif isinstance(other, set):
            return self.set_handler.sets[self.name] - other
        else:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, NamedSet):
            return self.set_handler.sets[self.name] == self.set_handler.sets[other.name]
        elif isinstance(other, set):
            return self.set_handler.sets[self.name] == other
        else:
            return False


@dataclass
class SetHandler:
    cache_file: str = "set_cache.pkl"
    sets: dict = field(default_factory=dict)

    def __post_init__(self):
        self.load_cache()

    def init_set(self, *set_names):
        """Initialize new sets for each provided set name."""
        for name in set_names:
            if name not in self.sets:
                self.sets[name] = set()

    def update_set(self, name_or_dict, elements=None, reset=False):
        """
        Update one or multiple sets with new elements.
        
        Args:
            name_or_dict: Either a set name (str) or a dictionary of {set_name: elements}
            elements: Iterable of elements to add (only used if name_or_dict is str)
            reset: Whether to reset sets before updating
        """
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

    def reset_set(self, *set_names, reset=True):
        """Reset specified sets to empty. If no set names are provided, reset all sets."""
        if set_names:
            for name in set_names:
                if name in self.sets:
                    self.sets[name] = set()
        else:
            # Reset all sets if no names are provided
            self.sets = {}
        # Save the updated sets to the cache
        self.save_cache()

    def __getitem__(self, name: str):
        if name not in self.sets:
            self.sets[name] = set()
        return self.sets[name]

    def __getattr__(self, name: str):
        if name in self.sets:
            return self.sets[name]
        else:
            raise AttributeError(f"'SetHandler' has no attribute '{name}'")

    def save_cache(self):
        with open(self.cache_file, "wb") as f:
            pickle.dump(self.sets, f)

    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "rb") as f:
                self.sets = pickle.load(f)
        else:
            self.sets = {}
