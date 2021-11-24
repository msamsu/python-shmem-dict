import os
import uuid
from multiprocessing import shared_memory
from time import sleep


class ForgottenLockError(Exception):
    pass


def check_lock_for_reading(method):
    def foo(structure, *args, **kwargs):
        assert isinstance(structure, ShStructure)
        for ii in range(100):
            if structure.can_read():
                return method(structure, *args, **kwargs)
            else:
                sleep(0.01)
        raise ForgottenLockError(f'Cannot read {structure.name} for its lock')
    return foo


def lock_structure_for_writing(method):
    def foo(structure, *args, **kwargs):
        for ii in range(100):
            try:
                if structure.create_lock():
                    return method(structure, *args, **kwargs)
                else:
                    sleep(0.01)
            finally:
                structure.remove_lock()
        raise ForgottenLockError(f'Cannot write to {structure.name} for its lock')
    return foo


class ShStructure:

    KEYS_TYPE = NotImplemented

    def __init__(self, name, data=None):
        assert isinstance(name, str) and name, name
        self.name = name
        self.keys = []
        self.vals = {}
        if data is None:
            self._load_structure()
        else:
            print(f'saving/overriding data in {self.__class__.__name__}:{name}')
            os.system(f'find /dev/shm/ -name "sharedmem_{self.name}_*" -delete')
            self._save_structure(data)

    def can_read(self):
        try:
            shared_memory.ShareableList(name=f'sharedmem_{self.name}_lock')
        except (FileNotFoundError, ValueError):
            # ValueError: cannot mmap an empty file
            return True
        else:
            return False

    def create_lock(self):
        try:
            shared_memory.ShareableList([1], name=f'sharedmem_{self.name}_lock')
        except FileExistsError:
            return False
        else:
            return True

    def remove_lock(self):
        try:
            lock = shared_memory.ShareableList(name=f'sharedmem_{self.name}_lock')
        except FileNotFoundError:
            pass
        else:
            lock.shm.close()
            lock.shm.unlink()

    def _load_structure(self):
        try:
            self.keys = shared_memory.ShareableList(name=f'sharedmem_{self.name}_keys')
        except FileNotFoundError:
            pass

    @lock_structure_for_writing
    def _save_structure(self, data):
        raise NotImplementedError()

    def _save_keys(self, keys):
        assert isinstance(keys, list), keys
        assert self.KEYS_TYPE, self.KEYS_TYPE
        assert all(isinstance(item, self.KEYS_TYPE) for item in keys), keys
        if set(keys) == set(self.keys):
            return
        if hasattr(self.keys, 'shm'):
            self.keys.shm.close()
            self.keys.shm.unlink()
        self.keys = shared_memory.ShareableList(keys, name=f'sharedmem_{self.name}_keys')

    def _encode(self, val):
        # TODO: typing of `val`
        # recursive dict
        # recursive list
        # pickling the rest?

        if isinstance(val, dict):
            sub_name = f'{self.name}_{uuid.uuid4()}'
            ShDict(sub_name, val)
            return f'recursive_dict:{sub_name}'

        if isinstance(val, list):
            sub_name = f'{self.name}_{uuid.uuid4()}'
            ShList(sub_name, val)
            return f'recursive_list:{sub_name}'

        return val

    def _decode(self, val):
        if isinstance(val, str) and val.startswith('recursive_dict:'):
            return ShDict(val.split(':')[1])
        if isinstance(val, str) and val.startswith('recursive_list:'):
            return ShList(val.split(':')[1])
        return val

    def _save_value(self, key, val):
        if key in self.keys and key not in self.vals:
            self.vals[key] = shared_memory.ShareableList(name=f'sharedmem_{self.name}_val_{key}')
        if key in self.vals:
            self.vals[key].shm.close()
            self.vals[key].shm.unlink()
        val = self._encode(val)
        self.vals[key] = shared_memory.ShareableList([val], name=f'sharedmem_{self.name}_val_{key}')

    @check_lock_for_reading
    def __getitem__(self, key):
        if key not in self.keys:
            # we try to reload `keys` (another process may have changed them)
            self._load_structure()
        if key not in self.keys:
            raise KeyError(key)
        # (re)loading value of the `key` (another process may have changed it)
        self.vals[key] = shared_memory.ShareableList(name=f'sharedmem_{self.name}_val_{key}')
        return self._decode(self.vals[key][0])

    @check_lock_for_reading
    def reconstruct(self, data):
        return self.__class__(self.name, data)


class ShList(ShStructure):

    KEYS_TYPE = int

    @lock_structure_for_writing
    def _save_structure(self, data):
        assert isinstance(data, list), data
        if data:
            for key, val in enumerate(data):
                self._save_value(key, val)
            self._save_keys(list(range(len(data))))

    @lock_structure_for_writing
    def append(self, value):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        act_len = len(self.keys)
        self._save_value(act_len, value)
        self._save_keys(list(range(act_len + 1)))

    @check_lock_for_reading
    def __len__(self):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        return len(self.keys)

    @check_lock_for_reading
    def __iter__(self):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        for key in self.keys:
            # (re)loading value of the `key` (another process may have changed it)
            self.vals[key] = shared_memory.ShareableList(name=f'sharedmem_{self.name}_val_{key}')
            yield self._decode(self.vals[key][0])


class ShDict(ShStructure):

    KEYS_TYPE = str

    @lock_structure_for_writing
    def _save_structure(self, data):
        assert isinstance(data, dict), data
        if data:
            for key, val in data.items():
                self._save_value(key, val)
            self._save_keys(list(sorted(data.keys())))

    @lock_structure_for_writing
    def __setitem__(self, key, value):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        self._save_value(key, value)
        self._save_keys(list(self.keys) + [key])

    @lock_structure_for_writing
    def __delitem__(self, key):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        if key in self.keys:
            keys = list(self.keys)
            keys.remove(key)
            self._save_keys(keys)
        if key in self.vals:
            self.vals[key].shm.close()
            self.vals[key].shm.unlink()
            del self.vals[key]

    @check_lock_for_reading
    def items(self):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        for key in self.keys:
            yield key, self[key]

    @check_lock_for_reading
    def setdefault(self, key, default):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        if key not in self.keys:
            self[key] = default
        return self[key]

    @check_lock_for_reading
    def update(self, dd):
        assert isinstance(dd, dict), dd
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        for key, val in dd.items():
            self[key] = val

    @check_lock_for_reading
    def get(self, key, default):
        # we try to reload `keys` (another process may have changed them)
        self._load_structure()
        if key in self.keys:
            return self[key]
        return default


class UnitTest:

    def run(self):
        foo = ShDict('foo', {})
        print(foo.__dict__)

        foo = ShDict('foo', {'aaa': 'bbb'})
        assert foo['aaa'] == 'bbb', foo.__dict__

        foo = ShDict('foo', {'aaa': ['bbb', 'ddd']})
        assert foo['aaa'][0] == 'bbb', foo.__dict__
        assert set(list(foo['aaa'])) == set(['bbb', 'ddd']), foo.__dict__
        foo['aaa'].append({'dir': 1, 'rate': 12.2})
        assert dict(foo['aaa'][2].items()) == {'dir': 1, 'rate': 12.2}, foo.__dict__

        return foo
