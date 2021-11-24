# python-shmem-dict
Python 3.8 nested shared memory dict / list

Create ShDict in the owner process:
```python
from shmemdict import ShDict
init_data = {}
foo = ShDict('dict_name', init_data)
foo['key'] = 'val'
print(list(foo.items()))
```

Access it from a different one:
```python
from shmemdict import ShDict
foo = ShDict('dict_name')
print(list(foo.items()))
```
