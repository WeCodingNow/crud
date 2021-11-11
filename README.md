# CRUD

[![Run static analysis](https://github.com/tarantool/crud/actions/workflows/check_on_push.yaml/badge.svg)](https://github.com/tarantool/crud/actions/workflows/check_on_push.yaml)
[![Run tests](https://github.com/tarantool/crud/actions/workflows/test_on_push.yaml/badge.svg)](https://github.com/tarantool/crud/actions/workflows/test_on_push.yaml)
[![Coverage Status](https://coveralls.io/repos/github/tarantool/crud/badge.svg?branch=master)](https://coveralls.io/github/tarantool/crud?branch=master)

The `CRUD` module allows to perform CRUD operations on the cluster.
It also provides the `crud-storage` role for
[Tarantool Cartridge](https://github.com/tarantool/cartridge).

## API

The CRUD operations should be called from router.
All storage replica sets should call `crud.init_storage()`
(or enable the `crud-storage` role)
first to initialize storage-side functions that are used to manipulate data
across the cluster.
All routers should call `crud.init_router()` (or enable the `crud-router` role)
to make `crud` functions callable via `net.box`.

All operations return a table that contains rows (tuples) and metadata
(space format).
It can be used to convert received tuples to objects via `crud.unflatten_rows` function.

For example:

```lua
res, err = crud.select('customers')
res
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 12477, 'Elizabeth', 12]
  - [2, 21401, 'David', 33]
...
crud.unflatten_rows(res.rows, res.metadata)
---
- - bucket_id: 12477
    age: 12
    name: Elizabeth
    id: 1
  - bucket_id: 21401
    age: 33
    name: David
    id: 2
...
```

**Notes:**

* A space should have a format.

**Sharding key and bucket id calculation**

*Sharding key* is a set of tuple field values used for calculation *bucket ID*.
*Sharding key definition* is a set of tuple field names that describe what
tuple field should be a part of sharding key. *Bucket ID* determines which
replicaset stores certain data. Function that used for bucket ID calculation is
named *sharding function*.

By default CRUD calculates bucket ID using primary key and a function
`vshard.router.bucket_id_strcrc32(key)`, it happen automatically and doesn't
require any actions from user side. However, for operations that accepts
tuple/object bucket ID can be specified as tuple/object field as well as
`opts.bucket_id` value.

Starting from 0.10.0 users who don't want to use primary key as a sharding key
may set custom sharding key definition as a part of [DDL
schema](https://github.com/tarantool/ddl#input-data-format) or insert manually
to the space `_ddl_sharding_key` (for both cases consider a DDL module
documentation). As soon as sharding key for a certain space is available in
`_ddl_sharding_key` space CRUD will use it for bucket ID calculation
automatically. Note that CRUD methods `delete()`, `get()` and `update()`
requires that sharding key must be a part of primary key.

Table below describe what operations supports custom sharding key:

| CRUD method                      | Sharding key support       |
| -------------------------------- | -------------------------- |
| `get()`                          | Yes                        |
| `insert()` / `insert_object()`   | Yes                        |
| `delete()`                       | Yes                        |
| `replace()` / `replace_object()` | Yes                        |
| `upsert()` / `upsert_object()`   | Yes                        |
| `select()` / `pairs()`           | Yes                        |
| `update()`                       | Yes                        |
| `upsert()` / `upsert_object()`   | Yes                        |
| `replace() / replace_object()`   | Yes                        |
| `min()` / `max()`                | No (not required)          |
| `cut_rows()` / `cut_objects()`   | No (not required)          |
| `truncate()`                     | No (not required)          |
| `len()`                          | No (not required)          |

Current limitations for using custom sharding key:

- It's not possible to update sharding keys automatically when schema is
  updated on storages, see
  [#212](https://github.com/tarantool/crud/issues/212). However it is possible
  to do it manually with `require('crud.common.sharding_key').update_cache()`.
- No support of JSON path for sharding key, see
  [#219](https://github.com/tarantool/crud/issues/219).
- `primary_index_fieldno_map` is not cached, see
  [#243](https://github.com/tarantool/crud/issues/243).

### Insert

```lua
-- Insert tuple
local result, err = crud.insert(space_name, tuple, opts)
-- Insert object
local result, err = crud.insert_object(space_name, object, opts)
```

where:

* `space_name` (`string`) - name of the space to insert an object
* `tuple` / `object` (`table`) - tuple/object to insert
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one inserted row, error.

**Example:**

```lua
crud.insert('customers', {1, box.NULL, 'Elizabeth', 23})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 23]
...
crud.insert_object('customers', {
    id = 2, name = 'Elizabeth', age = 24,
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [2, 401, 'Elizabeth', 24]
...
```

### Batch insert

Right now CRUD cannot provide batch insert with full consistency.
CRUD offers batch insert with partial consistency. That means
that full consistency can be provided only on single replicaset
using `box` transactions.

```lua
-- Batch insert tuples
local result, err = crud.batch_insert(space_name, tuples, opts)
-- Batch insert objects
local result, err = crud.batch_insert_object(space_name, objects, opts)
```

where:

* `space_name` (`string`) - name of the space to insert an object
* `tuples` / `objects` (`table`) - array of tuples/objects to insert
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains inserted rows, array of errors (
one error corresponds to one replicaset for which the error occurred).
Error object can contain `tuple` field. This field contains the tuple
for which the error occurred.

**Example:**

```lua
crud.batch_insert('customers', {
  {1, box.NULL, 'Elizabeth', 23},
  {2, box.NULL, 'Anastasia', 22},
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 23]
  - [2, 401, 'Anastasia', 22]
...
crud.batch_insert_object('customers', {
    {id = 3, name = 'Elizabeth', age = 24},
    {id = 10, name = 'Anastasia', age = 21},
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 2804, 'Elizabeth', 24]
  - [10, 569, 'Anastasia', 21]

-- Partial success
local res, errs = crud.batch_insert_object('customers', {
    {id = 22, name = 'Alex', age = 34},
    {id = 3, name = 'Anastasia', age = 22},
    {id = 5, name = 'Sergey', age = 25},
})
---
res
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [5, 1172, 'Sergey', 25],
  - [22, 655, 'Alex', 34],

errs
- [err = 'Duplicate key exists ...', ..., tuple = [3, 2804, 'Anastasia', 22]]
...
```

### Get

```lua
local object, err = crud.get(space_name, key, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `fields` (`?table`) - field names for getting only a subset of fields
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `mode` (`?string`, `read` or `write`) - if `write` is specified then `get` is
    performed on master
  * `prefer_replica` (`?boolean`) - if `true` then the preferred target is one of
    the replicas
  * `balance` (`?boolean`) - use replica according to vshard load balancing policy

Returns metadata and array contains one row, error.

**Example:**

```lua
crud.get('customers', 1)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 23]
...
```

### Update

```lua
local object, err = crud.update(space_name, key, operations, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update)
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one updated row, error.

**Example:**

```lua
crud.update('customers', 1, {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 24]
...
```

### Delete

```lua
local object, err = crud.delete(space_name, key, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one deleted row (empty for vinyl), error.

**Example:**

```lua
crud.delete('customers', 1)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 24]
```

### Replace

```lua
-- Replace tuple
local result, err = crud.replace(space_name, tuple, opts)
-- Replace object
local result, err = crud.replace_object(space_name, object, opts)
```

where:

* `space_name` (`string`) - name of the space
* `tuple` / `object` (`table`) - tuple/object to insert or replace exist one
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns inserted or replaced rows and metadata or nil with error.

**Example:**

```lua
crud.replace('customers', {1, box.NULL, 'Alice', 22})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Alice', 22]
...
crud.replace_object('customers', {
    id = 1, name = 'Alice', age = 22,
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Alice', 22]
...
```

### Upsert

```lua
-- Upsert tuple
local result, err = crud.upsert(space_name, tuple, operations, opts)
-- Upsert object
local result, err = crud.upsert_object(space_name, tuple, operations, opts)
```

where:

* `space_name` (`string`) - name of the space
* `tuple` / `object` (`table`) - tuple/object to insert if there is no existing tuple which matches the key fields
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update) if there is an existing tuple which matches the key fields of tuple
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and empty array of rows or nil, error.

**Example:**

```lua
crud.upsert('customers',
    {1, box.NULL, 'Alice', 22},
    {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows: []
...
crud.upsert_object('customers',
    {id = 1, name = 'Alice', age = 22},
    {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows: []
...
```


### Select

`CRUD` supports multi-conditional selects, treating a cluster as a single space.
The conditions may include field names or numbers, as well as index names.
The recommended first condition is a TREE index; this helps reducing the number
of tuples to scan. Otherwise a full scan is performed.

```lua
local objects, err = crud.select(space_name, conditions, opts)
```

where:

* `space_name` (`string`) - name of the space
* `conditions` (`?table`) - array of [select conditions](#select-conditions)
* `opts`:
  * `first` (`?number`) - the maximum count of the objects to return.
     If negative value is specified, the last objects are returned
     (`after` option is required in this case).
  * `after` (`?table`) - tuple after which objects should be selected
  * `batch_size` (`?number`) - number of tuples to process per one request to storage
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `force_map_call` (`?boolean`) - if `true`
     then the map call is performed without any optimizations even
     if full primary key equal condition is specified
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `fields` (`?table`) - field names for getting only a subset of fields
  * `mode` (`?string`, `read` or `write`) - if `write` is specified then `select` is
    performed on master
  * `prefer_replica` (`?boolean`) - if `true` then the preferred target is one of
    the replicas
  * `balance` (`?boolean`) - use replica according to vshard load balancing policy


Returns metadata and array of rows, error.

#### Select conditions

Select conditions are very similar to Tarantool update
[operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update).

Each condition is a table `{operator, field-identifier, value}`:

* Supported operators are: `=` (or `==`), `>`, `>=`, `<`, `<=`.
* Field identifier can be field name, field number, or index name.

**Example:**

```lua
crud.select('customers', {{'<=', 'age', 35}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [5, 1172, 'Jack', 35]
  - [3, 2804, 'David', 33]
  - [6, 1064, 'William', 25]
  - [7, 693, 'Elizabeth', 18]
  - [1, 477, 'Elizabeth', 12]
...
```

**Note**: tuples are sorted by age because space has index `age`.
Otherwise, tuples are sorted by primary key.

See more examples of select queries [here.](https://github.com/tarantool/crud/blob/master/doc/select.md)

### Pairs

You can iterate across a distributed space using the `crud.pairs` function.
Its arguments are the same as [`crud.select`](#select) arguments,
but negative `first` values aren't allowed.
User could pass use_tomap flag (false by default) to iterate over flat tuples or objects.

**Example:**

```lua
local tuples = {}
for _, tuple in crud.pairs('customers', {{'<=', 'age', 35}}, {use_tomap = false}) do
    -- {5, 1172, 'Jack', 35}
    table.insert(tuples, tuple)
end

local objects = {}
for _, object in crud.pairs('customers', {{'<=', 'age', 35}}, {use_tomap = true}) do
    -- {id = 5, name = 'Jack', bucket_id = 1172, age = 35}
    table.insert(objects, object)
end
```

See more examples of pairs queries [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Min and max

```lua
-- Find the minimum value in the specified index
local result, err = crud.min(space_name, 'age', opts)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 12]

-- Find the maximum value in the specified index
local result, err = crud.max(space_name, 'age', opts)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [5, 1172, 'Jack', 35]
```

### Cut extra rows

You could use `crud.cut_rows` function to cut off scan key and primary key values that were merged to the select/pairs partial result (select/pairs with `fields` option).

```lua
local res, err = crud.cut_rows(rows, metadata, fields)
```

where:

* `rows` (`table`) - array of tuples for cutting
* `matadata` (`?metadata`) - metadata about `rows` fields
* `fields` (`table`) - field names of fields that should be contained in the result

Returns metadata and array of rows, error.

See more examples of `crud.cut_rows` usage [here](https://github.com/tarantool/crud/blob/master/doc/select.md) and [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Cut extra objects

If you use `pairs` with `use_tomap` flag and you need to cut off scan key and primary key values that were merged to the pairs partial result (pairs with `fields` option) you should use `crud.cut_objects`.

```lua
local new_objects = crud.cut_objects(objects, fields)
```

where:

* `objects` (`table`) - array of objects for cutting
* `fields` (`table`) - field names of fields that should be contained in the result

Returns array of objects.

See more examples of `crud.cut_objects` usage [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Truncate

```lua
-- Truncate space
local result, err = crud.truncate(space_name, opts)
```

where:

* `space_name` (`string`) - name of the space
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns true or nil with error.

**Example:**

```lua
#crud.select('customers', {{'<=', 'age', 35}})
---
- 1
...
crud.truncate('customers', {timeout = 2})
---
- true
...
#crud.select('customers', {{'<=', 'age', 35}})
---
- 0
...
```

### Len

```lua
-- Calculates the number of tuples in the space for memtx engine
-- Calculates the maximum approximate number of tuples in the space for vinyl engine
local result, err = crud.len(space_name, opts)
```

where:

* `space_name` (`string|number`) - name of the space as well
  as numerical id of the space
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns number or nil with error.

**Example:**

Using `memtx`:

```lua
#crud.select('customers')
---
- 5
...
crud.len('customers', {timeout = 2})
---
- 5
...
```

Using `vinyl`:

```lua
crud.len('customers')
---
- 0
...
crud.delete('customers', 1)
---
...
crud.len('customers')
---
- 1
...
```

## Cartridge roles

`cartridge.roles.crud-storage` is a Tarantool Cartridge role that depends on the
`vshard-storage` role, but also initializes functions that
are used on the storage side to perform CRUD operations.

`cartridge.roles.crud-router` is a role that depends on the
`vshard-router` role, but also exposes public `crud` functions in the global
scope, so that you can call them via `net.box`.


### Usage

1. Add `crud` to dependencies in the project rockspec.

**Note**: it's better to use tagged version than `scm-1`.
Check the latest available [release](https://github.com/tarantool/crud/releases) tag and use it.

```lua
-- <project-name>-scm-1.rockspec
dependencies = {
    ...
    'crud == <the-latest-tag>-1',
    ...
}
```

2. Create the role that stores your data and depends on `crud-storage`.

```lua
-- app.roles.customers-storage.lua
local cartridge = require('cartridge')

return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'age', type = 'number'},
                },
                if_not_exists = true,
            })
            customers_space:create_index('id', {
                parts = { {field ='id', is_nullable = false} },
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = { {field ='bucket_id', is_nullable = false} },
                if_not_exists = true,
            })
            customers_space:create_index('age', {
                parts = { {field ='age'} },
                unique = false,
                if_not_exists = true,
            })
        end,
        dependencies = {'cartridge.roles.crud-storage'},
    }
```

```lua
-- app.roles.customers-router.lua
local cartridge = require('cartridge')
return {
        role_name = 'customers-router',
        dependencies = {'cartridge.roles.crud-router'},
    }
```

3. Start the application and create `customers-storage` and
   `customers-router` replica sets.

4. Don't forget to bootstrap vshard.

Now your cluster contains storages that are configured to be used for
CRUD-operations.
You can simply call CRUD functions on the router to insert, select, and update
data across the cluster.
