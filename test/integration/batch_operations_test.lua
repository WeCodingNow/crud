local fio = require('fio')

local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local pgroup = t.group('batch_operations', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_non_existent_space = function(g)
    -- batch_insert
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- batch_insert_object
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
end

pgroup.test_batch_insert_object_get = function(g)
    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 2, name = 'Anna'})

    -- batch_insert_object
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert_object again
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)
end

pgroup.test_batch_insert_get = function(g)
    -- batch_insert
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert again
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert again
    -- fails for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)
end

pgroup.test_batch_insert_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'customers',
        {
            {15, box.NULL, 'Fedor', 59},
            {25, box.NULL, 'Anna', 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- batch_insert
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_batch_insert_object_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 15, name = 'Fedor', age = 59},
            {id = 25, name = 'Anna', age = 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- batch_insert_object
    local result, errs = g.cluster.main_server.net_box:call('crud.batch_insert_object', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_opts_not_damaged = function(g)
    -- batch insert
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.batch_insert('customers', {
            {1, box.NULL, 'Alex', 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)

    -- batch insert_object
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.batch_insert_object('customers', {
            {id = 2, name = 'Fedor', age = 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)
end
