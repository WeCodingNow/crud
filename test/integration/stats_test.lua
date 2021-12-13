local fio = require('fio')
local clock = require('clock')
local t = require('luatest')

local stats_registry_common = require('crud.stats.registry_common')

local g = t.group('stats_integration')
local helpers = require('test.helper')

local space_name = 'customers'
local unknown_space_name = 'non_existing_space'

g.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })
    g.cluster:start()
    g.router = g.cluster:server('router').net_box

    helpers.prepare_simple_functions(g.router)
    g.router:eval("crud = require('crud')")
    g.router:eval("crud.enable_stats()")

    t.assert_equals(helpers.is_space_exist(g.router, space_name), true)
    t.assert_equals(helpers.is_space_exist(g.router, unknown_space_name), false)
end)

g.after_all(function(g)
    helpers.stop_cluster(g.cluster)
end)

g.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, space_name)
end)

function g:get_stats(space_name)
    return self.router:eval("return crud.stats(...)", { space_name })
end

-- If there weren't any operations, space stats is {}.
-- To compute stats diff, this helper return real stats
-- if they're already present or default stats if
-- this operation of space hasn't been observed yet.
local function get_before_stats(space_stats, op)
    if space_stats[op] ~= nil then
        return space_stats[op]
    else
        return stats_registry_common.build_collectors(op)
    end
end

local eval = {
    pairs = [[
        local space_name = select(1, ...)
        local conditions = select(2, ...)

        local result = {}
        for _, v in crud.pairs(space_name, conditions) do
            table.insert(result, v)
        end

        return result
    ]],

    pairs_pcall = [[
        local space_name = select(1, ...)
        local conditions = select(2, ...)

        local _, err = pcall(crud.pairs, space_name, conditions)

        return nil, tostring(err)
    ]],
}

-- Call some operations on existing
-- spaces and ensure statistics is updated.
local simple_operation_cases = {
    insert = {
        func = 'crud.insert',
        args = {
            space_name,
            { 12, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
        },
        op = 'insert',
    },
    insert_object = {
        func = 'crud.insert_object',
        args = {
            space_name,
            { id = 13, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
        },
        op = 'insert',
    },
    get = {
        func = 'crud.get',
        args = { space_name, { 12 } },
        op = 'get',
    },
    select = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 3 }} },
        op = 'select',
    },
    pairs = {
        eval = eval.pairs,
        args = { space_name, {{ '==', 'id_index', 3 }} },
        op = 'select',
    },
    replace = {
        func = 'crud.replace',
        args = {
            space_name,
            { 12, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
        },
        op = 'replace',
    },
    replace_object = {
        func = 'crud.replace_object',
        args = {
            space_name,
            { id = 12, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
        },
        op = 'replace',
    },
    update = {
        prepare = function(g)
            helpers.insert_objects(g, space_name, {{
                id = 15, name = 'Ivan', last_name = 'Ivanov',
                age = 20, city = 'Moscow'
            }})
        end,
        func = 'crud.update',
        args = { space_name, 12, {{'+', 'age', 10}} },
        op = 'update',
    },
    upsert = {
        func = 'crud.upsert',
        args = {
            space_name,
            { 16, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
            {{'+', 'age', 1}},
        },
        op = 'upsert',
    },
    upsert_object = {
        func = 'crud.upsert_object',
        args = {
            space_name,
            { id = 17, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
            {{'+', 'age', 1}}
        },
        op = 'upsert',
    },
    delete = {
        func = 'crud.delete',
        args = { space_name, { 12 } },
        op = 'delete',
    },
    truncate = {
        func = 'crud.truncate',
        args = { space_name },
        op = 'truncate',
    },
    len = {
        func = 'crud.len',
        args = { space_name },
        op = 'len',
    },
    min = {
        func = 'crud.min',
        args = { space_name },
        op = 'borders',
    },
    max = {
        func = 'crud.max',
        args = { space_name },
        op = 'borders',
    },
    insert_error = {
        func = 'crud.insert',
        args = { space_name, { 'id' } },
        op = 'insert',
        expect_error = true,
    },
    insert_object_error = {
        func = 'crud.insert_object',
        args = { space_name, { 'id' } },
        op = 'insert',
        expect_error = true,
    },
    get_error = {
        func = 'crud.get',
        args = { space_name, { 'id' } },
        op = 'get',
        expect_error = true,
    },
    select_error = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 'sdf' }} },
        op = 'select',
        expect_error = true,
    },
    pairs_error = {
        eval = eval.pairs,
        args = { space_name, {{ '%=', 'id_index', 'sdf' }} },
        op = 'select',
        expect_error = true,
        pcall = true,
    },
    replace_error = {
        func = 'crud.replace',
        args = { space_name, { 'id' } },
        op = 'replace',
        expect_error = true,
    },
    replace_object_error = {
        func = 'crud.replace_object',
        args = { space_name, { 'id' } },
        op = 'replace',
        expect_error = true,
    },
    update_error = {
        func = 'crud.update',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'update',
        expect_error = true,
    },
    upsert_error = {
        func = 'crud.upsert',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'upsert',
        expect_error = true,
    },
    upsert_object_error = {
        func = 'crud.upsert_object',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'upsert',
        expect_error = true,
    },
    delete_error = {
        func = 'crud.delete',
        args = { space_name, { 'id' } },
        op = 'delete',
        expect_error = true,
    },
    min_error = {
        func = 'crud.min',
        args = { space_name, 'badindex' },
        op = 'borders',
        expect_error = true,
    },
    max_error = {
        func = 'crud.max',
        args = { space_name, 'badindex' },
        op = 'borders',
        expect_error = true,
    },
}

for name, case in pairs(simple_operation_cases) do
    local test_name = ('test_%s'):format(name)

    if case.prepare ~= nil then
        g.before_test(test_name, case.prepare)
    end

    g[test_name] = function(g)
        -- Collect stats before call.
        local stats_before = g:get_stats(space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local before_start = clock.monotonic()

        local _, err
        if case.eval ~= nil then
            if case.pcall then
                _, err = pcall(g.router.eval, g.router, case.eval, case.args)
            else
                _, err = g.router:eval(case.eval, case.args)
            end
        else
            _, err = g.router:call(case.func, case.args)
        end

        local after_finish = clock.monotonic()

        if case.expect_error ~= true then
            t.assert_equals(err, nil)
        else
            t.assert_not_equals(err, nil)
        end

        -- Collect stats after call.
        local stats_after = g:get_stats(space_name)
        t.assert_type(stats_after, 'table')
        t.assert_not_equals(stats_after[case.op], nil)

        -- Expecting 'ok' metrics to change on `expect_error == false`
        -- or 'error' to change otherwise.
        local changed, unchanged
        if case.expect_error == true then
            changed = 'error'
            unchanged = 'ok'
        else
            unchanged = 'error'
            changed = 'ok'
        end

        local op_before = get_before_stats(stats_before, case.op)
        local changed_before = op_before[changed]
        local changed_after = stats_after[case.op][changed]

        t.assert_equals(changed_after.count - changed_before.count, 1,
            'Expected count incremented')

        local ok_latency_max = math.max(changed_before.latency, after_finish - before_start)

        t.assert_gt(changed_after.latency, 0,
            'Changed latency has appropriate value')
        t.assert_le(changed_after.latency, ok_latency_max,
            'Changed latency has appropriate value')

        local time_diff = changed_after.time - changed_before.time

        t.assert_gt(time_diff, 0, 'Total time increase has appropriate value')
        t.assert_le(time_diff, after_finish - before_start,
            'Total time increase has appropriate value')

        local unchanged_before = op_before[unchanged]
        local unchanged_after = stats_after[case.op][unchanged]

        t.assert_equals(unchanged_before, unchanged_after, 'Other stats remained the same')
    end
end

-- Call some non-select operations on non-existing
-- spaces and ensure statistics is updated.
local unknown_space_cases = {
    insert = {
        func = 'crud.insert',
        args = { unknown_space_name, {} },
        op = 'insert',
    },
    insert_object = {
        func = 'crud.insert_object',
        args = { unknown_space_name, {} },
        op = 'insert',
    },
    get = {
        func = 'crud.get',
        args = { unknown_space_name, {} },
        op = 'get',
    },
    select = {
        func = 'crud.select',
        args = { unknown_space_name, {} },
        op = 'select',
    },
    pairs = {
        eval = eval.pairs_pcall,
        args = { unknown_space_name, {} },
        op = 'select',
    },
    replace = {
        func = 'crud.replace',
        args = { unknown_space_name, {} },
        op = 'replace',
    },
    replace_object = {
        func = 'crud.replace_object',
        args = { unknown_space_name, {}, {} },
        op = 'replace',
    },
    update = {
        func = 'crud.update',
        args = { unknown_space_name, {}, {} },
        op = 'update',
    },
    upsert = {
        func = 'crud.upsert',
        args = { unknown_space_name, {}, {} },
        op = 'upsert',
    },
    upsert_object = {
        func = 'crud.upsert_object',
        args = { unknown_space_name, {}, {} },
        op = 'upsert',
    },
    delete = {
        func = 'crud.delete',
        args = { unknown_space_name, {} },
        op = 'delete',
    },
    truncate = {
        func = 'crud.truncate',
        args = { unknown_space_name },
        op = 'truncate',
    },
    len = {
        func = 'crud.len',
        args = { unknown_space_name },
        op = 'len',
    },
    min = {
        func = 'crud.min',
        args = { unknown_space_name },
        op = 'borders',
    },
    max = {
        func = 'crud.max',
        args = { unknown_space_name },
        op = 'borders',
    },
}

for name, case in pairs(unknown_space_cases) do
    local test_name = ('test_%s_on_unknown_space'):format(name)

    g[test_name] = function(g)
        -- Collect statss before call.
        local stats_before = g:get_stats()
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, case.args)
        else
            _, err = g.router:call(case.func, case.args)
        end

        t.assert_not_equals(err, nil)

        -- Collect stats after call.
        local stats_after = g:get_stats()
        t.assert_type(stats_after, 'table')

        t.assert_equals(stats_after.space_not_found - stats_before.space_not_found, 1,
            "space_not_found statistic incremented")
        t.assert_equals(stats_after.spaces, stats_before.spaces,
            "Existing spaces stats haven't changed")
    end
end
