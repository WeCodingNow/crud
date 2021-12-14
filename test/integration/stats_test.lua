local fio = require('fio')
local clock = require('clock')
local t = require('luatest')

local stats_registry_common = require('crud.stats.registry_common')

local pgroup = t.group('stats_integration', {
    { driver = 'local' },
    { driver = 'metrics' },
})
local group_metrics = t.group('stats_metrics_integration', {
    { driver = 'metrics' },
})
local helpers = require('test.helper')

local space_id = 542
local space_name = 'customers'
local unknown_space_name = 'non_existing_space'

local function before_all(g)
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

    t.assert_equals(helpers.is_space_exist(g.router, space_name), true)
    t.assert_equals(helpers.is_space_exist(g.router, unknown_space_name), false)

    if g.params.driver == 'metrics' then
        local is_metrics_supported = g.router:eval([[
            return require('crud.stats.metrics_registry').is_supported()
        ]])
        t.skip_if(is_metrics_supported == false, 'Metrics registry is unsupported')
    end
end

local function after_all(g)
    helpers.stop_cluster(g.cluster)
end


local function get_stats(g, space_name)
    return g.router:eval("return crud.stats(...)", { space_name })
end

local function enable_stats(g, params)
    params = params or g.params
    g.router:eval("crud.enable_stats(...)", { params })
end

local function disable_stats(g)
    g.router:eval("crud.disable_stats()")
end

local function before_each(g)
    enable_stats(g)
    helpers.truncate_space_on_cluster(g.cluster, space_name)
end

local function get_metrics(g)
    return g.router:eval("return require('metrics').collect()")
end

pgroup.before_all(before_all)

pgroup.after_all(after_all)

pgroup.before_each(before_each)


group_metrics.before_all(before_all)

group_metrics.after_all(after_all)

group_metrics.before_each(before_each)


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
        pgroup.before_test(test_name, case.prepare)
    end

    pgroup[test_name] = function(g)
        -- Collect stats before call.
        local stats_before = get_stats(g, space_name)
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
        local stats_after = get_stats(g, space_name)
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

    pgroup[test_name] = function(g)
        -- Collect stats before call.
        local stats_before = get_stats(g)
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
        local stats_after = get_stats(g)
        t.assert_type(stats_after, 'table')

        t.assert_equals(stats_after.space_not_found - stats_before.space_not_found, 1,
            "space_not_found statistic incremented")
        t.assert_equals(stats_after.spaces, stats_before.spaces,
            "Existing spaces stats haven't changed")
    end
end

local prepare_select_data = function(g)
    helpers.insert_objects(g, space_name, {
        -- Storage is s-2.
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        },
        -- Storage is s-2.
        {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        },
        -- Storage is s-1.
        {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        },
        -- Storage is s-2.
        {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }
    })
end

local select_cases = {
    select_by_primary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'id_index', 3 }},
        map_reduces = 0,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_by_secondary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'age_index', 46 }},
        map_reduces = 1,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_full_scan = {
        func = 'crud.select',
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
        map_reduces = 1,
        tuples_fetched = 0,
        tuples_lookup = 4,
    },
    pairs_by_primary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'id_index', 3 }},
        map_reduces = 0,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    pairs_by_secondary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'age_index', 46 }},
        map_reduces = 1,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    pairs_full_scan = {
        eval = eval.pairs,
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
        map_reduces = 1,
        tuples_fetched = 0,
        tuples_lookup = 4,
    },
}

for name, case in pairs(select_cases) do
    local test_name = ('test_%s_details'):format(name)

    pgroup.before_test(test_name, prepare_select_data)

    pgroup[test_name] = function(g)
        local op = 'select'
        local space_name = space_name

        -- Collect stats before call.
        local stats_before = get_stats(g, space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions })
        end

        t.assert_equals(err, nil)

        -- Collect stats after call.
        local stats_after = get_stats(g, space_name)
        t.assert_type(stats_after, 'table')

        local op_before = get_before_stats(stats_before, op)
        local details_before = op_before.details
        local details_after = stats_after[op].details

        local tuples_fetched_diff = details_after.tuples_fetched - details_before.tuples_fetched
        t.assert_equals(tuples_fetched_diff, case.tuples_fetched,
            'Expected count of tuples fetched')

        local tuples_lookup_diff = details_after.tuples_lookup - details_before.tuples_lookup
        t.assert_equals(tuples_lookup_diff, case.tuples_lookup,
            'Expected count of tuples looked up on storage')

        local map_reduces_diff = details_after.map_reduces - details_before.map_reduces
        t.assert_equals(map_reduces_diff, case.map_reduces,
            'Expected count of map reduces planned')
    end
end

pgroup.test_resolve_name_from_id = function(g)
    local op = 'len'
    g.router:call('crud.len', { space_id })

    local stats = get_stats(g, space_name)
    t.assert_not_equals(stats[op], nil, "Statistics is filled by name")
end


-- Generate non-null stats for all cases.
local function generate_stats(g)
    for _, case in pairs(simple_operation_cases) do
        if case.prepare ~= nil then
            case.prepare(g)
        end

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

        if case.expect_error ~= true then
            t.assert_equals(err, nil)
        else
            t.assert_not_equals(err, nil)
        end
    end

    -- Generate non-null select details.
    prepare_select_data(g)
    for _, case in pairs(select_cases) do
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions })
        end

        t.assert_equals(err, nil)
    end

    -- Generate non-null space_not_found stats.
    local case = unknown_space_cases.insert
    local _, err = g.router:call(case.func, case.args)
    t.assert_not_equals(err, nil)
end

-- https://github.com/tarantool/metrics/blob/fc5a67072340b12f983f09b7d383aca9e2f10cf1/test/utils.lua#L22-L31
local function find_obs(metric_name, label_pairs, observations)
    for _, obs in pairs(observations) do
        local same_label_pairs = pcall(t.assert_equals, obs.label_pairs, label_pairs)
        if obs.metric_name == metric_name and same_label_pairs then
            return obs
        end
    end
    t.assert_items_include(
        observations,
        { metric_name = metric_name, label_pairs = label_pairs },
        'Observation found')
end

-- https://github.com/tarantool/metrics/blob/fc5a67072340b12f983f09b7d383aca9e2f10cf1/test/utils.lua#L55-L63
local function find_metric(metric_name, metrics_data)
    local m = {}
    for _, v in ipairs(metrics_data) do
        if v.metric_name == metric_name then
            table.insert(m, v)
        end
    end
    return #m > 0 and m or nil
end

local function get_unique_label_values(metrics_data, label_key)
    local label_values_map = {}
    for _, v in ipairs(metrics_data) do
        local label_pairs = v.label_pairs or {}
        if label_pairs[label_key] ~= nil then
            label_values_map[label_pairs[label_key]] = true
        end
    end

    local label_values = {}
    for k, _ in pairs(label_values_map) do
        table.insert(label_values, k)
    end

    return label_values
end

local function validate_stats(metrics)
    local stats = find_metric('tnt_crud_stats', metrics)
    t.assert_type(stats, 'table', '`tnt_crud_stats` summary metrics found')

    local stats_count = find_metric('tnt_crud_stats_count', metrics)
    t.assert_type(stats_count, 'table', '`tnt_crud_stats` summary metrics found')

    local stats_sum = find_metric('tnt_crud_stats_sum', metrics)
    t.assert_type(stats_sum, 'table', '`tnt_crud_stats` summary metrics found')


    local expected_operations = { 'insert', 'get', 'replace', 'update',
        'upsert', 'delete', 'select', 'truncate', 'len', 'borders' }

    t.assert_items_equals(get_unique_label_values(stats, 'operation'), expected_operations,
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(stats_count, 'operation'), expected_operations,
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(stats_sum, 'operation'), expected_operations,
        'Metrics are labelled with operation')


    local expected_statuses = { 'ok', 'error' }

    t.assert_items_equals( get_unique_label_values(stats, 'status'), expected_statuses,
        'Metrics are labelled with status')

    t.assert_items_equals(get_unique_label_values(stats_count, 'status'), expected_statuses,
        'Metrics are labelled with status')

    t.assert_items_equals(get_unique_label_values(stats_sum, 'status'), expected_statuses,
        'Metrics are labelled with status')


    local expected_names = { space_name }

    t.assert_items_equals(get_unique_label_values(stats, 'name'), expected_names,
        'Metrics are labelled with space name (only existing spaces)')

    t.assert_items_equals(get_unique_label_values(stats_count, 'name'),
        expected_names,
        'Metrics are labelled with space name (only existing spaces)')

    t.assert_items_equals(
        get_unique_label_values(stats_sum, 'name'),
        expected_names,
        'Metrics are labelled with space name (only existing spaces)')


    local tuples_fetched = find_metric('tnt_crud_tuples_fetched', metrics)
    t.assert_type(tuples_fetched, 'table', '`tnt_crud_tuples_fetched` metrics found')

    t.assert_items_equals(get_unique_label_values(tuples_fetched, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(tuples_fetched, 'name'), expected_names,
        'Metrics are labelled with space name (only existing spaces)')


    local tuples_lookup = find_metric('tnt_crud_tuples_lookup', metrics)
    t.assert_type(tuples_lookup, 'table', '`tnt_crud_tuples_lookup` metrics found')

    t.assert_items_equals( get_unique_label_values(tuples_lookup, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(tuples_lookup, 'name'), expected_names,
        'Metrics are labelled with space name (only existing spaces)')


    local map_reduces = find_metric('tnt_crud_map_reduces', metrics)
    t.assert_type(map_reduces, 'table', '`tnt_crud_map_reduces` metrics found')

    t.assert_items_equals(get_unique_label_values(map_reduces, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(map_reduces, 'name'), expected_names,
        'Metrics are labelled with space name (only existing spaces)')


    local space_not_found = find_metric('tnt_crud_space_not_found', metrics)
    t.assert_type(space_not_found, 'table', '`tnt_crud_space_not_found` metrics found')
end


group_metrics.before_test(
    'test_stats_stored_in_global_metrics_registry',
    generate_stats)

group_metrics.test_stats_stored_in_global_metrics_registry = function(g)
    local metrics = get_metrics(g)
    validate_stats(metrics)
end


group_metrics.before_test('test_metrics_updated_per_call', generate_stats)

group_metrics.test_metrics_updated_per_call = function(g)
    local metrics_before = get_metrics(g)
    local stats_labels = { operation = 'select', status = 'ok', name = space_name }
    local details_labels = { operation = 'select', name = space_name }

    local count_before = find_obs('tnt_crud_stats_count', stats_labels, metrics_before)
    local time_before = find_obs('tnt_crud_stats_sum', stats_labels, metrics_before)
    local tuples_lookup_before = find_obs('tnt_crud_tuples_lookup', details_labels, metrics_before)
    local tuples_fetched_before = find_obs('tnt_crud_tuples_fetched', details_labels, metrics_before)
    local map_reduces_before = find_obs('tnt_crud_map_reduces', details_labels, metrics_before)

    local case = select_cases['select_by_secondary_index']
    local _, err = g.router:call(case.func, { space_name, case.conditions })
    t.assert_equals(err, nil)

    local metrics_after = get_metrics(g)
    local count_after = find_obs('tnt_crud_stats_count', stats_labels, metrics_after)
    local time_after = find_obs('tnt_crud_stats_sum', stats_labels, metrics_after)
    local tuples_lookup_after = find_obs('tnt_crud_tuples_lookup', details_labels, metrics_after)
    local tuples_fetched_after = find_obs('tnt_crud_tuples_fetched', details_labels, metrics_after)
    local map_reduces_after = find_obs('tnt_crud_map_reduces', details_labels, metrics_after)

    t.assert_equals(count_after.value - count_before.value, 1,
        '`select` metrics count increased')
    t.assert_ge(time_after.value - time_before.value, 0,
        '`select` total time increased')
    t.assert_ge(tuples_lookup_after.value - tuples_lookup_before.value, case.tuples_lookup,
        '`select` tuples lookup expected change')
    t.assert_ge(tuples_fetched_after.value - tuples_fetched_before.value, case.tuples_fetched,
        '`select` tuples feched expected change')
    t.assert_ge(map_reduces_after.value - map_reduces_before.value, case.tuples_lookup,
        '`select` map reduces expected change')
end


group_metrics.before_test(
    'test_space_not_found_metrics_updated_per_call',
    generate_stats)

group_metrics.test_space_not_found_metrics_updated_per_call = function(g)
    local metrics_before = get_metrics(g)

    local space_not_found_before = find_obs('tnt_crud_space_not_found', {}, metrics_before)

    local case = unknown_space_cases.insert
    local _, err = g.router:call(case.func, case.args)
    t.assert_not_equals(err, nil)

    local metrics_after = get_metrics(g)
    local space_not_found_after = find_obs('tnt_crud_space_not_found', {}, metrics_after)

    t.assert_equals(space_not_found_after.value - space_not_found_before.value, 1,
        '`tnt_crud_space_not_found` metrics count increased')
end


group_metrics.before_test(
    'test_metrics_collectors_destroyed_if_stats_disabled',
    generate_stats)

group_metrics.test_metrics_collectors_destroyed_if_stats_disabled = function(g)
    disable_stats(g)

    local metrics = get_metrics(g)

    local stats = find_metric('tnt_crud_stats', metrics)
    t.assert_equals(stats, nil, '`tnt_crud_stats` summary metrics not found')

    local stats_count = find_metric('tnt_crud_stats_count', metrics)
    t.assert_equals(stats_count, nil, '`tnt_crud_stats` summary metrics not found')

    local stats_sum = find_metric('tnt_crud_stats_sum', metrics)
    t.assert_equals(stats_sum, nil, '`tnt_crud_stats` summary metrics not found')

    local tuples_fetched = find_metric('tnt_crud_tuples_fetched', metrics)
    t.assert_equals(tuples_fetched, nil, '`tnt_crud_tuples_fetched` metrics not found')

    local tuples_lookup = find_metric('tnt_crud_tuples_lookup', metrics)
    t.assert_equals(tuples_lookup, nil, '`tnt_crud_tuples_lookup` metrics not found')

    local map_reduces = find_metric('tnt_crud_map_reduces', metrics)
    t.assert_equals(map_reduces, nil, '`tnt_crud_map_reduces` metrics not found')

    local space_not_found = find_metric('tnt_crud_space_not_found', metrics)
    t.assert_equals(space_not_found, nil, '`tnt_crud_space_not_found` metrics not found')
end


group_metrics.before_test(
    'test_stats_stored_in_metrics_registry_after_switch_to_metrics_driver',
    disable_stats)

group_metrics.test_stats_stored_in_metrics_registry_after_switch_to_metrics_driver = function(g)
    enable_stats(g, { driver = 'local' })
    -- Switch to metrics driver.
    enable_stats(g, { driver = 'metrics' })

    generate_stats(g)
    local metrics = get_metrics(g)
    validate_stats(metrics)
end
