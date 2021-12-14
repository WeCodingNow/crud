local clock = require('clock')
local fio = require('fio')
local fun = require('fun')
local t = require('luatest')

local stats_module = require('crud.stats.module')
local utils = require('crud.common.utils')

local pgroup = t.group('stats_unit', {
    { driver = 'local' },
    { driver = 'metrics' },
})
local group_driver = t.group('stats_driver_unit')
local helpers = require('test.helper')

local space_id = 542
local space_name = 'customers'
local unknown_space_name = 'non_existing_space'

local function before_all(g)
    -- Enable test cluster for "is space exist?" checks.
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })
    g.cluster:start()
    g.router = g.cluster:server('router').net_box

    helpers.prepare_simple_functions(g.router)
    g.router:eval("stats_module = require('crud.stats.module')")

    t.assert_equals(helpers.is_space_exist(g.router, space_name), true)
    t.assert_equals(helpers.is_space_exist(g.router, unknown_space_name), false)

    g.is_metrics_supported = g.router:eval([[
        return require('crud.stats.metrics_registry').is_supported()
    ]])

    if g.params ~= nil and g.params.driver == 'metrics' then
        t.skip_if(g.is_metrics_supported == false, 'Metrics registry is unsupported')
    end
end

local function after_all(g)
    helpers.stop_cluster(g.cluster)
end

local function get_stats(g, space_name)
    return g.router:eval("return stats_module.get(...)", { space_name })
end

local function enable_stats(g, params)
    params = params or g.params
    g.router:eval("stats_module.enable(...)", { params })
end

local function disable_stats(g)
    g.router:eval("stats_module.disable()")
end

local function reset_stats(g)
    g.router:eval("return stats_module.reset()")
end

pgroup.before_all(before_all)

pgroup.after_all(after_all)

-- Reset statistics between tests, reenable if needed.
pgroup.before_each(enable_stats)

pgroup.after_each(disable_stats)


group_driver.before_all(before_all)

group_driver.after_all(after_all)

group_driver.after_each(disable_stats)


pgroup.test_get_format_after_enable = function(g)
    local stats = get_stats(g)

    t.assert_type(stats, 'table')
    t.assert_equals(stats.spaces, {})
    t.assert_equals(stats.space_not_found, 0)
end

pgroup.test_get_by_space_name_format_after_enable = function(g)
    local stats = get_stats(g, space_name)

    t.assert_type(stats, 'table')
    t.assert_equals(stats, {})
end

-- Test statistics values after wrapped functions call
-- for existing space.
local observe_cases = {
    wrapper_observes_expected_values_on_ok = {
        operations = stats_module.op,
        func = 'return_true',
        changed_coll = 'ok',
        unchanged_coll = 'error',
    },
    wrapper_observes_expected_values_on_error_return = {
        operations = stats_module.op,
        func = 'return_err',
        changed_coll = 'error',
        unchanged_coll = 'ok',
    },
    wrapper_observes_expected_values_on_error_throw = {
        operations = stats_module.op,
        func = 'throws_error',
        changed_coll = 'error',
        unchanged_coll = 'ok',
        pcall = true,
    },
    pairs_wrapper_observes_expected_values_on_ok = {
        operations = { stats_module.op.SELECT },
        func = 'pairs_ok',
        changed_coll = 'ok',
        unchanged_coll = 'error',
        opts = { pairs = true },
    },
    pairs_wrapper_observes_expected_values_on_error = {
        operations = { stats_module.op.SELECT },
        func = 'throws_error',
        changed_coll = 'error',
        unchanged_coll = 'ok',
        pcall = true,
        opts = { pairs = true },
    },
}

local call_wrapped = [[
    local func = rawget(_G, select(1, ...))
    local op = select(2, ...)
    local opts = select(3, ...)
    local space_name = select(4, ...)

    stats_module.wrap(func, op, opts)(space_name)
]]

for name, case in pairs(observe_cases) do
    for _, op in pairs(case.operations) do
        local test_name = ('test_%s_%s'):format(op, name)

        pgroup[test_name] = function(g)
            -- Call wrapped functions on server side.
            -- Collect execution times from outside.
            local run_count = 10
            local time_diffs = {}

            local args = { case.func, op, case.opts, space_name }

            for _ = 1, run_count do
                local before_start = clock.monotonic()

                if case.pcall then
                    pcall(g.router.eval, g.router, call_wrapped, args)
                else
                    g.router:eval(call_wrapped, args)
                end

                local after_finish = clock.monotonic()

                table.insert(time_diffs, after_finish - before_start)
            end

            table.sort(time_diffs)
            local total_time = fun.foldl(function(acc, x) return acc + x end, 0, time_diffs)

            -- Validate stats format after execution.
            local total_stats = get_stats(g)
            t.assert_type(total_stats, 'table', 'Total stats present after observations')

            local space_stats = get_stats(g, space_name)
            t.assert_type(space_stats, 'table', 'Space stats present after observations')

            t.assert_equals(total_stats.spaces[space_name], space_stats,
                'Space stats is a section of total stats')

            local op_stats = space_stats[op]
            t.assert_type(op_stats, 'table', 'Op stats present after observations for the space')

            -- Expected collectors (changed_coll: 'ok' or 'error') have changed.
            local changed = op_stats[case.changed_coll]
            t.assert_type(changed, 'table', 'Status stats present after observations')

            t.assert_equals(changed.count, run_count, 'Count incremented by count of runs')

            local sleep_time = helpers.simple_functions_params().sleep_time
            t.assert_ge(changed.latency, sleep_time, 'Latency has appropriate value')
            t.assert_le(changed.latency, time_diffs[#time_diffs], 'Latency has appropriate value')

            t.assert_ge(changed.time, sleep_time * run_count,
                'Total time increase has appropriate value')
            t.assert_le(changed.time, total_time, 'Total time increase has appropriate value')

            -- Other collectors (unchanged_coll: 'error' or 'ok')
            -- have been initialized and have default values.
            local unchanged = op_stats[case.unchanged_coll]
            t.assert_type(unchanged, 'table', 'Other status stats present after observations')

            t.assert_equals(
                unchanged,
                {
                    count = 0,
                    latency = 0,
                    time = 0
                },
                'Other status collectors initialized after observations'
            )

            -- SELECT collectors have additional details section.
            if op == stats_module.op.SELECT then
                t.assert_equals(
                    op_stats.details,
                    {
                        tuples_fetched = 0,
                        tuples_lookup = 0,
                        map_reduces = 0,
                    },
                    'Detail collectors initialized after select observations'
                )
            end
        end
    end
end

-- Test wrapper preserves return values.
local disable_stats_cases = {
    stats_disable_before_wrap_ = {
        before_wrap = 'stats_module.disable()',
        after_wrap = '',
    },
    stats_disable_after_wrap_ = {
        before_wrap = '',
        after_wrap = 'stats_module.disable()',
    },
    [''] = {
        before_wrap = '',
        after_wrap = '',
    },
}

local preserve_return_cases = {
    wrapper_preserves_return_values_on_ok = {
        func = 'return_true',
        res = true,
        err = nil,
    },
    wrapper_preserves_return_values_on_error = {
        func = 'return_err',
        res = nil,
        err = helpers.simple_functions_params().error,
    },
}

local preserve_throw_cases = {
    wrapper_preserves_error_throw = {
        opts = { pairs = false },
    },
    pairs_wrapper_preserves_error_throw = {
        opts = { pairs = true },
    },
}

for name_head, disable_case in pairs(disable_stats_cases) do
    for name_tail, return_case in pairs(preserve_return_cases) do
        local test_name = ('test_%s%s'):format(name_head, name_tail)

        pgroup[test_name] = function(g)
            local op = stats_module.op.INSERT

            local eval = ([[
                local func = rawget(_G, select(1, ...))
                local op = select(2, ...)
                local space_name = select(3, ...)

                %s -- before_wrap
                local w_func = stats_module.wrap(func, op)
                %s -- after_wrap

                return w_func(space_name)
            ]]):format(disable_case.before_wrap, disable_case.after_wrap)

            local res, err = g.router:eval(eval, { return_case.func, op, space_name })

            t.assert_equals(res, return_case.res, 'Wrapper preserves first return value')
            t.assert_equals(err, return_case.err, 'Wrapper preserves second return value')
        end
    end

    local test_name = ('test_%spairs_wrapper_preserves_return_values'):format(name_head)

    pgroup[test_name] = function(g)
        local op = stats_module.op.INSERT

        local input = { a = 'a', b = 'b' }
        local eval = ([[
            local input = select(1, ...)
            local func = function() return pairs(input) end
            local op = select(2, ...)
            local space_name = select(3, ...)

            %s -- before_wrap
            local w_func = stats_module.wrap(func, op, { pairs = true })
            %s -- after_wrap

            local res = {}
            for k, v in w_func(space_name) do
                res[k] = v
            end

            return res
        ]]):format(disable_case.before_wrap, disable_case.after_wrap)

        local res = g.router:eval(eval, { input, op, space_name })

        t.assert_equals(input, res, 'Wrapper preserves pairs return values')
    end

    for name_tail, throw_case in pairs(preserve_throw_cases) do
        local test_name = ('test_%s%s'):format(name_head, name_tail)

        pgroup[test_name] = function(g)
            local op = stats_module.op.INSERT

            local eval = ([[
                local func = rawget(_G, 'throws_error')
                local opts = select(1, ...)
                local op = select(2, ...)
                local space_name = select(3, ...)

                %s -- before_wrap
                local w_func = stats_module.wrap(func, op, opts)
                %s -- after_wrap

                w_func(space_name)
            ]]):format(disable_case.before_wrap, disable_case.after_wrap)

            t.assert_error_msg_contains(
                helpers.simple_functions_params().error_msg,
                g.router.eval, g.router, eval, { throw_case.opts, op, space_name }
            )
        end
    end
end

-- Test statistics values after wrapped functions call
-- for non-existing space.
local err_not_exist_msg = utils.space_doesnt_exist_msg(unknown_space_name)
local err_validation_msg = "Params validation failed"
local error_cases = {
    -- If standartized utils.space_doesnt_exist_msg error
    -- returned, space not found.
    unknown_space_error_return = {
        func = (" function(space_name) return nil, OpError:new(%q); end "):format(err_not_exist_msg),
        msg = err_not_exist_msg,
    },
    unknown_space_error_throw = {
        func = (" function(space_name) OpError:assert(false, %q); end "):format(err_not_exist_msg),
        msg = err_not_exist_msg,
        throw = true,
    },
    -- If error returned, space is not in stats registry and
    -- is unknown to vshard, space not found.
    arbitrary_error_return_for_unknown_space = {
        func = (" function(space_name) return nil, OpError:new(%q); end "):format(err_validation_msg),
        msg = err_validation_msg,
    },
    arbitrary_error_throw_for_unknown_space = {
        func = (" function(space_name) OpError:assert(false, %q); end "):format(err_validation_msg),
        msg = err_validation_msg,
        throw = true,
    },
}

for name, case in pairs(error_cases) do
    local test_name = ('test_%s_increases_space_not_found_count'):format(name)

    pgroup[test_name] = function(g)
        local op = stats_module.op.INSERT

        local eval = ([[
            local errors = require('errors')
            local utils = require('crud.common.utils')

            local OpError = errors.new_class('OpError')

            local func = %s
            local op = select(1, ...)
            local space_name = select(2, ...)

            return stats_module.wrap(func, op)(space_name)
        ]]):format(case.func)

        local err_msg
        if case.throw then
            local status, err = pcall(g.router.eval, g.router, eval,
                { op, unknown_space_name })
            t.assert_equals(status, false)
            err_msg = tostring(err)
        else
            local _, err = g.router:eval(eval, { op, unknown_space_name })
            err_msg = err.str
        end

        t.assert_str_contains(err_msg, case.msg, "Error preserved")

        local stats = get_stats(g)

        t.assert_equals(stats.space_not_found, 1)
        t.assert_equals(stats.spaces[unknown_space_name], nil,
            "Non-existing space haven't generated stats section")
    end
end

pgroup.test_stats_is_empty_after_disable = function(g)
    disable_stats(g)

    local op = stats_module.op.INSERT
    g.router:eval(call_wrapped, { 'return_true', op, {}, space_name })

    local stats = get_stats(g)
    t.assert_equals(stats, {})
end

local function prepare_non_default_stats(g)
    local op = stats_module.op.INSERT
    g.router:eval(call_wrapped, { 'return_true', op, {}, space_name })

    local stats = get_stats(g, space_name)
    t.assert_equals(stats[op].ok.count, 1, 'Non-zero stats prepared')

    return stats
end

pgroup.test_enable_with_same_driver_is_idempotent = function(g)
    local stats_before = prepare_non_default_stats(g)

    enable_stats(g)

    local stats_after = get_stats(g, space_name)

    t.assert_equals(stats_after, stats_before, 'Stats have not been reset')
end

pgroup.test_reset = function(g)
    prepare_non_default_stats(g)

    reset_stats(g)

    local stats = get_stats(g, space_name)

    t.assert_equals(stats, {}, 'Stats have been reset')
end

pgroup.test_reset_for_disabled_stats_does_not_init_module = function(g)
    disable_stats(g)

    local stats_before = get_stats(g)
    t.assert_equals(stats_before, {}, "Stats is empty")

    reset_stats(g)

    local stats_after = get_stats(g)
    t.assert_equals(stats_after, {}, "Stats is still empty")
end

pgroup.test_enabling_stats_on_non_router_throws_error = function(g)
    local storage = g.cluster:server('s1-master').net_box
    t.assert_error(storage.eval, storage, " require('crud.stats.module').enable() ")
end

pgroup.test_stats_fetch_callback = function(g)
    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }

    g.router:eval([[ stats_module.get_fetch_callback()(...) ]],
        { storage_cursor_stats, space_name })

    local op = stats_module.op.SELECT
    local stats = get_stats(g, space_name)

    t.assert_not_equals(stats[op], nil,
        'Fetch stats update inits SELECT collectors')

    local details = stats[op].details

    t.assert_equals(details.tuples_fetched, 5,
        'tuples_fetched is inremented by expected value')
    t.assert_equals(details.tuples_lookup, 25,
        'tuples_lookup is inremented by expected value')
end

pgroup.test_disable_stats_before_fetch_callback_get_do_not_break_call = function(g)
    disable_stats(g)

    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }
    g.router:eval([[ stats_module.get_fetch_callback()(...) ]],
        { storage_cursor_stats, space_name })

    t.success('No unexpected errors')
end

pgroup.test_disable_stats_after_fetch_callback_get_do_not_break_call = function(g)
    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }

    g.router:eval([[
        local callback = stats_module.get_fetch_callback()
        stats_module.disable()
        callback(...)
    ]], { storage_cursor_stats, space_name })

    t.success('No unexpected errors')
end

pgroup.test_space_is_known_to_registry_after_details_observe = function(g)
    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }

    g.router:eval([[ stats_module.get_fetch_callback()(...) ]],
        { storage_cursor_stats, space_name })

    local is_unknown_space = g.router:eval([[
        return stats_module.internal.registry.is_unknown_space(...)
    ]], { space_name })

    t.assert_equals(is_unknown_space, false)
end

pgroup.test_resolve_name_from_id = function(g)
    local op = stats_module.op.LEN
    g.router:eval(call_wrapped, { 'return_true', stats_module.op.LEN, {}, space_id })

    local stats = get_stats(g, space_name)
    t.assert_not_equals(stats[op], nil, "Statistics is filled by name")
end

group_driver.test_default_driver = function(g)
    enable_stats(g)

    local driver = g.router:eval(" return stats_module.internal.driver ")

    if g.is_metrics_supported then
        t.assert_equals(driver, 'metrics')
    else
        t.assert_equals(driver, 'local')
    end
end

group_driver.before_test(
    'test_stats_reenable_with_different_driver_reset_stats',
    function(g)
        t.skip_if(g.is_metrics_supported == false, 'Metrics registry is unsupported')
    end
)

group_driver.test_stats_reenable_with_different_driver_reset_stats = function(g)
    enable_stats(g, { driver = 'metrics' })

    prepare_non_default_stats(g)

    enable_stats(g, { driver = 'local' })
    local stats = get_stats(g)
    t.assert_equals(stats.spaces, {}, 'Stats have been reset')
end

group_driver.test_unknown_driver_throws_error = function(g)
    t.assert_error_msg_contains(
        'Unsupported driver: unknown',
        enable_stats, g, { driver = 'unknown' })
end

group_driver.before_test(
    'test_stats_enable_with_metrics_throws_error_if_unsupported',
    function(g)
        t.skip_if(g.is_metrics_supported == true, 'Metrics registry is supported')
    end
)

group_driver.test_stats_enable_with_metrics_throws_error_if_unsupported = function(g)
    t.assert_error_msg_contains(
        'Unsupported driver: metrics',
        enable_stats, g, { driver = 'metrics' })
end

