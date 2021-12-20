local fio = require('fio')
local clock = require('clock')
local fiber = require('fiber')
local errors = require('errors')
local net_box = require('net.box')
local log = require('log')

local t = require('luatest')
local group = t.group('perf')

local helpers = require('test.helper')


local id = 0
local function gen()
    id = id + 1
    return id
end

local function reset_gen()
    id = 0
end

group.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_ddl'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'crud-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            },
            {
                uuid = helpers.uuid('d'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('d', 1), alias = 's3-master' },
                    { instance_uuid = helpers.uuid('d', 2), alias = 's3-replica' },
                },
            }
        },
    })
    g.cluster:start()

    g.router = g.cluster:server('router').net_box

    g.router:eval([[
        rawset(_G, 'crud', require('crud'))
    ]])

    -- Run real perf tests only with flag, otherwise run short version
    -- to test compatibility as part of unit/integration test run.
    g.perf_mode_on = os.getenv('PERF_MODE_ON')

    g.total_report = {}
end)

group.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    reset_gen()
end)

local function normalize(s, n)
    if type(s) == 'number' then
        s = ('%.2f'):format(s)
    end

    local len = s:len()
    if len > n then
        return s:sub(1, n)
    end

    return (' '):rep(n - len) .. s
end

local row_name = {
    insert = 'insert',
    select_pk = 'select by pk',
    select_gt_pk = 'select gt by pk (limit 10)',
    select_secondary_eq = 'select eq by secondary (limit 10)',
    select_secondary_sharded = 'select eq by sharding secondary',
}

local column_name = {
    vshard = 'vshard',
    without_stats_wrapper = 'crud (raw [1])',
    stats_disabled = 'crud (stats disabled)',
    bucket_id = 'crud (bucket_id [2])',
    local_stats = 'crud (local stats)',
    metrics_stats = 'crud (metrics stats)',
}

local column_comment = {
    '1. Without stats wrapper',
    '2. Known bucket_id, stats disabled',
}

local function visualize_section(total_report, name, section, params)
    local report_str = ('== %s ==\n\n'):format(name)

    local headers = normalize('', params.row_header_width) .. ' ||'

    for _, column in ipairs(params.columns) do
        headers = headers .. ' ' .. normalize(column, params.col_width[column]) .. ' |'
    end

    report_str = report_str .. headers .. '\n'
    report_str = report_str .. ('='):rep(headers:len()) .. '\n'

    for _, row in ipairs(params.rows) do
        local row_str = normalize(row, params.row_header_width) .. ' ||'

        for _, column in ipairs(params.columns) do
            if total_report[row] ~= nil and total_report[row][column] ~= nil then
                local report = total_report[row][column]
                row_str = row_str .. ' ' .. normalize(report.str[section], params.col_width[column]) .. ' |'
            else
                row_str = row_str .. ' ' .. normalize('unknown', params.col_width[column]) .. ' |'
            end
        end

        report_str = report_str .. row_str .. '\n'
        report_str = report_str .. ('-'):rep(row_str:len()) .. '\n'
    end

    report_str = report_str .. '\n\n\n'

    return report_str
end

local function visualize_report(report)
    local params = {}

    -- Set columns and rows explicitly to preserve custom order.
    params.columns = {
        column_name.vshard,
        column_name.without_stats_wrapper,
        column_name.stats_disabled,
        column_name.bucket_id,
        column_name.local_stats,
        column_name.metrics_stats,
    }

    params.rows = {
        row_name.select_pk,
        row_name.select_gt_pk,
        row_name.select_secondary_eq,
        row_name.select_secondary_sharded,
        row_name.insert,
    }

    params.row_header_width = 1
    for _, name in pairs(row_name) do
        params.row_header_width = math.max(name:len(), params.row_header_width)
    end

    local min_col_width = 12
    params.col_width = {}
    for _, name in ipairs(params.columns) do
        params.col_width[name] = math.max(name:len(), min_col_width)
    end

    local report_str = '\n==== PERFORMANCE REPORT ====\n\n\n'

    report_str = report_str .. visualize_section(report, 'SUCCESS REQUESTS', 'success_count', params)
    report_str = report_str .. visualize_section(report, 'SUCCESS REQUESTS PER SECOND', 'success_rps', params)
    report_str = report_str .. visualize_section(report, 'ERRORS', 'error_count', params)
    report_str = report_str .. visualize_section(report, 'AVERAGE CALL TIME', 'average_time', params)
    report_str = report_str .. visualize_section(report, 'MAX CALL TIME', 'max_time', params)

    for _, comment in ipairs(column_comment) do
        report_str = report_str .. comment .. '\n'
    end

    report_str = report_str .. '\n\n'

    log.info(report_str)
end

group.after_each(function(g)
    g.router:call("crud.disable_stats")
end)

group.after_all(function(g)
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)

    pcall(visualize_report, g.total_report)
end)

local function generate_customer()
    return { gen(), box.NULL, 'David Smith', 33 }
end

local select_prepare = function(g)
    local count
    if g.perf_mode_on then
        count = 10100
    else
        count = 100
    end

    for _ = 1, count do
        g.router:call('crud.insert', { 'customers', generate_customer() })
    end
    reset_gen()
end

local select_sharded_by_secondary_prepare = function(g)
    local count
    if g.perf_mode_on then
        count = 10100
    else
        count = 100
    end

    for _ = 1, count do
        g.router:call(
            'crud.insert',
            {
                'customers_name_age_key_different_indexes',
                { gen(), box.NULL, 'David Smith', gen() % 50 + 18 }
            }
        )
    end
    reset_gen()
end

local vshard_prepare = function(g)
    g.router:eval([[
        local vshard = require('vshard')

        local function _vshard_insert(space_name, tuple)
            local bucket_id = vshard.router.bucket_id_strcrc32(tuple[1])
            return vshard.router.callrw(
                bucket_id,
                '_vshard_insert_storage',
                { space_name, tuple, bucket_id }
            )
        end

        rawset(_G, '_vshard_insert', _vshard_insert)


        local function _vshard_select(space_name, key)
            local bucket_id = vshard.router.bucket_id_strcrc32(key)
            return vshard.router.callrw(
                bucket_id,
                '_vshard_select_storage',
                { space_name, key }
            )
        end

        rawset(_G, '_vshard_select', _vshard_select)


        local function sort(a, b)
            return a[1] < b[1]
        end

        local function _vshard_select_gt(space, key, opts)
            assert(type(opts.limit) == 'number')
            assert(opts.limit > 0)

            local tuples = {}

            for id, replicaset in pairs(vshard.router.routeall()) do
                local resp, err = replicaset:call(
                    '_vshard_select_storage',
                    { space, key, nil, box.index.GT, opts.limit }
                )
                if err ~= nil then
                    error(err)
                end

                for _, v in ipairs(resp) do
                    table.insert(tuples, v)
                end

            end

            -- Naive merger.
            local response = { }

            table.sort(tuples, sort)

            for i = 1, opts.limit do
                response[i] = tuples[i]
            end

            return response
        end

        rawset(_G, '_vshard_select_gt', _vshard_select_gt)


        local function _vshard_select_secondary(space_name, index_name, key, opts)
            assert(type(opts.limit) == 'number')
            assert(opts.limit > 0)

            local storage_response = {}

            for id, replicaset in pairs(vshard.router.routeall()) do
                local resp, err = replicaset:call(
                    '_vshard_select_storage',
                    { space_name, key, index_name, box.index.EQ, opts.limit }
                )
                if err ~= nil then
                    error(err)
                end

                storage_response[id] = resp
            end

            -- Naive merger.
            local response = { }

            local ind = 0
            for i = 1, opts.limit do
                for _, tuples in pairs(storage_response) do
                    if tuples[i] ~= nil then
                        response[ind] = tuples[i]
                    end
                end

                if ind == opts.limit then
                    break
                end
            end

            return response
        end

        rawset(_G, '_vshard_select_secondary', _vshard_select_secondary)


        local function _vshard_select_customer_by_name_and_age(key)
            local bucket_id = vshard.router.bucket_id_strcrc32(key)

            return vshard.router.callrw(
                bucket_id,
                '_vshard_select_customer_by_name_and_age_storage',
                { key }
            )
        end

        rawset(_G, '_vshard_select_customer_by_name_and_age', _vshard_select_customer_by_name_and_age)
    ]])

    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local function _vshard_insert_storage(space_name, tuple, bucket_id)
                local space = box.space[space_name]
                assert(space ~= nil)

                assert(space.index.bucket_id ~= nil)
                tuple[space.index.bucket_id.parts[1].fieldno] = bucket_id

                local ok = space:insert(tuple)
                assert(ok ~= nil)
            end

            rawset(_G, '_vshard_insert_storage', _vshard_insert_storage)


            local function _vshard_select_storage(space_name, key, index_name, iterator, limit)
                local space = box.space[space_name]
                assert(space ~= nil)

                local index = nil
                if index_name == nil then
                    index = box.space[space_name].index[0]
                else
                    index = box.space[space_name].index[index_name]
                end
                assert(index ~= nil)

                iterator = iterator or box.index.EQ
                return index:select(key, { limit = limit, iterator = iterator })
            end

            rawset(_G, '_vshard_select_storage', _vshard_select_storage)


            local function _vshard_select_customer_by_name_and_age_storage(key)
                local space = box.space.customers_name_age_key_different_indexes
                local index = space.index.age

                for _, tuple in index:pairs(key[2]) do
                    if tuple.name == key[1] then
                        return tuple
                    end
                end
                return {}
            end

            rawset(_G, '_vshard_select_customer_by_name_and_age_storage',
                _vshard_select_customer_by_name_and_age_storage)
        ]])
    end
end

local insert_params = function()
    return { 'customers', generate_customer() }
end

local select_params_pk_eq = function()
    return { 'customers', {{'==', 'id', gen() % 10000}} }
end

local select_params_pk_eq_bucket_id = function()
    local id = gen() % 10000
    return { 'customers', {{'==', 'id', id}}, id }
end

local vshard_select_params_pk_eq = function()
    return { 'customers', gen() % 10000 }
end

local select_params_pk_gt = function()
    return { 'customers', {{'>', 'id', gen() % 10000}}, { first = 10 } }
end

local vshard_select_params_pk_gt = function()
    return { 'customers', gen() % 10000, { limit = 10 } }
end

local select_params_secondary_eq = function()
    return { 'customers', {{'==', 'age', 33}}, { first = 10 } }
end

local vshard_select_params_secondary_eq = function()
    return { 'customers', 'age', 33, { limit = 10 } }
end

local select_params_sharded_by_secondary = function()
    return {
        'customers_name_age_key_different_indexes',
        { { '==', 'name', 'David Smith' }, { '==', 'age', gen() % 50 + 18 } },
        { first = 1 }
    }
end

local select_params_sharded_by_secondary_bucket_id = function()
    local age = gen() % 50 + 18
    return {
        'customers_name_age_key_different_indexes',
        { { '==', 'name', 'David Smith' }, { '==', 'age', age } },
        { first = 1 },
        age
    }
end

local vshard_select_params_sharded_by_secondary = function()
    return {{ 'David Smith', gen() % 50 + 18 }}
end

local stats_cases = {
    stats_disabled = {
        column_name = column_name.stats_disabled,
    },
    local_stats = {
        prepare = function(g)
            g.router:call("crud.enable_stats", {{ driver = 'local' }})
        end,
        column_name = column_name.local_stats,
    },
    metrics_stats = {
        prepare = function(g)
            local is_metrics_supported = g.router:eval([[
                return require('crud.stats.metrics_registry').is_supported()
            ]])
            t.skip_if(is_metrics_supported == false, 'Metrics registry is unsupported')
            g.router:call("crud.enable_stats", {{ driver = 'metrics' }})
        end,
        column_name = column_name.metrics_stats,
    },
}

local integration_params = {
    timeout = 2,
    fiber_count = 5,
    connection_count = 2,
}

local insert_perf = {
    timeout = 30,
    fiber_count = 600,
    connection_count = 10,
}

-- Higher load may lead to net_msg_max limit break.
local select_perf = {
    timeout = 30,
    fiber_count = 200,
    connection_count = 10,
}

local cases = {
    crud_insert = {
        call = 'crud.insert',
        params = insert_params,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

    crud_insert_without_stats_wrapper = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_insert', require('crud.insert').tuple)
            ]])
        end,
        call = '_plain_insert',
        params = insert_params,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

    vshard_insert = {
        prepare = vshard_prepare,
        call = '_vshard_insert',
        params = insert_params,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

    crud_select_pk_eq = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_pk_eq,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    crud_select_known_bucket_id_pk_eq = {
        prepare = function(g)
            select_prepare(g)

            g.router:eval([[
                local vshard = require('vshard')

                local function _crud_select_bucket(space_name, conditions, sharding_key)
                    local bucket_id = vshard.router.bucket_id_strcrc32(sharding_key)
                    return crud.select(space_name, conditions, { bucket_id = bucket_id })
                end

                rawset(_G, '_crud_select_bucket', _crud_select_bucket)
            ]])
        end,
        call = '_crud_select_bucket',
        params = select_params_pk_eq_bucket_id,
        matrix = { [''] = { column_name = column_name.bucket_id } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    crud_select_without_stats_wrapper_pk_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_pk_eq,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    vshard_select_pk_eq = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select',
        params = vshard_select_params_pk_eq,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    crud_select_pk_gt = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_pk_gt,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    crud_select_without_stats_wrapper_pk_gt = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_select', require('crud.select').call)
            ]])
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_pk_gt,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    vshard_select_pk_gt = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_gt',
        params = vshard_select_params_pk_gt,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    crud_select_secondary_eq = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_secondary_eq,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    crud_select_without_stats_wrapper_secondary_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_secondary_eq,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    vshard_select_secondary_eq = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_secondary',
        params = vshard_select_params_secondary_eq,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    crud_select_sharding_secondary_eq = {
        prepare = select_sharded_by_secondary_prepare,
        call = 'crud.select',
        params = select_params_sharded_by_secondary,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    crud_select_sharding_secondary_eq_bucket_id = {
        prepare = function(g)
            select_sharded_by_secondary_prepare(g)

            g.router:eval([[
                local vshard = require('vshard')

                local function _crud_select_bucket_secondary(space_name, conditions, opts, sharding_key)
                    local bucket_id = vshard.router.bucket_id_strcrc32(sharding_key)
                    opts.bucket_id = bucket_id
                    return crud.select(space_name, conditions, opts)
                end

                rawset(_G, '_crud_select_bucket_secondary', _crud_select_bucket_secondary)
            ]])
        end,
        call = '_crud_select_bucket_secondary',
        params = select_params_sharded_by_secondary_bucket_id,
        matrix = { [''] = { column_name = column_name.bucket_id } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    crud_select_without_stats_wrapper_sharding_secondary_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_sharded_by_secondary_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_sharded_by_secondary,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    vshard_select_sharding_secondary_eq = {
        prepare = function(g)
            select_sharded_by_secondary_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_customer_by_name_and_age',
        params = vshard_select_params_sharded_by_secondary,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },
}

local function fiber_generator(conn, call, params, report, timeout)
    local start = clock.monotonic()

    while (clock.monotonic() - start) < timeout do
        local call_start = clock.monotonic()
        local ok, res, err = pcall(conn.call, conn, call, params())
        local call_time = clock.monotonic() - call_start

        if not ok then
            log.error(res)
            table.insert(report.errors, res)
        elseif err ~= nil then
            errors.wrap(err)
            log.error(err)
            table.insert(report.errors, err)
        else
            report.count = report.count + 1
        end

        report.total_time = report.total_time + call_time
        report.max_time = math.max(report.max_time, call_time)
    end
end

for name, case in pairs(cases) do
    local matrix = case.matrix or { [''] = { { column_name = '' } } }

    for subname, subcase in pairs(matrix) do
        local name_tail = ''
        if subname ~= '' then
            name_tail = ('_with_%s'):format(subname)
        end

        local test_name = ('test_%s%s'):format(name, name_tail)

        group.before_test(test_name, function(g)
            if case.prepare ~= nil then
                case.prepare(g)
            end

            if subcase.prepare ~= nil then
                subcase.prepare(g)
            end
        end)

        group[test_name] = function(g)
            local params
            if g.perf_mode_on then
                params = case.perf_params
            else
                params = case.integration_params
            end

            local connections = {}

            local router = g.cluster:server('router')
            for _ = 1, params.connection_count do
                local c = net_box:connect(router.net_box_uri, router.net_box_credentials)
                if c == nil then
                    t.fail('Failed to prepare connections')
                end
                table.insert(connections, c)
            end

            local fibers = {}
            local report = { errors = {}, count = 0, total_time = 0, max_time = 0 }
            for id = 1, params.fiber_count do
                local conn_id = id % params.connection_count + 1
                local conn = connections[conn_id]
                local f = fiber.new(fiber_generator, conn, case.call, case.params, report, params.timeout)
                f:set_joinable(true)
                table.insert(fibers, f)
            end

            local start_time = clock.monotonic()
            for i = 1, params.fiber_count do
                fibers[i]:join()
            end
            local run_time = clock.monotonic() - start_time

            report.str = {
                success_count = ('%d'):format(report.count),
                error_count = ('%d'):format(#report.errors),
                success_rps = ('%.2f'):format(report.count / run_time),
                max_time = ('%.3f ms'):format(report.max_time * 1e3),
            }

            local total_count = report.count + #report.errors
            if total_count > 0 then
                report.str.average_time = ('%.3f ms'):format(report.total_time / total_count * 1e3)
            else
                report.str.average_time = 'unknown'
            end

            g.total_report[case.row_name] = g.total_report[case.row_name] or {}
            g.total_report[case.row_name][subcase.column_name] = report

            log.info('\n%s: %s success requests (rps %s), %s errors, call average time %s, call max time %s \n',
                test_name, report.str.success_count, report.str.success_rps, report.str.error_count,
                report.str.average_time, report.str.max_time)
        end
    end
end
