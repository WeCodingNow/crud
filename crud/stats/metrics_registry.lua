local is_package, metrics = pcall(require, 'metrics')

local dev_checks = require('crud.common.dev_checks')
local op_module = require('crud.stats.operation')
local registry_common = require('crud.stats.registry_common')

local registry = {}
local internal_registry = {}

local metric_name = {
    -- Summary collector for all operations.
    stats = 'tnt_crud_stats',
    -- `*_count` and `*_sum` are automatically created
    -- by summary collector.
    stats_count = 'tnt_crud_stats_count',
    stats_sum = 'tnt_crud_stats_sum',

    -- Counter collector for spaces not found.
    space_not_found = 'tnt_crud_space_not_found',

    -- Counter collectors for select/pairs details.
    details = {
        tuples_fetched = 'tnt_crud_tuples_fetched',
        tuples_lookup = 'tnt_crud_tuples_lookup',
        map_reduces = 'tnt_crud_map_reduces',
    }
}

local LATENCY_QUANTILE = 0.99

-- Increasing tolerance threshold affects performance.
local DEFAULT_QUANTILES = {
    [LATENCY_QUANTILE] = 1e-3,
}

local DEFAULT_SUMMARY_PARAMS = {
    age_buckets_count = 2,
    max_age_time = 60,
}

--- Check if application supports metrics rock for registry
--
--  `metrics >= 0.9.0` is required to use summary with
--  age buckets. `metrics >= 0.5.0, < 0.9.0` is unsupported
--  due to quantile overflow bug
--  (https://github.com/tarantool/metrics/issues/235).
--
-- @function is_supported
--
-- @treturn boolean Returns true if `metrics >= 0.9.0` found, false otherwise.
--
function registry.is_supported()
    if is_package == false then
        return false
    end

    -- Only metrics >= 0.9.0 supported.
    local is_summary, summary = pcall(require, 'metrics.collectors.summary')
    if is_summary == false or summary.rotate_age_buckets == nil then
        return false
    end

    return true
end

--- Initialize collectors in global metrics registry
--
--  Registries are not meant to used explicitly
--  by users, init is not guaranteed to be idempotent.
--  Destroy collectors only through this registry methods.
--
-- @function init
--
-- @treturn boolean Returns true.
--
function registry.init()
    internal_registry[metric_name.stats] = metrics.summary(
        metric_name.stats,
        'CRUD router calls statistics',
        DEFAULT_QUANTILES,
        DEFAULT_SUMMARY_PARAMS)

    internal_registry[metric_name.space_not_found] = metrics.counter(
        metric_name.space_not_found,
        'Spaces not found during CRUD calls')

    internal_registry[metric_name.details.tuples_fetched] = metrics.counter(
        metric_name.details.tuples_fetched,
        'Tuples fetched from CRUD storages during select/pairs')

    internal_registry[metric_name.details.tuples_lookup] = metrics.counter(
        metric_name.details.tuples_lookup,
        'Tuples looked up on CRUD storages while collecting response during select/pairs')

    internal_registry[metric_name.details.map_reduces] = metrics.counter(
        metric_name.details.map_reduces,
        'Map reduces planned during CRUD select/pairs')

    return true
end

--- Unregister collectors in global metrics registry
--
--  Registries are not meant to used explicitly
--  by users, destroy is not guaranteed to be idempotent.
--  Destroy collectors only through this registry methods.
--
-- @function destroy
--
-- @treturn boolean Returns true.
--
function registry.destroy()
    for _, c in pairs(internal_registry) do
        metrics.registry:unregister(c)
    end

    internal_registry = {}
    return true
end

--- Get copy of global metrics registry
--
--  Registries are not meant to used explicitly
--  by users, get is not guaranteed to work without init.
--
-- @function get
--
-- @tparam string space_name
--  (Optional) If specified, returns table with statistics
--  of operations on table, separated by operation type and
--  execution status. If there wasn't any requests for table,
--  returns {}. In not specified, returns table with statistics
--  about all existing spaces and count of calls to spaces
--  that wasn't found.
--
-- @treturn table Returns copy of metrics registry.
function registry.get(space_name)
    dev_checks('?string')

    local stats = {
        spaces = {},
        space_not_found = 0,
    }

    -- Fill operation basic statistics values.
    for _, obs in ipairs(internal_registry[metric_name.stats]:collect()) do
        local op = obs.label_pairs.operation
        local status = obs.label_pairs.status
        local name = obs.label_pairs.name

        if space_name ~= nil and name ~= space_name then
            goto stats_continue
        end

        registry_common.init_collectors_if_required(stats.spaces, name, op)
        local space_stats = stats.spaces[name]

        if obs.metric_name == metric_name.stats then
            if obs.label_pairs.quantile == LATENCY_QUANTILE then
                space_stats[op][status].latency = obs.value
            end
        elseif obs.metric_name == metric_name.stats_sum then
            space_stats[op][status].time = obs.value
        elseif obs.metric_name == metric_name.stats_count then
            space_stats[op][status].count = obs.value
        end

        :: stats_continue ::
    end

    -- Fill select/pairs detail statistics values.
    for stat_name, metric_name in pairs(metric_name.details) do
        for _, obs in ipairs(internal_registry[metric_name]:collect()) do
            local name = obs.label_pairs.name
            local op = obs.label_pairs.operation

            if space_name ~= nil and name ~= space_name then
                goto details_continue
            end

            registry_common.init_collectors_if_required(stats.spaces, name, op)
            stats.spaces[name][op].details[stat_name] = obs.value

            :: details_continue ::
        end
    end

    if space_name ~= nil then
        return stats.spaces[space_name] or {}
    end

    local _, obs = next(internal_registry[metric_name.space_not_found]:collect())
    if obs ~= nil then
        stats.space_not_found = obs.value
    end

    return stats
end

--- Check if space statistics are present in registry
--
-- @function is_unknown_space
--
-- @tparam string space_name
--  Name of space.
--
-- @treturn boolean True, if space stats found. False otherwise.
--
function registry.is_unknown_space(space_name)
    dev_checks('string')

    for _, obs in ipairs(internal_registry[metric_name.stats]:collect()) do
        local name = obs.label_pairs.name

        if name == space_name then
            return false
        end
    end

    for _, metric_name in pairs(metric_name.details) do
        for _, obs in ipairs(internal_registry[metric_name]:collect()) do
            local name = obs.label_pairs.name

            if name == space_name then
                return false
            end
        end
    end

    return true
end

--- Increase requests count and update latency info
--
-- @function observe
--
-- @tparam string space_name
--  Name of space.
--
-- @tparam number latency
--  Time of call execution.
--
-- @tparam string op
--  Label of registry collectors.
--  Use `require('crud.common.const').OP` to pick one.
--
-- @tparam string success
--  'ok' if no errors on execution, 'error' otherwise.
--
-- @treturn boolean Returns true.
--
function registry.observe(latency, space_name, op, status)
    dev_checks('number', 'string', 'string', 'string')

    -- Use `operations` label to be consistent with `tnt_stats_op_*` labels.
    -- Use `name` label to be consistent with `tnt_space_*` labels.
    -- Use `status` label to be consistent with `tnt_vinyl_*` and HTTP metrics labels.
    local label_pairs = { operation = op, name = space_name, status = status }

    internal_registry[metric_name.stats]:observe(latency, label_pairs)

    return true
end

--- Increase count of "space not found" collector by one
--
-- @function observe_space_not_found
--
-- @treturn boolean Returns true.
--
function registry.observe_space_not_found()
    internal_registry[metric_name.space_not_found]:inc(1)

    return true
end

--- Increase statistics of storage select/pairs calls
--
-- @function observe_fetch
--
-- @tparam string space_name
--  Name of space.
--
-- @tparam number tuples_fetched
--  Count of tuples fetched during storage call.
--
-- @tparam number tuples_lookup
--  Count of tuples looked up on storages while collecting response.
--
-- @treturn boolean Returns true.
--
function registry.observe_fetch(tuples_fetched, tuples_lookup, space_name)
    dev_checks('number', 'number', 'string')

    local label_pairs = { name = space_name, operation = op_module.SELECT }

    internal_registry[metric_name.details.tuples_fetched]:inc(tuples_fetched, label_pairs)
    internal_registry[metric_name.details.tuples_lookup]:inc(tuples_lookup, label_pairs)

    return true
end

--- Increase statistics of planned map reduces during select/pairs
--
-- @function observe_map_reduces
--
-- @tparam number count
--  Count of map reduces planned.
--
-- @tparam string space_name
--  Name of space.
--
-- @treturn boolean Returns true.
--
function registry.observe_map_reduces(count, space_name)
    dev_checks('number', 'string')

    local label_pairs = { name = space_name, operation = op_module.SELECT }
    internal_registry[metric_name.details.map_reduces]:inc(count, label_pairs)

    return true
end

return registry
