local clock = require('clock')
local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local op_module = require('crud.stats.operation')

local StatsError = errors.new_class('StatsError', {capture_stack = false})

local stats = {}
local internal = {
    registry = nil,
    driver = nil,
}
stats.internal = internal

local local_registry = require('crud.stats.local_registry')
local metrics_registry = require('crud.stats.metrics_registry')

local drivers = {
    ['local'] = local_registry,
}
if metrics_registry.is_supported() then
    drivers['metrics'] = metrics_registry
end

--- Check if statistics module if enabled
--
-- @function is_enabled
--
-- @treturn[1] boolean Returns true or false.
--
function stats.is_enabled()
    return internal.registry ~= nil
end

--- Initializes statistics registry, enables callbacks and wrappers
--
--  If already enabled, do nothing.
--
-- @function enable
--
-- @tparam table opts
--
--  @tfield string driver
--   'local' or 'metrics'.
--   If 'local', stores statistics in local registry (some Lua tables)
--   and computes latency as overall average. 'metrics' requires
--   `metrics >= 0.9.0` installed and stores statistics in
--   global metrics registry (integrated with exporters)
--   and computes latency as 0.99 quantile with aging.
--   If 'metrics' driver is available, it is used by default,
--   otherwise 'local' is used.
--
-- @treturn boolean Returns true.
--
function stats.enable(opts)
    checks({ driver = '?string' })

    StatsError:assert(
        rawget(_G, 'crud') ~= nil,
        "Can be enabled only on crud router"
    )

    opts = opts or {}
    if opts.driver == nil then
        if drivers.metrics ~= nil then
            opts.driver = 'metrics'
        else
            opts.driver = 'local'
        end
    end

    StatsError:assert(
        drivers[opts.driver] ~= nil,
        'Unsupported driver: %s', opts.driver
    )

    if internal.driver == opts.driver then
        return true
    end

    -- Disable old driver registry, if another one was requested.
    stats.disable()

    internal.driver = opts.driver
    internal.registry = drivers[opts.driver]
    internal.registry.init()

    return true
end

--- Resets statistics registry
--
--  After reset collectors set is the same as right
--  after first stats.enable().
--
-- @function reset
--
-- @treturn boolean Returns true.
--
function stats.reset()
    if not stats.is_enabled() then
        return true
    end

    internal.registry.destroy()
    internal.registry.init()

    return true
end

--- Destroys statistics registry and disable callbacks
--
--  If already disabled, do nothing.
--
-- @function disable
--
-- @treturn boolean Returns true.
--
function stats.disable()
    if not stats.is_enabled() then
        return true
    end

    internal.registry.destroy()
    internal.registry = nil
    internal.driver = nil

    return true
end

--- Get statistics on CRUD operations
--
-- @function get
--
-- @tparam string space_name
--  (Optional) If specified, returns table with statistics
--  of operations on space, separated by operation type and
--  execution status. If there wasn't any requests of "op" type
--  for space, there won't be corresponding collectors.
--  If not specified, returns table with statistics
--  about all observed spaces and count of calls to spaces
--  that wasn't found.
--
-- @treturn table Statistics on CRUD operations.
--  If statistics disabled, returns {}.
--
function stats.get(space_name)
    checks('?string')

    if not stats.is_enabled() then
        return {}
    end

    return internal.registry.get(space_name)
end

local function wrap_tail(space_name, op, opts, start_time, call_status, ...)
    local finish_time = clock.monotonic()
    local latency = finish_time - start_time

    local err = nil
    local status = 'ok'
    if call_status == false then
        status = 'error'
        err = select(1, ...)
    end

    -- If not `pairs` call, return values `nil, err`
    -- treated as error case.
    local second_return_val = select(2, ...)
    if opts.pairs == false and second_return_val ~= nil then
        status = 'error'
        err = second_return_val
    end

    local context_stats = utils.get_context_section('router_stats')
    -- Describe local variables to use `goto`.
    local space_not_found_msg, space

    -- If space not exists, do not build a separate collector for it.
    -- Call request for non-existing space will always result in error.
    -- The resulting overhead is insignificant for existing spaces:
    -- at worst it would be a single excessive check for an instance lifetime.
    -- If we can't verify space existence because of network errors,
    -- it is treated as unknown as well.
    if status == 'error' and internal.registry.is_unknown_space(space_name) then
        if type(err) == 'table' and type(err.err) == 'string' then
            space_not_found_msg = utils.space_doesnt_exist_msg(space_name)
            if string.find(err.err, space_not_found_msg) ~= nil then
                internal.registry.observe_space_not_found()
                goto return_values
            end
        end

        -- We can't rely only on parsing error value because space existence
        -- is not always the first check in request validation.
        -- Check explicitly if space do not exist.
        space = utils.get_space(space_name, vshard.router.routeall())
        if space == nil then
            internal.registry.observe_space_not_found()
            goto return_values
        end
    end

    -- If space id is provided instead of name, resolve name.
    if type(space_name) ~= 'string' then
        if space == nil then
            space = utils.get_space(space_name, vshard.router.routeall())
        end

        space_name = space.name
    end

    internal.registry.observe(latency, space_name, op, status)

    if context_stats ~= nil then
        if context_stats.map_reduces ~= nil then
            internal.registry.observe_map_reduces(
                context_stats.map_reduces, space_name)
        end
        utils.drop_context_section('router_stats')
    end

    :: return_values ::

    if call_status == false then
        error((...), 2)
    end

    return ...
end

--- Wrap CRUD operation call to collect statistics
--
--  Approach based on `box.atomic()`:
--  https://github.com/tarantool/tarantool/blob/b9f7204b5e0d10b443c6f198e9f7f04e0d16a867/src/box/lua/schema.lua#L369
--
-- @function wrap
--
-- @tparam function func
--  Function to wrap. First argument is expected to
--  be a space name string. If statistics enabled,
--  errors are caught and thrown again.
--
-- @tparam string op
--  Label of registry collectors.
--  Use `require('crud.stats.module').op` to pick one.
--
-- @tparam table opts
--
--  @tfield boolean pairs
--   (Optional, default: false) If false, second return value
--   of wrapped function is treated as error (`nil, err` case).
--   Since pairs calls return three arguments as generator
--   and throw errors if needed, use { pairs = true } to
--   wrap them.
--
-- @return First two arguments of wrapped function output.
--
function stats.wrap(func, op, opts)
    dev_checks('function', 'string', { pairs = '?boolean' })

    return function(...)
        if not stats.is_enabled() then
            return func(...)
        end

        if opts == nil then opts = {} end
        if opts.pairs == nil then opts.pairs = false end

        local space_name = select(1, ...)

        local start_time = clock.monotonic()

        return wrap_tail(
            space_name, op, opts, start_time,
            pcall(func, ...)
        )
    end
end

local storage_stats_schema = { tuples_fetched = 'number', tuples_lookup = 'number' }
--- Callback to collect storage tuples stats (select/pairs)
--
-- @function update_fetch_stats
--
-- @tparam table storage_stats
--  Statistics from select storage call.
--
--  @tfield number tuples_fetched
--   Count of tuples fetched during storage call.
--
--  @tfield number tuples_lookup
--   Count of tuples looked up on storages while collecting response.
--
-- @tparam string space_name
--  Name of space.
--
-- @treturn boolean Returns true.
--
local function update_fetch_stats(storage_stats, space_name)
    dev_checks(storage_stats_schema, 'string')

    if not stats.is_enabled() then
        return true
    end

    internal.registry.observe_fetch(
        storage_stats.tuples_fetched,
        storage_stats.tuples_lookup,
        space_name
    )

    return true
end

--- Returns callback to collect storage tuples stats (select/pairs)
--
-- @function get_fetch_callback
--
-- @treturn[1] function `update_fetch_stats` function to collect tuples stats.
-- @treturn[2] function Dummy function, if stats disabled.
--
function stats.get_fetch_callback()
    if not stats.is_enabled() then
        return utils.pass
    end

    return update_fetch_stats
end

--- Table with CRUD operation lables
--
-- @table label
--
stats.op = op_module

return stats
