local clock = require('clock')
local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local op_module = require('crud.stats.operation')
local registry = require('crud.stats.local_registry')

local StatsError = errors.new_class('StatsError', {capture_stack = false})

local stats = {}
local is_enabled = false

--- Initializes statistics registry, enables callbacks and wrappers
--
--  If already enabled, do nothing.
--
-- @function enable
--
-- @treturn boolean Returns true.
--
function stats.enable()
    if is_enabled then
        return true
    end

    StatsError:assert(
        rawget(_G, 'crud') ~= nil,
        "Can be enabled only on crud router"
    )

    registry.init()
    is_enabled = true

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
    if not is_enabled then
        return true
    end

    registry.destroy()
    registry.init()

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
    if not is_enabled then
        return true
    end

    registry.destroy()
    is_enabled = false

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

    return registry.get(space_name)
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
    if status == 'error' and registry.is_unknown_space(space_name) then
        if type(err) == 'table' and type(err.err) == 'string' then
            space_not_found_msg = utils.space_doesnt_exist_msg(space_name)
            if string.find(err.err, space_not_found_msg) ~= nil then
                registry.observe_space_not_found()
                goto return_values
            end
        end

        -- We can't rely only on parsing error value because space existence
        -- is not always the first check in request validation.
        -- Check explicitly if space do not exist.
        space = utils.get_space(space_name, vshard.router.routeall())
        if space == nil then
            registry.observe_space_not_found()
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

    registry.observe(latency, space_name, op, status)

    if context_stats ~= nil then
        if context_stats.map_reduces ~= nil then
            registry.observe_map_reduces(context_stats.map_reduces, space_name)
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
        if not is_enabled then
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

    if not is_enabled then
        return true
    end

    registry.observe_fetch(
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
    if not is_enabled then
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
