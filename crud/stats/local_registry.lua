local dev_checks = require('crud.common.dev_checks')
local registry_common = require('crud.stats.registry_common')

local registry = {}
local internal_registry = {}

--- Initialize local metrics registry
--
--  Registries are not meant to used explicitly
--  by users, init is not guaranteed to be idempotent.
--
-- @function init
--
-- @treturn boolean Returns true.
--
function registry.init()
    internal_registry.spaces = {}
    internal_registry.space_not_found = 0

    return true
end

--- Destroy local metrics registry
--
--  Registries are not meant to used explicitly
--  by users, destroy is not guaranteed to be idempotent.
--
-- @function destroy
--
-- @treturn boolean Returns true.
--
function registry.destroy()
    internal_registry = {}

    return true
end

--- Get copy of local metrics registry
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
-- @treturn table Returns copy of metrics registry (or registry section).
--
function registry.get(space_name)
    dev_checks('?string')

    if space_name ~= nil then
        return table.deepcopy(internal_registry.spaces[space_name]) or {}
    end

    return table.deepcopy(internal_registry)
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

    return internal_registry.spaces[space_name] == nil
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

    registry_common.init_collectors_if_required(internal_registry.spaces, space_name, op)
    local collectors = internal_registry.spaces[space_name][op][status]

    collectors.count = collectors.count + 1
    collectors.time = collectors.time + latency
    collectors.latency = collectors.time / collectors.count

    return true
end

--- Increase count of "space not found" collector by one
--
-- @function observe_space_not_found
--
-- @treturn boolean Returns true.
--
function registry.observe_space_not_found()
    internal_registry.space_not_found = internal_registry.space_not_found + 1

    return true
end

return registry
