local dev_checks = require('crud.common.dev_checks')

local registry_common = {}

--- Build collectors for local registry
--
-- @function build_collectors
--
-- @treturn table Returns collectors for success and error requests.
--  Collectors store 'count', 'latency' and 'time' values.
--
function registry_common.build_collectors()
    local collectors = {
        ok = {
            count = 0,
            latency = 0,
            time = 0,
        },
        error = {
            count = 0,
            latency = 0,
            time = 0,
        },
    }

    return collectors
end

--- Initialize all statistic collectors for a space operation
--
-- @function init_collectors_if_required
--
-- @tparam table spaces
--  `spaces` section of registry.
--
-- @tparam string space_name
--  Name of space.
--
-- @tparam string op
--  Label of registry collectors.
--  Use `require('crud.stats.module').op` to pick one.
--
function registry_common.init_collectors_if_required(spaces, space_name, op)
    dev_checks('table', 'string', 'string')

    if spaces[space_name] == nil then
        spaces[space_name] = {}
    end

    local space_collectors = spaces[space_name]
    if space_collectors[op] == nil then
        space_collectors[op] = registry_common.build_collectors()
    end
end

return registry_common
