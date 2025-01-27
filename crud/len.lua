local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local LenError = errors.new_class('LenError', {capture_stack = false})

local len = {}

local LEN_FUNC_NAME = '_crud.len_on_storage'

local function len_on_storage(space_name)
    dev_checks('string|number')

    return box.space[space_name]:len()
end

function len.init()
    _G._crud.len_on_storage = len_on_storage
end

--- Calculates the number of tuples in the space for memtx engine
--- Calculates the maximum approximate number of tuples in the space for vinyl engine
--
-- @function call
--
-- @param string|number space_name
--  A space name as well as numerical id
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] number
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function len.call(space_name, opts)
    checks('string|number', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, LenError:new("Space %q doesn't exist", space_name)
    end

    local results, err = vshard.router.map_callrw(LEN_FUNC_NAME, {space_name}, opts)

    if err ~= nil then
        return nil, LenError:new("Failed to call len on storage-side: %s", err)
    end

    local total_len = 0
    for _, replicaset_results in pairs(results) do
        if replicaset_results[1] ~= nil then
            total_len = total_len + replicaset_results[1]
        end
    end

    return total_len
end

return len
