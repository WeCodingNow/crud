local log = require('log')

local compat = {}

function compat.require(module_name, builtin_module_name)
    local module_cached_name = string.format('__crud_%s_cached', module_name)

    local module

    local module_cached = rawget(_G, module_cached_name)
    if module_cached ~= nil then
        module = module_cached
    elseif package.search(module_name) then
        -- we don't use pcall(require, modile_name) here because it
        -- leads to ignoring errors other than 'No LuaRocks module found'
        log.info('%q module is used', module_name)
        module = require(module_name)
    else
        log.info('%q module is not found. Built-in %q is used', module_name, builtin_module_name)
        module = require(builtin_module_name)
    end

    rawset(_G, module_cached_name, module)

    local hotreload = package.loaded['cartridge.hotreload']
    if hotreload ~= nil then
        hotreload.whitelist_globals({module_cached_name})
    end

    return module
end

return compat