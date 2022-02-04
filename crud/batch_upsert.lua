local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local BatchUpsertError = errors.new_class('BatchUpsertError', {capture_stack = false})

local batch_upsert = {}

local BATCH_UPSERT_FUNC_NAME = '_crud.batch_upsert_on_storage'

local function batch_upsert_on_storage(space_name, batch, operations)
    dev_checks('string', 'table', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, BatchUpsertError:new("Space %q doesn't exist", space_name)
    end

    local inserted_tuples = {}

    box.begin()
    for _, tuple in ipairs(batch) do
        local insert_result = schema.wrap_box_space_func_result(space, 'upsert', {tuple, operations}, {})

        table.insert(inserted_tuples, insert_result)
        if insert_result.err ~= nil then
            box.commit()
            return nil, {
                err = insert_result.err,
                tuple = tuple,
            }
        end
    end
    box.commit()

    return inserted_tuples
end

function batch_upsert.init()
    _G._crud.batch_upsert_on_storage = batch_upsert_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_batch_upsert_on_router(space_name, tuples, user_operations, opts)
    dev_checks('string', 'table', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, {BatchUpsertError:new("Space %q doesn't exist", space_name)}, true
    end

    local space_format = space:format()
    local operations = user_operations
    local err
    if not utils.tarantool_supports_fieldpaths() then
        operations, err = utils.convert_operations(user_operations, space_format)
        if err ~= nil then
            return nil, {BatchUpsertError:new("Wrong operations are specified: %s", err)}, true
        end
    end

    local batches_by_replicasets, err = sharding.split_tuples_by_replicaset(tuples, space)
    if err ~= nil then
        return nil, {err}, true
    end

    local call_opts = {
        timeout = opts.timeout,
        is_async = true,
    }

    local futures_by_replicasets = {}
    for replicaset, batch in pairs(batches_by_replicasets) do
        local func_args = {
            space_name,
            batch,
            operations,
        }

        local future = replicaset:call(BATCH_UPSERT_FUNC_NAME, func_args, call_opts)
        futures_by_replicasets[replicaset.uuid] = future
    end

    local results, errs = call.batch(
            futures_by_replicasets,
            BATCH_UPSERT_FUNC_NAME,
            opts.timeout
    )

    local rows = {}
    for _, result in pairs(results) do
        rows = utils.table_extend(rows, result[1])
    end

    if next(rows) == nil then
        return nil, errs
    end

    local res, err = utils.format_result(rows, space, opts.fields)
    if err ~= nil then
        return nil, {err}
    end

    return res, errs
end

--- Batch update or insert tuples to the specified space
--
-- @function tuples_batch
--
-- @param string space_name
--  A space name
--
-- @param table tuples
--  Tuples
--
-- @tparam ?table opts
--  Options of batch_upsert.tuples_batch
--
-- @return[1] tuples
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_upsert.tuples_batch(space_name, tuples, user_operations, opts)
    checks('string', 'table', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    return schema.wrap_func_reload(call_batch_upsert_on_router, space_name, tuples, user_operations, opts)
end

--- Batch update or insert objects to the specified space
--
-- @function objects_batch
--
-- @param string space_name
--  A space name
--
-- @param table objs
--  Objects
--
-- @tparam ?table opts
--  Options of batch_upsert.tuples_batch
--
-- @return[1] objects
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_upsert.objects_batch(space_name, objs, user_operations, opts)
    checks('string', 'table', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    local tuples = {}
    for _, obj in ipairs(objs) do

        local tuple, err = utils.flatten_obj_reload(space_name, obj)
        if err ~= nil then
            local err_obj = BatchUpsertError:new("Failed to flatten object: %s", err)
            err_obj.tuple = obj
            return nil, {err_obj}
        end

        table.insert(tuples, tuple)
    end

    return batch_upsert.tuples_batch(space_name, tuples, user_operations, opts)
end

return batch_upsert
