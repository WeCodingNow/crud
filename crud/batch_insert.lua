local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local BatchInsertError = errors.new_class('BatchInsertError', {capture_stack = false})

local batch_insert = {}

local BATCH_INSERT_FUNC_NAME = '_crud.batch_insert_on_storage'

local function batch_insert_on_storage(space_name, batch, opts)
    dev_checks('string', 'table', {
        fields = '?table',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, BatchInsertError:new("Space %q doesn't exist", space_name)
    end

    local inserted_tuples = {}

    box.begin()
    for _, tuple in ipairs(batch) do
        local insert_result = schema.wrap_box_space_func_result(space, 'insert', {tuple}, {
            field_names = opts.fields,
        })

        table.insert(inserted_tuples, insert_result.res)
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

function batch_insert.init()
    _G._crud.batch_insert_on_storage = batch_insert_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_batch_insert_on_router(space_name, tuples, opts)
    dev_checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, {BatchInsertError:new("Space %q doesn't exist", space_name)}, true
    end

    local batches_by_replicasets, err = sharding.split_tuples_by_replicaset(tuples, space)
    if err ~= nil then
        return nil, {err}, true
    end

    local batch_insert_on_storage_opts = {
        fields = opts.fields,
    }

    local call_opts = {
        timeout = opts.timeout,
        is_async = true,
    }

    local futures_by_replicasets = {}
    for replicaset, batch in pairs(batches_by_replicasets) do
        local func_args = {
            space_name,
            batch,
            batch_insert_on_storage_opts,
        }

        local future = replicaset:call(BATCH_INSERT_FUNC_NAME, func_args, call_opts)
        futures_by_replicasets[replicaset.uuid] = future
    end

    local results, errs = call.batch(
        futures_by_replicasets,
        BATCH_INSERT_FUNC_NAME,
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

--- Batch inserts tuples to the specified space
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
--  Options of batch_insert.tuples_batch
--
-- @return[1] tuples
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_insert.tuples_batch(space_name, tuples, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    return schema.wrap_func_reload(call_batch_insert_on_router, space_name, tuples, opts)
end

--- Batch inserts objects to the specified space
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
--  Options of batch_insert.tuples_batch
--
-- @return[1] objects
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_insert.objects_batch(space_name, objs, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
    })

    local tuples = {}
    for _, obj in ipairs(objs) do

        local tuple, err = utils.flatten_obj_reload(space_name, obj)
        if err ~= nil then
            local err_obj = BatchInsertError:new("Failed to flatten object: %s", err)
            err_obj.tuple = obj
            return nil, {err_obj}
        end

        table.insert(tuples, tuple)
    end

    return batch_insert.tuples_batch(space_name, tuples, opts)
end

return batch_insert
