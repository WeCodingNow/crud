--- Tarantool module for performing operations across the cluster
--
-- @module crud

local insert = require('crud.insert')
local replace = require('crud.replace')
local get = require('crud.get')
local update = require('crud.update')
local upsert = require('crud.upsert')
local delete = require('crud.delete')
local select = require('crud.select')
local truncate = require('crud.truncate')
local len = require('crud.len')
local count = require('crud.count')
local borders = require('crud.borders')
local sharding_metadata = require('crud.common.sharding.sharding_metadata')
local utils = require('crud.common.utils')
local tracing = require('tracing_decorator')

local crud = {}

--- CRUD operations.
-- @section crud

-- @refer insert.tuple
-- @function insert
crud.insert = insert.tuple
crud.insert = tracing.decorate(crud.insert, 'crud.insert')

-- @refer insert.object
-- @function insert_object
crud.insert_object = insert.object
crud.insert_object = tracing.decorate(crud.insert_object, 'crud.insert_object')

-- @refer get.call
-- @function get
crud.get = get.call
crud.get = tracing.decorate(crud.get, 'crud.get')

-- @refer replace.tuple
-- @function replace
crud.replace = replace.tuple
crud.replace = tracing.decorate(crud.replace, 'crud.replace')

-- @refer replace.object
-- @function replace_object
crud.replace_object = replace.object
crud.replace_object = tracing.decorate(crud.replace_object, 'crud.replace_object')

-- @refer update.call
-- @function update
crud.update = update.call
crud.update = tracing.decorate(crud.update, 'crud.update')

-- @refer upsert.tuple
-- @function upsert
crud.upsert = upsert.tuple
crud.upsert = tracing.decorate(crud.upsert, 'crud.upsert')

-- @refer upsert.object
-- @function upsert
crud.upsert_object = upsert.object
crud.upsert_object = tracing.decorate(crud.upsert_object, 'crud.upsert_object')

-- @refer delete.call
-- @function delete
crud.delete = delete.call
crud.delete = tracing.decorate(crud.delete, 'crud.delete')

-- @refer select.call
-- @function select
crud.select = select.call
crud.select = tracing.decorate(crud.select, 'crud.select')

-- @refer select.pairs
-- @function pairs
crud.pairs = select.pairs
crud.pairs = tracing.decorate(crud.pairs, 'crud.pairs')

-- @refer utils.unflatten_rows
-- @function unflatten_rows
crud.unflatten_rows = utils.unflatten_rows
crud.unflatten_rows = tracing.decorate(crud.unflatten_rows, 'crud.unflatten_rows')

-- @refer truncate.call
-- @function truncate
crud.truncate = truncate.call
crud.truncate = tracing.decorate(crud.truncate, 'crud.truncate')

-- @refer len.call
-- @function len
crud.len = len.call
crud.len = tracing.decorate(crud.len, 'crud.len')

-- @refer count.call
-- @function count
crud.count = count.call
crud.count = tracing.decorate(crud.count, 'crud.count')

-- @refer borders.min
-- @function min
crud.min = borders.min
crud.min = tracing.decorate(crud.min, 'crud.min')

-- @refer borders.max
-- @function max
crud.max = borders.max
crud.max = tracing.decorate(crud.max, 'crud.max')

-- @refer utils.cut_rows
-- @function cut_rows
crud.cut_rows = utils.cut_rows
crud.cut_rows = tracing.decorate(crud.cut_rows, 'crud.cut_rows')

-- @refer utils.cut_objects
-- @function cut_objects
crud.cut_objects = utils.cut_objects
crud.cut_objects = tracing.decorate(crud.cut_objects, 'crud.cut_objects')

--- Initializes crud on node
--
-- Exports all functions that are used for calls
-- and CRUD operations.
--
-- @function init
--
function crud.init_storage()
    if rawget(_G, '_crud') == nil then
        rawset(_G, '_crud', {})
    end

    tracing.init()

    insert.init()
    get.init()
    replace.init()
    update.init()
    upsert.init()
    delete.init()
    select.init()
    truncate.init()
    len.init()
    count.init()
    borders.init()
    sharding_metadata.init()
end

function crud.init_router()
   rawset(_G, 'crud', crud)
end

function crud.stop_router()
    rawset(_G, 'crud', nil)
end

function crud.stop_storage()
    rawset(_G, '_crud', nil)
end

return crud
