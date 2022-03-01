local log = require('log')
local opentracing = require('opentracing')
local zipkin = require('zipkin.tracer')
local uuid = require('uuid')
local fiber = require('fiber')


local tracing = {}


-- context
local CONTEXT = 'trace_ctx'

function tracing.get_context()
    -- TODO: check for null
    local context = fiber.self().storage[CONTEXT]

    if not context then
        context = {}
        fiber.self().storage[CONTEXT] = context
    end

    return context
end

local function gen_uuid_str()
    return string.hex(uuid.new():bin())
end

-- XXX: probably should move it to a context class method
function tracing.set_fiber_trace_id(trace_id)
    -- TODO: add a check for uuid
    local context = tracing.get_context()

    if not trace_id then
        trace_id = gen_uuid_str()
    end

    context.trace_id = trace_id
    return trace_id
end

-- XXX: prolly should move to another module/class
function tracing.span_context_stack_init()
    local context = tracing.get_context()

    if not context.span_context_stack then
        context.span_context_stack = {
            carriers = {},
            cur = 0,
        }
    end
end

function tracing.trace_stack_push(carrier)
    local context = tracing.get_context()
    local span_context_stack = context.span_context_stack

    span_context_stack.cur = span_context_stack.cur + 1
    span_context_stack.carriers[span_context_stack.cur] = carrier
end

function tracing.trace_stack_pop()
    local context = tracing.get_context()
    local span_context_stack = context.span_context_stack
    assert(span_context_stack.cur > 0)

    local span_ctx = span_context_stack.carriers[span_context_stack.cur]
    -- XXX: maybe delete?
    span_context_stack.cur = span_context_stack.cur - 1

    return span_ctx
end

function tracing.trace_stack_top()
    local context = tracing.get_context()
    local span_context_stack = context.span_context_stack
    assert(span_context_stack.cur > 0)

    local span_ctx = span_context_stack.carriers[span_context_stack.cur]

    return span_ctx
end

-- opentracing spans wrapper
function tracing.start_span(name, opts)
    opts = opts or {}
    -- 1. this is on entrypoint
    -- 2. this is a child span
    local context = tracing.get_context()
    tracing.span_context_stack_init()

    local span
    local trace_id = context.trace_id
    if not trace_id then
        trace_id = tracing.set_fiber_trace_id(opts.trace_id)

        span = opentracing.start_span(name, {
            trace_id = trace_id
        })
    else
        local carrier = tracing.trace_stack_top()
        span = opentracing.start_span_from_context(
            opentracing.map_extract(carrier),
            name
        )
    end

    return span
end



-- util
local util = {}

function util.wrap_multires(...)
    return {
        n = select('#', ...),
        ...,
    }
end


function tracing.decorate(f, name)
    return function (...)
        log.info(("decorator of %s begins work"):format(name))

        local span = tracing.start_span(name)

        local carrier = {}
        opentracing.map_inject(span:context(), carrier)
        tracing.trace_stack_push(carrier)

        local packed = util.wrap_multires(f(...))

        log.info(("decorator of %s ends"):format(name))

        span:finish()
        tracing.trace_stack_pop()
        return unpack(packed, 1, packed.n)
    end
end

function tracing.init()
    log.info('INITIALIZING TRACING')

    log.info('INITIALIZING TRACING DONE')
end


return tracing
