# This file is a part of Julia. License is MIT: http://julialang.org/license

abstract AbstractWorkerPool

# An AbstractWorkerPool should implement
#
# `push!` - add a new worker into the pool
# `put!` - put back a worker to the pool
# `take!` - take a worker from the pool
# `length` - number of workers available in the pool
# `isready` - return false if a `take!` on the pool would block, else true
#
# The default implementations of the above (on a AbstractWorkerPool) require fields
#    channel::RemoteChannel{Channel{Int}}
#    workers::Set{Int}
#

type WorkerPool <: AbstractWorkerPool
    channel::RemoteChannel{Channel{Int}}
    workers::Set{Int}

    # Create a shared queue of available workers
    WorkerPool() = new(RemoteChannel(()->Channel{Int}(typemax(Int))), Set{Int}())
end


"""
    WorkerPool(workers)

Create a WorkerPool from a vector of worker ids.
"""
function WorkerPool(workers::Vector{Int})
    pool = WorkerPool()

    # Add workers to the pool
    for w in workers
        push!(pool, w)
    end

    return pool
end

push!(pool::AbstractWorkerPool, w::Int) = (push!(pool.workers, w); put!(pool.channel, w); pool)
push!(pool::AbstractWorkerPool, w::Worker) = push!(pool, w.id)
length(pool::AbstractWorkerPool) = length(pool.workers)
isready(pool::AbstractWorkerPool) = isready(pool.channel)

put!(pool::AbstractWorkerPool, w::Int) = (put!(pool.channel, w); pool)

workers(pool::AbstractWorkerPool) = collect(pool.workers)

function take!(pool::AbstractWorkerPool)
    # Find an active worker
    worker = 0
    while true
        if length(pool) == 0
            if pool === default_worker_pool()
                # No workers, the master process is used as a worker
                worker = 1
                break
            else
                throw(ErrorException("No active worker available in pool"))
            end
        end

        worker = take!(pool.channel)
        if worker in procs()
            break
        else
            delete!(pool.workers, worker) # Remove invalid worker from pool
        end
    end
    return worker
end

function remotecall_pool(rc_f, f, pool::AbstractWorkerPool, args...; kwargs...)
    worker = take!(pool)
    try
        rc_f(f, worker, args...; kwargs...)
    finally
        worker in pool.workers && put!(pool, worker)
    end
end

"""
    remotecall(f, pool::AbstractWorkerPool, args...; kwargs...)

Call `f(args...; kwargs...)` on one of the workers in `pool`. Returns a `Future`.
"""
remotecall(f, pool::AbstractWorkerPool, args...; kwargs...) = remotecall_pool(remotecall, f, pool, args...; kwargs...)


"""
    remotecall_wait(f, pool::AbstractWorkerPool, args...; kwargs...)

Call `f(args...; kwargs...)` on one of the workers in `pool`. Waits for completion, returns a `Future`.
"""
remotecall_wait(f, pool::AbstractWorkerPool, args...; kwargs...) = remotecall_pool(remotecall_wait, f, pool, args...; kwargs...)


"""
    remotecall_fetch(f, pool::AbstractWorkerPool, args...; kwargs...)

Call `f(args...; kwargs...)` on one of the workers in `pool`. Waits for completion and returns the result.
"""
remotecall_fetch(f, pool::AbstractWorkerPool, args...; kwargs...) = remotecall_pool(remotecall_fetch, f, pool, args...; kwargs...)

"""
    default_worker_pool()

WorkerPool containing idle `workers()` (used by `remote(f)`).
"""
_default_worker_pool = Nullable{WorkerPool}()
function default_worker_pool()
    if isnull(_default_worker_pool) && myid() == 1
        set_default_worker_pool(WorkerPool())
    end
    return get(_default_worker_pool)
end

function set_default_worker_pool(p::WorkerPool)
    global _default_worker_pool = Nullable(p)
end


"""
    remote([::AbstractWorkerPool], f) -> Function

Returns a lambda that executes function `f` on an available worker
using `remotecall_fetch`.
"""
remote(f) = (args...; kwargs...)->remotecall_fetch(f, default_worker_pool(), args...; kwargs...)
remote(p::AbstractWorkerPool, f) = (args...; kwargs...)->remotecall_fetch(f, p, args...; kwargs...)

type CachingPool <: AbstractWorkerPool
    channel::RemoteChannel{Channel{Int}}
    workers::Set{Int}

    # Mapping between a tuple (worker_id, f) and a remote_ref
    map_obj2ref::Dict{Tuple{Int, Any}, RemoteChannel}

    broadcasted_symbols::Set{Tuple{Module, Symbol}}   # Store the non-const bindings which can be released.

    function CachingPool()
        wp = new(RemoteChannel(()->Channel{Int}(typemax(Int))), Set{Int}(),
                 Dict{Int, Any}(), Set{Tuple{Module, Symbol}}())

        finalizer(wp, cp_clear)
        wp
    end
end

function CachingPool(workers::Vector{Int})
    pool = CachingPool()
    for w in workers
        push!(pool, w)
    end
    return pool
end

CachingPool(wp::WorkerPool) = CachingPool(workers(wp))

function empty!(pool::CachingPool)
    for (_,rr) in pool.map_obj2ref
        finalize(rr)
    end
    empty!(pool.map_obj2ref)

    results = asyncmap(p->remotecall_fetch(clear_definitions, p, pool.broadcasted_symbols), workers(pool))
    @assert all(results)
    empty!(pool.broadcasted_symbols)
    return pool
end

cp_clear(pool::CachingPool) = (@schedule empty!(pool); pool)

function exec_from_cache(f, rr::RemoteChannel, args...; kwargs...)
    if f === nothing
        if !isready(rr)
            error("Requested function not found in cache")
        else
            fetch(rr)(args...; kwargs...)
        end
    else
        if isready(rr)
            warn("Serialized function found in cache. Removing from cache.")
            take!(rr)
        end
        put!(rr, f)
        f(args...; kwargs...)
    end
end

function remotecall_pool(rc_f, f, pool::CachingPool, args...; kwargs...)
    worker = take!(pool)
    if haskey(pool.map_obj2ref, (worker, f))
        rr = pool.map_obj2ref[(worker, f)]
        remote_f = nothing
    else
        # TODO : Handle map_obj2ref state if `f` is unable to be cached remotely.
        rr = RemoteChannel(worker)
        pool.map_obj2ref[(worker, f)] = rr
        remote_f = f
    end

    try
        rc_f(exec_from_cache, worker, remote_f, rr, args...; kwargs...)
    finally
        worker in pool.workers && put!(pool, worker)
    end
end

function define_locally(isconst::Bool, mod::Symbol, kvpairs)
    mod = getmod(mod)

    for (k,v) in kvpairs
        if isconst
            eval(mod, Expr(:const, Expr(:(=), k, v)))
        else
            eval(mod, Expr(:(=), k, v))
        end
    end
    return true
end

function clear_definitions(names)
    for (mod, nm) in names
        eval(mod, Expr(:(=), nm, nothing))
    end
    return true
end

function remoteset!(pool::CachingPool, isconst::Bool=true, mod=Symbol(:Mod_, randstring()); kwargs...)
    results = asyncmap(p->remotecall_fetch(define_locally, p, isconst, Symbol(mod), kwargs), workers(pool))
    @assert all(results)

    mod = getmod(mod)

    # record the non-const symbols broadcasted for later cleanup
    if !isconst
        for kv in kwargs
            push!(pool.broadcasted_symbols, (mod, kv[1]))
        end
    end

    return mod
end

getmod(mod::Module) = mod
function getmod(mn::Symbol)
    if isdefined(mn)
        return getfield(Main, mn)
    else
        return eval(Main, :(module $mn end))
    end
end


