module GBT

using Distributed

include("gbtworkerfunctions.jl")
using .WorkerFunctions

function datahosts(prefix="")
    ["$(prefix)blc$i$j" for j=0:7 for i=0:7]
end

function setupworkers(hosts::AbstractVector=[];
    tunnel=true,
    dir="/tmp",
    exename="julia",
    prefix="",
    project="@BLDistributedDataProducts",
    kwargs...
)
    if nworkers() > 1 || workers()[1] != 1
        @warn "workers already added, not adding more"
    else
        if isempty(hosts)
            hosts = datahosts(prefix)
        end

        workerprocs = addprocs(hosts;
            max_parallel=length(hosts),
            exeflags="--project=$project",
            tunnel,
            dir,
            exename,
            kwargs...
        )

        Distributed.remotecall_eval(Main, workerprocs, :(
            using BLDistributedDataProducts.GBT.WorkerFunctions
        ))
    end
    nothing
end

#
# main process functions that aggregate results from workers
#

function assertworkers()
    @assert nworkers() > 1 || workers()[1] != 1 "no remote workers setup"
end

function workerplayers(dir)
    assertworkers()
    pmap(_->myplayers(dir), 1:nworkers())
end

function workersessions(pattern="[AT]GBT[12][0-9][AB]_*_"; root="/datax/dibas")
    assertworkers()
    wss = pmap(_->mysessions(pattern; root), 1:nworkers())
    filter(!isempty∘last, wss)
end

function workerscans(session, pattern="*"; root="/datax/dibas")
    assertworkers()
    wss = pmap(_->myscans(session, pattern; root), 1:nworkers())
    filter(!isempty∘last, wss)
end

function workerproducts(session, pattern::AbstractString; root="/datax/dibas")
    assertworkers()
    wps = pmap(_->myproducts(session, pattern; root), 1:nworkers())
    filter(!isempty∘last, wps)
end

function workerproducts(session, scan, suffix="*"; root="/datax/dibas")
    assertworkers()
    wps = pmap(_->myproducts(session, scan, suffix; root), 1:nworkers())
    filter(!isempty∘last, wps)
end

function workerslots(session, scan, suffix=""; root="/datax/dibas", by=identity)
    assertworkers()
    wss = pmap(_->myslot(session, scan, suffix; root), 1:nworkers())
    sort(filter(!isnothing∘last, wss); by)
end

function workerfbheaders(session, scan, suffix; root="/datax/dibas", by=identity)
    assertworkers()
    sort(pmap(_->myfbheader(session, scan, suffix; root), 1:nworkers()); by)
end

function slotfbheaders(session, scan, suffix; root="/datax/dibas")
    assertworkers()
    sort(pmap(_->getslotfbheader(session, scan, suffix; root), 1:nworkers()); by=first)
end

function slotfbdata(session, scan, suffix; root="/datax/dibas")
    assertworkers()
    sort(pmap(_->getslotfbdata(session, scan, suffix; root), 1:nworkers()); by=first)
end

function workerfbh5header(session, scan, suffix; root="/datax/dibas")
    assertworkers()
    pmap(_->myfbh5header(session, scan, suffix; root), 1:nworkers())
end

function fqav(A, n; f=sum)
    sz = (n, :, size(A)[2:end]...)
    dropdims(f(reshape(A,sz), dims=1), dims=1)
end

"""
    workerarray(session, scan, suffix="*"; root="/datax/dibas")

Returns an Array of workers for the given `session`, `scan`, `suffix`, and
`root` sorted in slot order.  If the number of workers is a mulitple of 8 and
each set of 8 consecutive workers are from the same band, this will be reshaped
as as 8xN Matrix.
"""
function workerarray(session, scan, suffix="*"; root="/datax/dibas")
    ws = workerslots(session, scan, suffix; root, by=last)
    if length(ws) % 8 == 0
        ws8 = reshape(ws, length(ws) > 8 ? (8,:) : 8)
        bands = (first∘last).(ws8)
        # If each column comes from a single band
        if all(bands .== bands[1:1, :])
            w = first.(ws8)
        else
            w = first.(ws)
        end
    else
        w = first.(ws)
    end
    w
end

"""
    workerfbh5data([workers::AbstractArray,] session, scan[, suffix][; root])

Maps the `workers` Array to an Array of data Arrays for the given `session`,
`scan`, `suffix`, and `root`.  If `workers` is not given, this uses the worker
Array returned by `workerarray`.  The `suffix` argument defaults to `0002.h5`.
The `root` keyword argument defaults to `/datax/dibas`.
"""
function workerfbh5data(workers::AbstractArray, session, scan, suffix="0002.h5"; root="/datax/dibas")
    futures = map(workers) do worker
        @spawnat worker getfbh5data(session, scan, suffix; root)
    end
    fetch.(futures)
end

function workerfbh5data(session, scan, suffix="0002.h5"; root="/datax/dibas")
    workers = workerarray(session, scan, suffix; root)
    workerfbh5data(workers, session, scan, suffix; root)
end

"""
    loadscan(session, scan, suffix="0002.h5"; root="/datax/dibas")

Passes the arguments to `workerfbh5data` and then concatenates the 8 Arrays from
each band into a single Array per band.  It also removes the so called *DC
spike* by setting each DC spike channel to the same value as a neighboring
channel.
"""
function loadscan(session, scan, suffix="0002.h5"; root="/datax/dibas")
    ds = workerfbh5data(session, scan, suffix; root)
    nfpc = size(ds[1], 1) ÷ 64 # 64 coarse channels per node at GBT
    spike = nfpc÷2 + 1
    ds1 = map(c->reduce(vcat, c), eachcol(ds))
    #=
    ks1 = map(d->kurtosis.(eachrow(d)), ds1)
    dsc = map(d->reshape(d, 1024, :, size(d,2)), ds1)
    ksc = map(k->reshape(k, 1024, :           ), ks1)
    foreach(d->d[513:1024:end,:,:].=d[512:1024:end,:,:], dsc) # De-spike!
    dsc, ksc
    =#
    foreach(d->d[spike:nfpc:end,:,:].=d[spike-1:nfpc:end,:,:], ds1) # De-spike!
    ds1
end

end # module GBTDistributedData