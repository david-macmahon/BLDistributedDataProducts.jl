module GBT

using Distributed

include("gbtworkerfunctions.jl")
using .WorkerFunctions

function datahosts(prefix="")
    ["$(prefix)blc$i$j" for i=0:7 for j=0:7]
end

function setupworkers(hosts::AbstractVector=[];
    tunnel=true,
    dir="/tmp",
    exename="julia",
    prefix="",
    project="@BLDistributedDataProducts",
    kwargs...
)
    if nprocs() > 1
        @warn "workers already added, not adding more"
        return Int[]
    else
        if isempty(hosts)
            hosts = datahosts(prefix)
        end

        workerprocs = addprocs(hosts;
            exeflags="--project=$project",
            tunnel,
            dir,
            exename,
            kwargs...
        )

        # Do one first to avoid potential precompilation collision
        Distributed.remotecall_eval(Main, workerprocs[1:1], :(
            using BLDistributedDataProducts.GBT.WorkerFunctions
        ))
        Distributed.remotecall_eval(Main, workerprocs[2:end], :(
            using BLDistributedDataProducts.GBT.WorkerFunctions
        ))
    end

    workerprocs
end

function getinventories(workers::AbstractArray, filere::Regex=r"0002.h5$";
    root="/datax/dibas",
    sessionre=r"[AT]GBT[12][0-9][AB]_\d+_\d+",
    extra = "GUPPI",
    playerre=r"^BLP([?<band>0-7])(?<bank>[0-7])$"
)
    futures = map(workers) do worker
        @spawnat worker getinventory(filere; root, sessionre, extra, playerre)
    end
    fetch.(futures)
end

function getheaders(workers::AbstractArray, fnames::AbstractArray{<:AbstractString})
    @assert size(workers) == size(fnames) "workers and fnames must have the same size"

    futures = map(zip(workers, fnames)) do (worker, fname)
        @spawnat worker getheader(fname)
    end
    fetch.(futures)
end

function getdata(
    workers::AbstractArray, fnames::AbstractArray{<:AbstractString}, idxs::Tuple=(:,:,:);
    fqavby::Integer=1, fqavfunc=sum
)
    @assert size(workers) == size(fnames) "workers and fnames must have the same size"

    futures = map(zip(workers, fnames)) do (worker, fname)
        @spawnat worker WorkerFunctions.getdata(fname, idxs; fqavby, fqavfunc)
    end
    fetch.(futures)
end

function getkurtosis(workers::AbstractArray, fnames::AbstractArray{<:AbstractString}, idxs::Tuple=(:,:,:))
    @assert size(workers) == size(fnames) "workers and fnames must have the same size"

    futures = map(zip(workers, fnames)) do (worker, fname)
        @spawnat worker WorkerFunctions.getkurtosis(fname, idxs)
    end
    fetch.(futures)
end

#=
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
=#

end # module GBTDistributedData
