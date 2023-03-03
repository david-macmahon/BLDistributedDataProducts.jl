module WorkerFunctions

using HDF5, H5Zbitshuffle, Distributed, Blio

export fqav
export getinventory
export getfbheader, getfbh5header, getheader
export getfbdata, getfbh5data, getdata

"""
    fqav(A, n::Integer; f=sum)

Reduce every `n` elements of the first dimension of `A` to a single value using
function `f`.
"""
function fqav(A, n::Integer; f=sum)
    n <= 1 && return A
    sz = (n, :, size(A)[2:end]...)
    dropdims(f(reshape(A,sz), dims=1), dims=1)
end

"""
    fqav(A::AbstractRange, n::Integer)

Return a range whose elements are the mean of every `n` elements of `r`.
"""
function fqav(r::AbstractRange, n::Integer)
    n <= 1 && return A
    fch1 = first(r) + (n-1)*step(r)/2
    foff = n * step(r)
    nchan = length(r) ÷ n
    range(fch1; step=foff, length=nchan)
end

function parseguppiname(name)
    match(r"(/BLP
        (?<band>[0-7])
        (?<bank>[0-7])/)?
        ([^/]*/)?
        ((?<host>blc..)_)?
        guppi_
        (?<imjd>\d+)_
        (?<smjd>\d+)_
        (\d+_)?
        (?<src>.*)_
        (?<scan>\d\d\d\d)"x, name)
end

function parserawspecname(name)
    match(r"/BLP
        (?<band>[0-7])
        (?<bank>[0-7])/
        ((?<host>blc..)_)?
        guppi_
        (?<imjd>\d+)_
        (?<smjd>\d+)_
        (\d+_)?
        (?<src>.*)_
        (?<scan>\d\d\d\d).rawspec.
        (?<product>\d\d\d\d).(h5|fil)$"x, name)
end

InventoryTuple = NamedTuple{
    (:imjd, :smjd, :session, :scan, :src_name, :band, :bank, :host, :file, :worker),
    Tuple{Int64, Int64, String, String, String, Int64, Int64, String, String, Int64}
}

function getinventory(filere::Regex;
    root="/datax/dibas",
    sessionre=r"[AT]GBT[12][0-9][AB]_\d+_\d+",
    extra = "GUPPI",
    playerre=r"^BLP([?<band>0-7])(?<bank>[0-7])$"
)
    host = gethostname()
    worker = myid()
    inventory = InventoryTuple[]

    _, sessions, _ = first(walkdir(root))
    filter!(s->match(sessionre, s)!==nothing, sessions)

    for session in sessions
        _, players, _ = first(walkdir(joinpath.(root, session, extra)))
        filter!(s->match(playerre, s)!==nothing, players)

        for player in players
            for (dir, _, files) in walkdir(joinpath(root, session, extra, player))
                filter!(s->match(filere, s)!==nothing, files)
                for base in files
                    file = joinpath(dir, base)
                    m = parseguppiname(file)
                    if m === nothing
                        @warn "$(host):$(file) did not match guppiname regex"
                        continue
                    end
                    if m[:band] === nothing || m[:bank] === nothing
                        @warn "$(host):$(file) did not match player regex"
                        continue
                    end
                    imjd = parse(Int, m[:imjd])
                    smjd = parse(Int, m[:smjd])
                    scan = string(m[:scan])
                    src_name = string(m[:src])
                    band = parse(Int, m[:band])
                    bank = parse(Int, m[:bank])
                    push!(inventory, (;
                        imjd,
                        smjd,
                        session,
                        scan,
                        src_name,
                        band,
                        bank,
                        host,
                        file,
                        worker
                    ))
                end # files
            end # walk player
        end # players
    end # sessions

    inventory
end

function getfbheader(fbname)
    open(io->read(io, Filterbank.Header), fbname)
end

function getfbh5header(fbh5name)
    h5open(fbh5name) do h5
        data = h5["data"]
        attrs = attributes(data)
        pairs = [Symbol(k) => attrs[k][] for k in keys(attrs)]
        push!(pairs, :ntimes => size(data, ndims(data)))
        NamedTuple(sort(pairs, by=first))
    end
end

function getheader(fname)
    HDF5.ishdf5(fname) ? getfbh5header(fname) : getfbheader(fname)
end

function getfbdata(fbname; ntime::Integer=0, fqavby::Integer=1, fqavfunc=sum)
    data = open(fbname) do io
        d = Array(read(io, Filterbank.Header), ntime)
        read!(io, d)
        d
    end
    fqav(data, fqavby; f=fqavfunc)
end

function getfbh5data(fbh5name; ntime::Integer=0, fqavby::Integer=1, fqavfunc=sum)
    data = h5open(fbh5name) do h5
        if ntime < 1
            h5["data"][]
        else
            h5["data"][:,:,1:ntime]
        end
    end
    fqav(data, fqavby; f=fqavfunc)
end

function getdata(fname; ntime::Integer=0, fqavby::Integer=1, fqavfunc=sum)
    HDF5.ishdf5(fname) ? getfbh5data(fname; ntime, fqavby, fqavfunc) : getfbdata(fname; ntime, fqavby, fqavfunc)
end

end # module WorkersFunctions
