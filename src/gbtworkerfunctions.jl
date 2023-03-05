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

"""
    sanitizeidxs(idxs::Tuple)

Return a Tuple that is the same as `idxs` except with Integers replaced by
ranges of length 1.
"""
function sanitizeidxs(idxs::Tuple)::Tuple
    Tuple(i isa Integer ? (i:i) : i for i in idxs)
end

function getfbdata(fbname, idxs::Tuple=(:,:,:); fqavby::Integer=1, fqavfunc=sum)
    @assert length(idxs) == 3 "idxs must have exactly three indices"
    _, dmmap = Filterbank.mmap(fbname)
    data = fqav(dmmap[idxs...], fqavby; f=fqavfunc)
    finalize(parent(dmmap))
    data
end

function getfbh5data(fbh5name, idxs::Tuple=(:,:,:); fqavby::Integer=1, fqavfunc=sum)
    @assert length(idxs) == 3 "idxs must have exactly three indices"
    data = h5open(fbh5name) do h5
        if idxs === (:,:,:)
            h5["data"][]
        else
            h5["data"][idxs...]
        end
    end
    fqav(data, fqavby; f=fqavfunc)
end

function getdata(fname, idxs::Tuple=(:,:,:); fqavby::Integer=1, fqavfunc=sum)
    idxs = sanitizeidxs(idxs)
    HDF5.ishdf5(fname) ? getfbh5data(fname, idxs; fqavby, fqavfunc) :
                         getfbdata(fname, idxs; fqavby, fqavfunc)
end

end # module WorkersFunctions
