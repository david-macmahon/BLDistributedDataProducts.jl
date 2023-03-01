module WorkerFunctions

using HDF5, H5Zbitshuffle, Distributed, Glob, Blio

export getinventory
export getheader

export getsessions,     mysessions
export getscans,        myscans
export getproducts,     myproducts
export getslot,         myslot
export getfbheader,     myfbheader
export getslotfbheader, myslotfbheader
export getfbh5header,   myfbh5header
export getfbdata,       myfbdata
export getslotfbdata,   myslotfbdata
export getfbh5data
export mysessionglob
export mysessionattrs

function myplayers(dir)
    myid() => sort(basename.(glob("BLP??", joinpath(dir, "GUPPI"))))
end

function myglob(pattern, dir)
    myid() => sort(glob(pattern, dir))
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

function getheader(fname::AbstractString)
    HDF5.ishdf5(fname) ? getfbh5header(fname) : getfbheader(fname)
end

function getsessions(pattern="[AT]GBT[12][0-9][AB]_*_"; root="/datax/dibas")
    basename.(filter(isdir, glob("*$pattern*", root)))
end

function mysessions(pattern="[AT]GBT[12][0-9][AB]_*_"; root="/datax/dibas")
    myid() => getsessions(pattern; root)
end

function getscans(session, pattern="*"; root="/datax/dibas")
    fnames = glob(joinpath("GUPPI/BLP??", pattern), joinpath(root, session))
    matches = filter(!isnothing, parseguppiname.(fnames))
    [(
        imjd=parse(Int, m[:imjd]),
        smjd=parse(Int, m[:smjd]),
        src=m[:src],
        scan=m[:scan]
    ) for m in matches] |> sort |> unique
end

function myscans(session, pattern="*"; root="/datax/dibas")
    myid() => getscans(session, pattern; root)
end

function getproducts(session, pattern; root="/datax/dibas", extra="GUPPI/BLP??")
    glob(joinpath(session, extra, pattern), root)
end

function myproducts(session, pattern; root="/datax/dibas", extra="GUPPI/BLP??")
    myid() => getproducts(session, pattern; root, extra)
end

function getproducts(session, scan, suffix; root="/datax/dibas", extra="GUPPI/BLP??")
    imjd = get(scan, :imjd, "*")
    smjd = get(scan, :smjd, "*")
    src = get(scan, :src, "*")
    scannum = get(scan, :scan, "*")
    pattern = "*guppi_$(imjd)_$(smjd)_*$src*$scannum*$suffix"
    getproducts(session, pattern; root, extra)
end

function getproducts(session, scan::Integer, suffix; root="/datax/dibas", extra="GUPPI/BLP??")
    getproducts(session, (; scan=string(scan, pad=4)), suffix; root, extra)
end

function myproducts(session, scan, suffix; root="/datax/dibas", extra="GUPPI/BLP??")
    myid() => getproducts(session, scan, suffix; root, extra)
end

function getslot(fname)
    m = match(r"GUPPI/BLP(?<band>[0-7])(?<bank>[0-7])", fname)
    m === nothing && return nothing
    parse.(Int, (m[:band], m[:bank]))
end

function getslot(session, scan, suffix=""; root="/datax/dibas")
    fnames = getproducts(session, scan, suffix; root)
    isempty(fnames) && return nothing
    getslot(fnames[1])
end

function myslot(ssession, scan, suffix=""; root="/datax/dibas")
    myid() => getslot(ssession, scan, suffix; root)
end

function getfbheader(fbfile)
    open(io->read(io, Filterbank.Header), fbfile)
end

function getfbheader(session, scan, suffix; root="/datax/dibas")
    fnames = getproducts(session, scan, suffix; root)
    n = length(fnames)
    n == 1 || error("got $n file names, expected 1")
    getfbheader(fnames[1])
end

function getslotfbheader(session, scan, suffix; root="/datax/dibas")
    slot = getslot(session, scan, suffix; root)
    slot => getfbheader(session, scan, suffix; root)
end

function myfbheader(session, scan, suffix; root="/datax/dibas")
    myid() => getfbheader(session, scan, suffix; root)
end

function myslotfbheader(session, scan, suffix; root="/datax/dibas")
    myid() => getslotfbheader(session, scan, suffix; root)
end

function getfbh5header(fbh5file)
    h5open(fbh5file) do h5
        data = h5["data"]
        attrs = attributes(data)
        pairs = [Symbol(k) => attrs[k][] for k in keys(attrs)]
        push!(pairs, :ntimes => size(data, ndims(data)))
        NamedTuple(sort(pairs, by=first))
    end
end

function getfbh5header(session, scan, suffix; root="/datax/dibas")
    products = getproducts(session, scan, suffix; root)
    n = length(products)
    n == 1 || error("got $n products, expected 1")
    getfbh5header(products[1])
end

function myfbh5header(session, scan, suffix; root="/datax/dibas")
    myid() => getfbh5header(session, scan, suffix; root)
end

function getfbdata(session, scan, suffix; root="/datax/dibas")
    products = getproducts(session, scan, suffix; root)
    n = length(products)
    n == 1 || error("got $n products, expected 1")
    open(products[1]) do io
        data = Array(read(io, Filterbank.Header))
        read!(io, data)
        data
    end
end

function myfbdata(session, scan, suffix; root="/datax/dibas")
    myid() => getfbdata(session, scan, suffix; root)
end

function getslotfbdata(session, scan, suffix; root="/datax/dibas")
    fnames = getproducts(session, scan, suffix; root)
    n = length(fnames)
    n == 1 || error("got $n file names, expected 1")
    slot = getslot(fnames[1])
    slot => open(fnames[1]) do io
        data = Array(read(io, Filterbank.Header))
        read!(io, data)
        data
    end
end

function myslotfbdata(session, scan, suffix; root="/datax/dibas")
    myid() => getslotfbdata(session, scan, suffix; root)
end

function getfbh5data(session, scan, suffix; root="/datax/dibas")
    products = getproducts(session, scan, suffix; root)
    n = length(products)
    n == 1 || error("got $n products, expected 1")
    h5open(products[1]) do h5
        dropdims(h5["data"][], dims=2)
    end
end

function mysessionglob(session, pattern; root="/datax/dibas", extra="GUPPI/BLP??")
    myid() => glob(joinpath(session, extra, pattern), root)
end

function mysessionattrs(session, pattern; root="/datax/dibas", extra="GUPPI/BLP??")
    files = glob(joinpath(session, extra, pattern), root)
    myid() => getfbh5header.(files)
end

end # module WorkersFunctions
