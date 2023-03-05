# BLDistributedDataProducts

Distributed access to Breakthrough Listen datasets at the Green Bank Telescope.
Other BL sites are TBD.  Distributed in this context means working across the
BL@GBT cluster.  This package does not provide data distribution services.

## Sessions, scans, bands, and banks

Datasets are recorded and reduced on multipe nodes during and after observatings
sessions.  The GBT separates the concept of *project ID* (e.g. `AGBT22B_999` and
*session ID* (e.g. `01`), but we consider a session to be a concatentation of
GBT's project ID and session ID (e.g. `AGBT22B_999_01`).  Within sessions, each
observation is called a `scan`.  GBT assigns monotonically increasing scan
numbers to scans in a session.  These are usually displayed as four digit
strings with leading zeros (e.g. `0001`).

Each recording node for a given scan has a portion of the
data for that scan.  The data are distributed over the nodes in one or more
*bands* (i.e.  racks) containing eight *banks* (i.e. nodes) each.  In a GUPPI
RAW product, the band is given by the `BANDNUM` header value; the bank is given
by the `BANKNUM` header value.  At GBT, the band and bank can also be obtained
from the name of the *player* (a logical reference to the recording node) which
has the format `BLP<band><bank>` (e.g. `BLP42` is band 4, bank 2).  The eight
banks of a band correspond to a single 1500 MHz wide IF signal.

Note: the term *bank* is also often used at the GBT to refer to a *band*.
Usually the intended meaning is clear from the context.

## Main process and distributed workers

This package is used in the *main* Julia process (e.g. the Julia REPL or a
Jupyter notebook) which can run on any node.  It uses *worker processes* on the
compute nodes to do most of the work.  The `GBT.setupworkers` function can be
used to start worker processes (referred to as *workers*) on some or, by
default, all of the GBT compute nodes.  The main process communicates with the
worker processes via `ssh`.  The user must have their `ssh` configuration setup
for passphraseless ssh access to the compute nodes from the host running the
main process.

The workers are started using `--project=@BLDistributedDataProducts` so users
are encouraged to setup a shared environment by that name on the GBT cluster and
install this pacakge into that environment.  Until this package (and `Blio`) are
added to the General Julia package registry, this can be setup by following
these installation steps:

```
$ julia -e '
import Pkg
Pkg.activate("BLDistributedDataProducts", shared=true)
Pkg.add(url="https://github.com/david-macmahon/Blio.jl")
Pkg.add(url="https://github.com/david-macmahon/BLDistributedDataProducts.jl")
'
```

The `GBT.setupworkers` function also takes a `project` keyword argument that
allows one to specify a different project name, if desired.

Each worker has a *process ID* (aka *PID*), but the assignment of PIDs to remote
nodes is non-deterministic and ephemeral (i.e. worker 2 may run on remote node
`blc42` on one run, but on the next run worker 2 may run a different remote
node).  `GBT.setupworkers` returns a Vector of worker PIDs that correspond
one-to-one with the given Vector of hostnames or the default list
`[blc00, blc01, ..., blc77]` if not explixitly specified.  This returned Vector
of worker PIDs should be saved for future use.

## Getting the inventory

In order to interact with remote datasets, we need to learn which datasets exist
on the remote nodes.  This is done using the `GBT.getinventories` function,
which takes a Vector of worker PIDs such as the one returned by
`GBT.setupworkers` and a regular expression to match datasets (defaulting to
`r"0002.h5$"`).  Each remote worker will search for files matching the regular
expression and return a Vector containing one NamedTuple for each file found.
The file search relies on a certain directory hierarchy (some of which can be
altered by keyword arguments).  From this hierarchy and other file naming
conventions, various bits of metadata are extraced.  Each NamedTuple contains
these fields in this order:

- `imjd::Int` - The integer MJD at the start of the recording
- `smjd::Int` - The second in the day at the start of recording
- `session::String` - The ID of of the observing session
- `scan::String` - The scan number within the observing session
- `src_name::String` - The name of the observed source
- `band::Int` - The ID of the band within the IF config (aka "rack")
- `bank::Int` - The ID of the node within the band/rack
- `host::String` - The name of the host that contains the found file
- `file::String` - The full absolute path to the found file
- `worker::Int` - The worker PID that is currently running on `host`

Thus, `GBT.getinventories` returns a `Vector{Vector{NamedTuple}}`, i.e. a Vector
of Vectors of NamedTuples.  The returned Vector contains one element per remote
node.  Each element is a Vector of NamedTuples corresponding to the files found
by the worker on the remote node.

Currently this package does not utilize DataFrames, but using DataFrames makes
it much more convenient to work with the returned inventories.  Created a
DataFrame from the returned Vector can be done like this:

```julia
invs = GBT.getinventories(workerprocs)
inv = DataFrame(Iterators.flatten(invs)) |> sort!
```

The above example also sorts the new DataFrame, which puts all the rows in time
order and then in `(band, bank)` order (aka Player order).  Often a subset of
rows corresponding to a specific `session` and `scan` is desired.  It can be
convenient to group the DataFrame by session and scan like this:

```julia
ssinv = groupby(inv, Cols(:session, :scan))
```

Then all the rows corresponding to a given session and scan can be obtained by
using a `(session, scan)` tuple:

```julia
myscan = ssinv[("AGBT21A_996_25", "0135")]
```

## Using the inventory

Now that we know which files are on which workers/hosts we can read headers and
data from the remote files.

### Reading headers

To read the headers (i.e. metadata), we use `GBT.getheaders`, which takes an
Array of workers and a similarly sized Array of filenames and maps them to an
Array of headers from the files.  Each header is either a `Filterbank.Header`
object for Filterbank files or a `NamedTuple` for HDF5 files.  Vectors of each
type can be used to construct DataFrames, if desired.

```julia
# ssinv is a GroupedDataFrame with grouping columns "session" and "scan" as created above
# myscan is a SubDataFrame with all rows for session AGBT21A_996_25 scan 0135
myscan = ssinv[(session="AGBT21A_996_25", scan="0135")]

# myscan.worker is a Vector of workers
# myscan.file is a Vector of file names
# hdrs is a Vector of headers (exact type depends on Filterbank vs HDF5)
hdrs = GBT.getheaders(myscan.worker, myscan.file)

# hdrdf is a DataFrame of headers whose rows correspond to rows of myscan
hdrdf = DataFrame(hdrs)
```

### Reading data

Reading data from remote files is done using `GBT.getdata`. Like
`GBT.getheaders`, `GBT.getdata` takes an Array of workers and an Array of
filenames.  It also takes an optional Tuple of indices, `idxs`, that can be used
to select a portion of each worker's dataset.  If not specified, all data is
returned.  As a reminder, the data arrays are indexed by
`(channel, polarization/stokes, time)`.  Integers in `idxs` are treated as
ranges of length 1 to ensure that the returned Array always has three
dimensions, even when single values from a dimension are being requested.

```julia
getdata(
    workers::AbstractArray,
    fnames::AbstractArray{<:AbstractString},
    idxs::Tuple=(:,:,:)
    ;
    fqavby::Integer=1,
    fqavfunc=sum
)
```

The keyword arguments can be used reduce the amount of data returned by reducing
every `fqavby` adjacent frequency channels to a single frequency channel via
function `fqavfunc` (defaulting to `sum`).  This is sometimes referred to as
*FreQuency AVeraging* so the related keywords start with `fqav`:

- `fqavby::Integer` - Perform frequency "averaging" by this factor when greater
  than 1.  If greater than 1 then it must be a divisor of the number of channels
  selected by `idxs`.  Every `fqavby` adjacent freqeuncy channles will be
  "averaged" together using the function provided by `fqavfunc`.  The number of
  frequency channels returned will be `nchan ÷ fqavby`.  The default is 1 (i.e.
  no frequency averaging).
- `fqavfunc` - The function to apply to adjacent channels when performing
  frequency "averaging".  The default is `sum`, but other possibilities are
  `mean`, `maximum`, `minimum`, etc.  The function must support the `dims`
  keyword to specify the dimension to work along.

## Utility functions

The GBT package has an `fqav` function that can be used to perform frequency
"averaging" on the first dimension of Arrays or on Range objects such as might
be used for frequency axes.  Note that the "averaging" function for Arrays can
be specified by keyword argument `f`, defaulting to `sum`, but `fqav` for Ranges
always computes the mean.

* `fqav(A, n::Integer; f=sum)`

  Reduce every `n` elements of the first dimension of `A` to a single value
  using function `f`.  If `n` is 1 then `A` is returned.

* `fqav(r::AbstractRange, n::Integer)`

  Return a range whose elements are the mean of every `n` elements of `r`.  If
  `n` is 1 then `r` is returned.
