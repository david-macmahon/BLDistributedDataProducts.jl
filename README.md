# BLDistributedDataProducts

Distributed access to Breakthrough Listen datasets at the Green Bank Telescope.
Other BL sites are TBD.  Distributed in this context means working across the
BL@GBT cluster.  This package does not provide data distribution services.

## Sessions, scans, products, slots

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
has the format `BLP<band><bank>` (e.g. `BLP42`).  The eight banks of a band
correspond to a single 1500 MHz wide IF signal.

Note: the term *bank* is also often used at the GBT to refer to a *band*.
Usually the intended meaning is clear from the context.

## Main process and distributed workers

This package is used in the "main" Julia process (e.g. the Julia REPL or a
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
on the remote nodes and the nodes' correspnding workers.  This is done using the
`GBT.getinventories` function, which takes a Vector of worker PIDs such as the
one returned by `GBT.setupworkers` and a regular expression to match datasets
(defaulting to `r"0002.h5$"`).  Each remote worker will search for files
matching the regular expression and return a Vector containing one NamedTuple
for each file found.  The file search relies on a certain directory hierarchy
(some of which can be altered by keyword arguments).  From this hierarchy and
other file naming conventions, various bits of metadata are extraced.  Each
NamedTuple contains these fields in this order:

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
inventories = GBT.getinventories(workerprocs)
dfinventory = DataFrame(Iterators.flatten(inventories)) |> sort!
```

The above example also sorts the new DataFrame, which puts all the rows in time
order and then in `(band, bank)` order (aka Player order).  Often a subset of
rows correspinding to a specific `session` and `scan` is desired.  It can be
convenient to group the DataFrame by session and scan like this:

```julia
gdfinventory = groupby(dfinentory, Cols(:session, :scan))
```

Then all the rows corresponding to a given session and scan can be obtained via:

```julia
gdfinventory[(session="AGBT21A_996_25", scan="0135")]
```

## Functions

In the following discussion, there is a distinction between *main process
functions*, which the user calls from the main process, and *worker functions*,
which run on the worker processes and are typically only called from within the
main process functions.

Main process functions live in the `BLDistributedDataProducts.GBT` module while
worker functions live in the `BLDistributedDataProducts.GBT.WorkerFunctions`
module.  Typcially users will use this package similar to this:

```julia
using BLDistributedDataProducts, DataFrames

workerprocs = GBT.setupworkers()
inventories = GBT.getinventories(workerprocs)
gdfinventory = groupby(sort!(DataFrame(Iterators.flatten(inventories))))
```

### Below here is TODO!!!

### FBH5 attributes

#### Worker functions

* `getfbh5attrs(session, scan, suffix; root=DATAROOT[])`
* `myfbh5attrs(session, scan, suffix; root=DATAROOT[])`

#### Main process function

* `workerfbh5attrs(session, scan, suffix; root=DATAROOT[])`

## High level functions

Several higher level main process functions are available:

`workerarray(session, scan, suffix="*"; root="/datax/dibas")`

`workerarray` returns an Array of workers for the given `session`, `scan`,
`suffix`, and `root` sorted in slot order.

If the number of workers is a mulitple of 8 and each set of 8 consecutive
workers are from the same band, this will be reshaped as an 8xN Matrix where
each column corresponds to a band (i.e.  single IF).

`workerfbh5data([workers::AbstractArray,] session, scan[, suffix][; root])`

`workerfbh5data` maps the given `workers` Array to an Array of FBH5 data Arrays
for the given `session`, `scan`, `suffix`, and `root`.

If `workers` is not given, this uses the worker Array returned by `workerarray`.
The `suffix` argument defaults to `0002.h5`.  The `root` keyword argument
defaults to `/datax/dibas`.

`loadscan(session, scan, suffix="0002.h5"; root="/datax/dibas")`

`loadscan` passes the arguments to `workerfbh5data` and then concatenates the 8
Arrays from each band into a single Array per band.

It also removes the so called *DC spike* by setting each DC spike channel to the
same value as a neighboring channel.