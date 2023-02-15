# BLDistributedDataProducts

Distributed access to Breakthrough Listen datasets at the Green Bank Telescope.
Other BL sites are TBD.  Distributed in this context means working across the
BL@GBT cluster.  This package does not provide data distribution services.

## Sessions, scans, products, slots

Datasets are recorded and reduced on multipe nodes during and after observatings
sessions.  The GBT separates the concept of *project ID* (e.g. `AGBT22B_999` and
*session ID* (e.g. `01`), but we consider a session to be a concatentation of
GBT's project ID and session ID (e.g. `AGBT22B_999_01`).  Within a sessions,
each observation is called a `scan`.  GBT assigns monotonically increasing scan
numbers to scans in a session.  These are usually displayed as four digit
integers with leading zeros (e.g. `0001`).  This package returnd scans using a
`NamedTuple` containing four fields:

  * `imjd` - The integer *modified Julian date* (MJD) of the start of the scan
  * `smjd` - The second in the MJD (0-86400)
  * `src` - The name of the *source* being observed
  * `scan` - The four digit GBT scan number (as a `String`)

When passing a `scan` parameter it is allowed to pass a `NamedTuple` with only
some of these fields defined, but it is also allowed and often sufficient to
just pass just an integer scan number (e.g. `23`) rather than a `NamedTuple`.

Each scan can have multiple *products* associated with it.  Essentially a
product is a file.  Each recording node for a given scan has a portion of the
data for that scan.  The data are distributed over the nodes in *bands* (i.e.
racks) containing eight *banks* (i.e. nodes) each.  This package refers to the
`(band, bank)` tuple as a *slot*.  In a GUPPI RAW product, the band is given by
the `BANDNUM` header value; the bank is given by the `BANKNUM` header value.  At
GBT, the band and bank can also be obtained from the name of the *player* (a
logical reference to the recording node) which has the format `BLP<band><bank>`
(e.g. `BLP42`).  The eight banks of a band correspond to a single 1500 MHz wide
IF signal.

Note: the term *bank* is also used at the GBT to refer to a *band*.  Usually the
intended meaning is clear from the context.

## Main process and distributed workers

This package is used in the "main" Julia process (e.g. the Julia REPL or a
Jupyter notebook) which can run on any node.  It uses *worker processes* on the
compute nodes to do most of the work.  The `GBT.setupworkers` function can be
used to start worker processes (referred to as *workers*) on some or, by
default, all of the GBT compute nodes.  The main process communicates with the
worker processes via `ssh`.  The user much have their `ssh` configuration setup
for passphraseless ssh access to the compute nodes from the host running the
main process.

The workers are started using `--project=@BLDistributedDataProducts` so users
are encouraged to setup a shared environment by that name on the GBT cluster and
install this pacakge into that environment.  Until this package (and `Blio`) are
added to the General Julia package registry, this can be setup by following
these steps:

```
$ julia -e '
import Pkg
Pkg.activate("BLDistributedDataProducts", shared=true)
Pkg.add(url="https://github.com/david-macmahon/Blio.jl")
Pkg.add(url="https://github.com/david-macmahon/BLDistributedDataProducts.jl")
'
```

The `GBT.setupworkers` function also takes a `project` keyword argument that
allows one to specify a custom path to an existing `BLDistributedDataProducts`
directory if desired.

## Worker to node mappings

The mapping of workers to nodes (i.e. hosts) is nondeterministic and the mapping
of nodes to bands/banks is somewhat variable as well.  This makes any sort of
heuristical mapping impractical so we instead rely on discovering these mappings
by querying the workers for information about sessions, scans, products, and
slots.

To support this, the workers are provided with functions to look for sessions,
scans, products, and slots on the their local file systems.  These functions
generally fall into one of two types: a generic `get` variant that gets the
results indicated by the function name (e.g.  `getsessions`) and a `my` variant
that returns a `worker_id => results`.  The `my` variants are useful for
tracking results from multiple workers.  The main process has `worker` variants
(e.g. `workersessions`) of these functions that use the `pmap` function to
aggregate the results from calling the `my` variants across all workers.

## Functions

In the following discussion, there is a distinction between *main process
functions*, which the user calls from the main process, and *worker functions*,
which run on the worker processes and are typically only called from within the
main process functions.

Main module functions live in the `BLDistributedDataProducts.GBT` module while
worker functions live in the `BLDistributedDataProducts.GBT.WorkerFunctions`
module.  Typcially users will load this package using something like this:

```julia
using BLDistributedDataProducts

GBT.setupworkers()

# Load "*0002.h5" data from all wsession AGBT22A_999_46
data = GBT.loadscan("AGBT22A_999_46", 3)
```

### Sessions

#### Worker functions

* `getsessions(pattern=SESSIONPATTERN[]; root=DATAROOT[])`
* `mysessions(pattern=SESSIONPATTERN[]; root=DATAROOT[])`

#### Main process function

* `workersessions(pattern=SESSIONPATTERN[]; root=DATAROOT[])`

### Scans

#### Worker functions

* `getscans(session, pattern="*"; root=DATAROOT[])`
* `myscans(session, pattern="*"; root=DATAROOT[])`

#### Main process function

* `workerscans(session, pattern="*"; root=DATAROOT[])`

### Products

#### Worker functions

* `getproducts(session, pattern; root=DATAROOT[])`
* `getproducts(session, scan, suffix; root=DATAROOT[])`
* `myproducts(session, pattern; root=DATAROOT[])`
* `myproducts(session, scan, suffix; root=DATAROOT[])`

#### Main process functions

* `workerproducts(session, pattern; root=DATAROOT[])`
* `workerproducts(session, scan, suffix; root=DATAROOT[])`

### Slots

#### Worker functions

* `getslot(session, scan; root=DATAROOT[])`
* `myslot(session, scan; root=DATAROOT[])`

#### Main process function

* `workerslots(session, scan; root=DATAROOT[])`

### FBH5 attributes

#### Worker functions

* `getfbh5attrs(session, scan, suffix; root=DATAROOT[])`
* `myfbh5attrs(session, scan, suffix; root=DATAROOT[])`

#### Main process function

* `workerfbh5attrs(session, scan, suffix; root=DATAROOT[])`

## High level functions

Several higher level main process functions are available:

### `workerarray(session, scan, suffix="*"; root="/datax/dibas")`

This fucntion returns an Array of workers for the given `session`, `scan`,
`suffix`, and `root` sorted in slot order.  If the number of workers is a
mulitple of 8 and each set of 8 consecutive workers are from the same band, this
will be reshaped as an 8xN Matrix where each column corresponds to a band (i.e.
single IF).

### `workerfbh5data([workers::AbstractArray,] session, scan[, suffix][; root])`

This function maps the given `workers` Array to an Array of data Arrays for the
given `session`, `scan`, `suffix`, and `root`.  If `workers` is not given, this
uses the worker Array returned by `workerarray`.  The `suffix` argument defaults
to `0002.h5`.  The `root` keyword argument defaults to `/datax/dibas`.

### `loadscan(session, scan, suffix="0002.h5"; root="/datax/dibas")`

This function passes the arguments to `workerfbh5data` and then concatenates the
8 Arrays from each band into a single Array per band.  It also removes the so
called *DC spike* by setting each DC spike channel to the same value as a
neighboring channel.