# Zeno

# About
Zeno is a framework for building connected, online or offline, collaborative
(with conflict resolution), access controlled applications. The offline and
conflict resolution features are supported by first class CRDT data structures
being used under the hood.

***Zeno is in an experimental and rapidly-changing state. Use at your own
risk.***

# Installation
In deps.edn:
```clojure
{:deps {oncurrent/zeno {:git/url "https://github.com/Oncurrent/zeno.git"
                        :sha "xxx"}}}
```

# Zeno Concepts

## Data
Zeno stores all state in a tree. There is one schema for the tree, usually
quite nested. The schema is created and passed to the Zeno server at creation
time using [Lancaster Schemas](https://github.com/deercreeklabs/lancaster).
While there is logically one tree its physical manifestation can be
distributed. For example, some parts of the tree are stored only on the local
client, while others are stored only on the server side, and yet others
continuously synced between the two and even multiple clients (through the
server, not peer to peer). Any path or node in the data tree is private to the
user who created it unless they share it with another user and the share is
accepted.

### Paths
State paths are a sequence of keys that index into the state data structure.
These keys can be keywords, strings, or integers, depending on the specific
state data structure. Keyword keys may or may not have namespaces. A path must
start with one of the build in roots, each of which supplies a different set of
storage and conflict resolution attributes and online/offline behavior:
* `:zeno/client`
  * see [Client](#client)
* `:zeno/crdt`
  * see [CRDT](#crdt)
* `:zeno/online`
  * name TDB
  * not implemented
  * see [Online](#online)
* `:zeno/sharing`
  * for sharing data between users/groups aka access control
  * see [Sharing](#sharing)
* `:zeno/server`
  * for storing data only the server can access
  * useful for e.g. certain types of bookkeeping data
  * assumes server is always online, data is strongly consistent
* other roots could be created for different storage and conflict resolution
  attributes or online/offline attributes/behaviors as
  [described below](#other-types)

Some examples:
* `[:zeno/client :user-id]`
* `[:zeno/client :score-info :high-score]`
* `[:zeno/crdt :users "my-user-id" :user/name]`
* `[:zeno/crdt :msgs 0]`
* `[:zeno/online :users "my-user-id" :fastq]`

#### End-relative Indexing (Sequences only)
For paths referring to sequence data types, the path can use either
front-relative indexing, .e.g.:

* `[:zeno/crdt :msgs 0]` - Refers to the first msg in the list
* `[:zeno/crdt :msgs 1]` - Refers to the second msg in the list

or end-relative indexing, e.g.:

* `[:zeno/crdt :msgs -1]` - Refers to the last msg in the list
* `[:zeno/crdt :msgs -2]` - Refers to the penultimate msg in the list

#### Other Special Paths
TODO
* `[:zeno/keys]` ...
* `[:zeno/actor-id]` ...
* ...

### Types of State

#### Client
Client state is local to the client. This means that this data is not shared
with the server nor any other clients. This data is ephemeral, meaning when
the client session is closed the data is forgotten. If a user logs out and back
in they are now a new client and do not retain any previous client data. The
data is not purged from memory, however, and so we still recommend users close
their browser when they log out for maximum security. This state is used via
the `[:zeno/client ...]` path.

#### CRDT
Aka "Online or Offline Data With Strong Eventual Consistency"

See [Consistency Models](#consistency-models) below for discussion on eventual
consistency vs strong eventual consistency vs strong consistency.

CRDT state is available for reading and writing whether the client is online
or offline. While online, all the data available to the client (controlled via
[sharing](#sharing)), is synced down to the client. Thus while offline one can
only access data that existed the last time they were connected. Any of said
data can be edited while offline and when the client reconnects it is synced
up to the server and any conflicts are merged via CRDT semantics (TODO: more
detail around what "CRDT semantics means for various cases e.g. scalars vs
sequences).

This is the state we intend application developers to use the most often.

This state is used via the `[:zeno/crdt ...]` path.

#### Online
Aka "Online Only Data With Strong Consistency"

See [Consistency Models](#consistency-models) below for discussion on eventual
consistency vs strong eventual consistency vs strong consistency.

Some state makes no sense in any eventually consistent paradigm or is
unreasonable to use in such a way (based on Zeno's implementation, see the
second example). Consider two examples:
1. An e-commerce site selling popular items. When a user sees an item is in
   stock and puts it in their cart the application should be able to guarantee
   the item will be available when they check out (perhaps with a limit on how
   long they have to checkout). Eventual consistency won't due here as it will
   only guarantee that the users will eventually all have the same world view
   undoubtedly leading to customers with false hopes for what is in their cart.
   If I'm offline, the application can't tell the server I added the item to my
   cart and to prevent others from adding it to theirs.
1. An application dealing with genomic sequencing data, typically stored in a
   file format called [FASTQ](https://en.wikipedia.org/wiki/FASTQ_format).
   FASTQ files are frequently several GB's in size. The reason you would want
   to use online only for this data is due to how the CRDT data is continuously
   synced to the local client in order to read data on demand while offline.
   Clearly such syncing only makes sense when the data is not very large. If
   you want to access large data sets on demand it is reasonable to expect the
   user to be online when they do so to avoid eagerly syncing that data down
   and using up all the clients RAM (or more).

This type of state has not been implemented and its name is TBD though we
typically refer to it as `[:zeno/online ...]` in conversation.

#### Other Types
There are several other combinations data storage and conflict resolution
attributes or online/offline behavior one might want. These use cases would be
addressed by other root data paths not yet named or implemented.

Consider, for example, a poll where users are voting on something. Let's say
it's an open poll meaning users can see what votes have already been cast. When
online the user can see what others have voted for as well as cast their own
vote. When offline you will not be able to read new data for the other votes
and you may decide you don't want to show what data you do have to the user to
not mislead them (this might especially be the case if users can change their
votes). After potentially notifying the user that they are offline and thus are
not being shown the current votes, you can still allow the user to cast their
vote for the application to sync up to the server in the background the next
time the device is online. This application would have an online behavior of
read and write but an offline behavior of write only.

### Subscription Maps
Subscription maps are used to specify a subscription to Zeno state. Here is an
example subscription map:
```clojure
{user-id [:zeno/client :user/id] ; TODO [:zeno/actor-id]?
 user-name [:zeno/crdt :users user-id :user/name]
 avatar-url [:zeno/crdt :users user-id :user/avatar-url]}
```
A subscription map's keys are Clojure symbols and the values are
[paths](#paths). The paths are used to index into Zeno state. Zeno then binds
the value of the state at the specified path to the appropriate symbol. For
example, in the subscription map above, the `user-id` symbol will be bound to
the value found in the Zeno state at `[:zeno/client :user/id]`.

Note that symbols may be used in a path. If a symbol is used in a path,
it must be defined by another map entry. For example, in the subscription
map above, the `user-id` symbol is used in both the `user-name`
and `avatar-url` paths.

Order is not important in the map; symbols can be defined in any order.

```clojure
;; TODO: Document what happens with cyclical symbols. I assume this will throw.
{a [:zeno/client b]
 b [:zeno/client a]}
```

### Update Commands
TODO

## Sharing
Aka "Access Control"

TODO

## Async API
In order to work well in browsers, the Zeno API is asynchronous. Most Zeno
functions have three forms:

A simple form: `(update-state! zc update-commands)` - Return value is ignored.

A callback form: `(update-state! zc update-commands cb)` - Return value is
provided by calling the given callback cb.

A channel form: `(<update-state! zc update-commands)` - Returns a core.async
channel, which will yield the function's return value.

# API
TODO

# Development

## Dev Environment
TODO

## Tests
TODO

# Appendix

## Consistency Models
> A data replication algorithm is executed by a set of computers -- or _nodes_
  -- in a distributed system, and ensures that all nodes eventually obtain an
  identical copy of some shared state. Whilst vital for overall systems
  correctness, implementing a replication algorithm is a challenging task, as
  any such algorithm must operate across computer networks that may arbitrarily
  delay, drop, or reorder messages, experience temporary partitions of the
  nodes, or even suffer outright node failure. Reflecting the importance of
  this task, a number of replication algorithms exist, with different
  algorithms exploring the inherent trade-offs between the strength of data
  consistency guarantees, and operational characteristics such as scalability
  and performance. Accordingly, replication algorithms can be divided into
  classes -- strong consistency, eventual consistency, and strong eventual
  consistency -- based on the consistency guarantees that they provide
  [[1]](#1).

### Strong Consistency
> Informally, the goal of strong consistency is to make a system behave
  like a single sequentially executing node, even when it is replicated and
  concurrent. Most systems implement strong consistency by designating a single
  node as the leader, which decides on a total order of operations and prevents
  concurrent access from causing conflicts [[1]](#1).

This means that when an actor updates some data it is blocked and no other actor
con update it nor client view it until all notes that service requests agree on
and/or are made aware of the new value. Thus network connectivity is required
and scalability is poor. But the guarantees are sometimes very useful.

### Eventual Consistency
> [Eventual consistency guarantees] that if no new updates are made to the
  shared state, all nodes will eventually have the same data [Bailis and Ghodsi
  2013; Burckhardt 2014; Terry et al. 1994; Vogels 2009]. Since this model
  allows conflicting updates to be made concurrently, it requires a mechanism
  for resolving such conflicts. For example, version control systems such as
  Git or Mercurial require the user to resolve merge conflicts manually; and
  some "NoSQL" distributed database systems such as Cassandra adopt a
  last-writer-wins policy, under which one update is chosen as the winner, and
  concurrent updates are discarded [Kingsbury 2013]. Eventual consistency
  offers weak guarantees: it does not constrain the system behaviour when
  updates never cease, or the values that read operations may return prior to
  convergence [[1]](#1).

Note the lack of automatic conflict resolution or merging of conflicting
updates, one of the concurrent updates simply has to be picked as the winner
and the others discarded. This is not too useful in collaboritive editing when
both users want their changes to remain.

### Strong Eventual Consistency
> Strong eventual consistency...is a model that strikes a compromise between
  strong and eventual consistency [Shapiro et al. 2011b]. Informally, it
  guarantees that whenever two nodes have received the same set of updates --
  possibly in a different order -- their view of the shared state is identical,
  and any conflicting updates are merged automatically [[1]](#1).

CRDT's implement strong eventual consistency. Since conflicting changes are
merged automatically the order in which they occured becomes a concern. Strong
eventual consistency, and thus CRDT's, require that the order in which the
updates are applied does not matter. In other words the updates must be
commutative. And since we are not picking one as the winner we have to consider
duplicate updates (due to e.g. the network) and so updates must be idempotent.
While from the users perspective commutativity and idempotency don't have to
exist, the underlying data structures and update semantics must obey them.

## References
<a id="1">[1]</a>
Victor B. F. Gomes, Martin Kleppmann, Dominic P. Mulligan, and Alastair R.
Beresford. 2017. Verifying strong eventual consistency in distributed systems.
<i>Proc. ACM Program. Lang.</i> 1, OOPSLA, Article 109 (October 2017), 28
pages. DOI:https://doi.org/10.1145/3133933


# License
Copyright Oncurrent, LLC

*Apache and the Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
