# Data Consistency Models
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

## Strong Consistency
> Informally, the goal of strong consistency is to make a system behave
  like a single sequentially executing node, even when it is replicated and
  concurrent. Most systems implement strong consistency by designating a single
  node as the leader, which decides on a total order of operations and prevents
  concurrent access from causing conflicts [[1]](#1).

This means that when an actor updates some data it is blocked and no other actor
con update it nor client view it until all notes that service requests agree on
and/or are made aware of the new value. Thus network connectivity is required
and scalability is poor. But the guarantees are sometimes very useful.

Zeno uses strong consistency for all data stored at `[:zeno/server ...]` (which
data can only be accessed by the server) as well as `[:zeno/client ...]` (which
is private to the client and never leaves their machine) and might in the
future make strong consistency available to clients via `[:zeno/online ...]`
(name TBD).

## Eventual Consistency
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
both users want their changes to remain (read on about strong eventual
consistency to see how Zeno avoids this limitation).

Due to Zeno's focus on collaborative applications Zeno does not provide any way
to use eventual consistency.

## Strong Eventual Consistency
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

Zeno uses strong eventual consistency for all data stored at `[:zeno/crdt
...]`. We expect this to be the most common place application developers turn
to store data. `[:zeno/sharing ...]` is also backed by the same CRDT data
structures and so can be used offline etc.

# References
<a id="1">[1]</a>
Victor B. F. Gomes, Martin Kleppmann, Dominic P. Mulligan, and Alastair R.
Beresford. 2017. Verifying strong eventual consistency in distributed systems.
<i>Proc. ACM Program. Lang.</i> 1, OOPSLA, Article 109 (October 2017), 28
pages. DOI:https://doi.org/10.1145/3133933
