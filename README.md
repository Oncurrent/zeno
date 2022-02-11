# Zeno

# About
Zeno is a framework for building applications that are:
* connected, meaning reactive within and between clients (aka no pressing
  refresh to see updates made by any client)
* online or offline
* collaborative (with conflict resolution)
* access controlled

The offline and conflict resolution features are supported by first class CRDT
data structures being used under the hood.

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
start with one of the built-in roots, each of which supplies a different set of
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

#### Zeno Special Keywords
Zeno provides a number of special paths and keywords to provide convenience and
to accomplish some aggregations. Rereading this section after reading about
[subscription maps](#subscription-maps) may be helpful.

For the below, suppose you store information about books by using a map where
the keys is the
[ISBN](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and
the value is information about the book (book-info) such as title, author, etc.


* `:zeno/keys`
  * If you want a list of ISBN's you could bind the map of books at path
    `[:zeno/crdt :books]` to the variable `books` in your subscription map and
    then on another line call `(keys books)`. `:zeno/keys` is a convenience you
    can put on the end of the data path to avoid the second step. Thus you can
    bind `isbns` straight to the list via `[:zeno/crdt :books :zeno/keys]` and
    avoid the second step of calling `keys`.
  * `:zeno/keys` can only appear at the end of the path.
  * The behavior matches Clojure's `keys`. Thus if you try `[:zeno/crdt :books isbn :author :zeno/keys]`
    you'll get an error as if you tried `(keys "Ernest Hemingway")` though Zeno
    does its own check so you get a more useful error.
* `[:zeno/*]`
  * Used for joins across _all_ nodes at a given level in the state tree. See
    [Joins](#joins) to join across a subset of nodes at a given level.
  * This allows you to get a list of all authors via `[:zeno/crdt :books
    :zeno/* :author]`.
  * It's also useful in specifying sharing paths (see [Sharing](#sharing)).
    If you want to allow a sharing group to see some `:private-field` of every
    book, rather than enumerating the paths to every `:private-field` of every
    book you can just add the path `[:zeno/crdt :books :zeno/* :private-field]`
    to the sharing group.
  * Note the difference between `[:zeno/crdt :books]` which will return you a
    map of ISBN's to book-info's (title, author, etc.) and
    `[:zeno/crdt :books :zeno/*]` which will return you a list of book-info's
    thus dropping the ISBN info (unless you have redundantly stored ISBN in
    each book-info).
  * `:zeno/*` can appear anywhere in the path.
  * If the value at a path is a scalar and you put `:zeno/*` after it you'll
    get a useful error from Zeno.
* `:zeno/count`
  * Used for count aggregations.
  * To get the count of any collection that may exist at some path simply add
    `:zeno/count` to the end. For example, to count how many books you have
    you could use `[:zeno/crdt :books :zeno/count]`.
  * `:zeno/count` can only appear at the end of the path.
  * It doesn't matter if the collection is associative or not, the behavior
    matches Clojure's `count`. This also means that `[:zeno/crdt :books isbn
    :page-count :zeno/count]` results in an error as if you tried `(count 42)`
    though Zeno does its own check so you get a more useful error.
* `:zeno/actor-id`
  * Upon authentication Zeno provides an `actor-id` that uniquely identifies
    the entity just authenticated. In your application that may or may not
    align with the traditional notion of a user id.
  * You can access the currently authenticated entities actor id via the path
    `[:zeno/actor-id]` at any time.
  * Additionally you can use `:zeno/actor-id` anywhere in a path and it will be
    replaced with the value for you under the hood.
* `:zeno/concat`
  * To understand `:zeno/concat` consider the following:
    * Suppose that you also store scientific papers in your application. While
      books frequently have one author scientific papers frequently have multiple
      authors and so you decide to store a list of authors rather than a single
      author.
    * You could access the list of authors of a single paper whose ID you have
      already stored in the variable `paper-id` via `[:zeno/crdt :papers paper-id
      :authors]`.
    * If you want a list of all the authors for all papers you can use
      `[:zeno/crdt :papers :zeno/* :authors]` but this returns a nested list. You
      can conveniently concat them all into a flat list via `[:zeno/crdt :papers
      :zeno/* :authors :zeno/concat]`
  * `:zeno/concat` can appear anywhere in the path.
  * The behavior matches Clojure's `concat`. It's up to you to ensure you don't
    use `:zeno/concat` on an invalid type e.g. and integer. Referring to our
    books example again, `[:zeno/crdt :books isbn :author :zeno/concat]` will
    throw an error just like `(concat "Ernest Hemingway")` does though Zeno
    does its own check so you get a more useful error.

##### Quick reference:
```clojure
[:zeno/crdt :books :zeno/keys] ; => list of ISBN's
[:zeno/crdt :books :zeno/count] ; => 42
[:zeno/crdt :books] ; => map of all books
[:zeno/crdt :books :zeno/*] ; => list of all book-info's
[:zeno/crdt :books :zeno/* :author] ; => list of authors (including duplicates)
[:zeno/crdt :books :zeno/* :private-field] ; => path to allow access to all :private-field's
[:zeno/crdt :papers :zeno/* :authors :zeno/concat] ; => flat list of all authors of all papers
```

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

See [Data Consistency Models](./data-consistency-models.md) for discussion on strong
consistency vs eventual consistency vs strong eventual consistency.

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

See [Data Consistency Models](./data-consistency-models.md) for discussion on strong
consistency vs eventual consistency vs strong eventual consistency.

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
{isbn [:zeno/client :current-book]
 author [:zeno/crdt :books isbn :author]
 avatar-url [:zeno/crdt :avatars author :avatar-url]}
```
A subscription map's keys are Clojure symbols and the values are
[paths](#paths). The paths are used to index into Zeno state. Zeno then binds
the value of the state at the specified path to the appropriate symbol. For
example, in the subscription map above, the `isbn` symbol will be bound to
the value found in the Zeno state at `[:zeno/client :current-book]`.

Note that symbols may be used in a path. If a symbol is used in a path, it must
be defined by another map entry or otherwise be available in the current scope.
For example, in the subscription map above, the `author` symbol is used in the
`avatar-url` path. The `author` symbol might have also come in as a parameter
to the `def-component` that contains the subscription map or otherwise be a
global variable.

Order is not important in the map; symbols can be defined in any order. Cycles
will be detected and an error thrown.
```clojure
;; Order doesn't matter, this
{a [:zeno/client :a]
 b [:zeno/client a :b]}
;; works just as well this
{b [:zeno/client a :b]
 a [:zeno/client :a]}

;; Cyclical binding not allowed
{a [:zeno/client b]
 b [:zeno/client a]}
```

#### Joins
As noted in [Paths](#paths) the keys in a path can be keywords, strings, or
integers, depending on the specific state data structure. You can also use a
symbol bound to a list in a path to accomplish a join. Consider the same
`:books` case as found in the [Zeno Special Keywords](#zeno-special-keywords)
section. Suppose you wanted to get the authors of a subset of books rather than
all of them. As discussed in the aforementioned section, `zeno/*` allows you to
join across all books. To accomplish our subset join we can use `[:zeno/crdt
:books isbns :author]` where `isbns` is a symbol previously bound to a list of
ISBN's.

### Update Commands
An update command is a map with three keys:
* `:path`: The [path](#paths) on which the update command will operate; e.g.
  `[:zeno/crdt :books isbn :author]`
* `:op`: One of the [supported update
  operations](#supported-update-operations); e.g. `:set`
* `:arg`: The command's argument; e.g. `"Ernest Hemingway"`
For example:
```clojure
;; Here zc is a preconfigured/created Zeno Client.
(zeno/update-state! zc {:path [:zeno/crdt :books isbn :author]
                        :op :set
                        :arg "Ernest Hemingway})
```
See [zeno-client](#zeno-client) for more details about `zc`.

#### Supported Update Operations
* `:set`
  * TODO
* `:remove`
  * TODO
* `:insert-before`
  * TODO
* `insert-after`
  * TODO
* `insert-range-before`
  * TODO
* `insert-range-after`
  * TODO
* TODO

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

# License
Copyright Oncurrent, Inc.

*Apache and the Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
