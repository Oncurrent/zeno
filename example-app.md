# Example App

The purpose of this description is to demonstrate some aspects of Zeno that are
hard to understand otherwise. It's a supplement to the [README](./README.md)
and won't make sense if you haven't read the README first, though at the same
time some aspects of the README might not be fully clear until you read this.
Various parts can be expanded in their description as is useful. Perhaps
expanding it into an actual example application one day would be useful.

Consider an application where users can create a login, full details not
covered. Once logged in there is only one page with three fields. One field
allows them to enter their favorite number (an integer), another the answer to
"life, the universe, and everything" (an integer), and finally a free form text
area. Each field exists to demonstrate some aspect of Zeno.

Everything is stored with `:zeno/crdt` as the root path (since that's the only
non-local root we have right now as `:zeno/online` hasn't been implemented).
The schema is as follows:
```clojure
(l/def-record-schema crdt-schema
  [:user-nums (l/map-schema l/int-schema)
   :the-answer l/int-schema
   :text (l/array-schema l/int-schema)])
```

We'll explain each in turn. First, `:user-nums`, when you authenticate with
Zeno it assigns you an id which we'll use as the user id for our app. These
details are not addressed at this time. Let's say we have an authenticated
user and thus we can get the user id via the special `:zeno/actor-id` keyword
used in a path in a subscription map.

There's really nothing interesting here, each user has their own favorite
number and can set it and view it again later.

Suppose the application UI allowed the user to show their favorite number to
others. This would be accomplished with a Zeno sharing group. You could
organize the sharing groups a few ways. For example you could create sharing
groups with one user and the many favorite number paths they can see or you
could create groups with one favorite number path and the many users who can
see it. Or something else. That's all up to the you the application developer.
Our purpose here is to describe what happens when a path is indeed shared via a
sharing group, whatever form it takes.

In this case it's simple, a given user is in one or many sharing groups that
give them read access to one or many other users favorite numbers and so the
UI can display a list of them. In this case the application doesn't allow users
to grant write access to any favorite numbers besides their own.

Now consider the answer to "life, the universe, and everything". This data is
not stored under user-id's but is instead top level at `[:zeno/crdt
:the-answer]`. This is where things start to get interesting. Let's start by
assuming we have no sharing groups that include `[:zeno/crdt :the-answer]` in
them. One of the primary reasons to choose Zeno, and especially the
`:zeno/crdt` root is to support offline editing with automatic conflict
resolution. The goal with explaining how the field `:the-answer` works is to
expose some of the non-intuitive consequences of this.

Let's say Bob and Sally both edit `:the-answer` field. Bob sets it to 42 and
Sally sets it to 21. At some point, whether right when they made the edit or
later, Bob and Sally are online and their changes are sent to the server for
persistence. There is no conflict here despite them both writing to the same
path. Each user has their own manifestation the crdt data tree continuously
syncing (if they are online) between their client and the server. Bob and Sally
can change `:the-answer` all they want without ever knowing the other is doing
anything. (Insert aside about doing this to enable creating things offline
without any bootstrapping write access problem etc.) This is the case until one
day Bob uses the UI to invite Sally to have read/write access to his
`:the-answer`. Sally acceps the invitation and now there is a conflict. All the
data in `:zeno/crdt` is stored in CRDT's and so Bob and Sally's can be merged.
Since integers are scalars one of them has to win out at the end of the day and
so the latest one wins, not a very interesting merge. Now Bob and Sally's share
the part of their tree that is `[:zeno/crdt :the-answer]` but the rest of their
trees continue to be independent. When Bob changes `:the-answer` it immediately
(assuming network connectivity) reflects on Sally's screen and vice versa. If
Sally is offline when Bob makes the change she won't see it but can still make
her own change. When she comes back online the server will merge the two, in
this case picking the latest. (Insert aside about how the latest is only "best
effort" (or weak effort) but is not needed for correctness just consistency.)

At this point we have two users Bob and Sally. Each have entered their own
favorite number but not shared it and Bob has invited Sally to share read/write
on `:the-answer`. At this point if Sally types in the free form text area Bob
will not see it, just like he didn't see her changes to `:the-answer` before
the share was established. The `:text` field is intended to demonstrate a more
interesting CRDT merge. We have chosen not to represent the data as a string
but rather as a list of integers. This is because a string is a scalar and you
cannot do a useful merge of a scalar but rather have to pick one or the other.
The application takes each character and converts it to it's ASCII number and
then puts all the numbers in an array. This process can be done both ways. Now
that our text is a sequence rather than a scalar we can merge. Say Bob enters
`"Bob is cool"` and Sally enters `"I like pizza"`. Neither are aware of the
other since there is no share. Now a read/write share is established. The
sequence of integers is merged and when the ASCII is intepreted the field will
now read `"Bob is coolI like pizza"` for both Bob and Sally (the order was
picked by the same latest schema as before). Now say that Bob and Sally both
get on an airplane and are too cheap to pay for wifi and so are editing
offline. Bob deletes the entire text and writes `"Hello World"`. Nothing
changes for Sally since they are offline. Sally deletes the entire text and
writes `"Hello, World!"`. The result when they both come online is `"Hello
WorldHello, World!"`. While it would be awesome if the result was `"Hello,
World!"` such a merge is not actually generalizable and is only correct due to
our context. CRDT's don't know what merge is correct in the human context they
do provide the mathematical foundation for consistent and lossless merges in
the face of faulty networks which might reorder or duplicate messages.
