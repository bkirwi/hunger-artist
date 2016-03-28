# [WIP] A Hunger Artist

> So he lived for many years, with small regular intervals of recuperation, in
> visible glory, honored by all the world, yet in spite of that troubled in
> spirit, and all the more troubled because no one would take his trouble
> seriously. What comfort could he posibly need? What more could he possibly
> wish for?
>
> -- Franz Kafka, _A Hunger Artist_

The official Java clients ship with a wonderful set of opinionated, high-level
Kafka client interfaces. You should use them if you can. 

*A Hunger Artist* provides an unopinionated, low-level set of tools for working
with Kafka. It's here for when you need to list out offsets, preserve order on
produce with bounded retries, or solve other problems that don't express
themselves well with the standard tooling.

In particular, this provides:

- A low-level, asynchronous Kafka client, with an API that maps directly to the
  [standard wire protocol](https://kafka.apache.org/protocol.html).
