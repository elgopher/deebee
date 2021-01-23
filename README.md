# deebee

Embedded database for persisting application state

# Motiviation

Create a minimalistic database for storing application state. Database should:

* provide functionality of reading and writing application state
* survive application crashes and some disk failures
* consume as little memory as possible
* should be fast for storing large amount of data (from megabytes to gigabytes)
* should be optimized for writes, not reads
