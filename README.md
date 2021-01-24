# DeeBee

Embedded database in Go for large data, update-heavy applications.

# Project goal

Create a minimalistic database for storing application state. Database should:

* provide functionality of reading and writing (especially updating) application state
* survive application crashes and some disk failures
* consume as little memory as possible
* should be fast for updating large amount of data (from megabytes to gigabytes)
* should be optimized for writes, not reads

# Project status

Undery heavy development :)
