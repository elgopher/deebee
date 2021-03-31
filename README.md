# DeeBee ![DeeBee](bee.png)

Embedded database in Go for super frequent updates of large data.

# Install

`go get -u github.com/jacekolszak/deebee`

# Quick Start

See [example/json/main.go](example/json/main.go)

# Project goal

Create a minimalistic database for storing application state. Database should:

* provide functionality of reading and writing (especially updating) application state
* survive application crashes and some disk failures
* consume as little memory as possible
* should be fast for updating large amount of data (from megabytes to gigabytes)
* should be optimized for writes, not reads

# Why and what are the alternatives?

* because very often all you need is to persist some memory structure to disk
* so why not simply use files directly?
    * because writing to file is not an atomic operation
    * because you want your app to be aware of disk failure or that data was not written entirely because of the
      app/system crash and such stored state cannot be used anymore
    * because you want to have multiple historical versions stored, especially for cases described above

# Project status

MVP almost ready. The API is still changing though. 
