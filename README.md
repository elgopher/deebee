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

* because very often all you need is to persist some **in-memory** data structure to disk
* so why not simply use `os` package and write files directly ?
    * because writing to a file is not an atomic operation - there are multiple steps involved - and most of the time you either want to write an entire file or no file at all
    * because there might be a problem with disk failure, or your app/operating system may crash during writing
    * because you want to update file many times, and you want to have access to its historical versions

# Project status

MVP almost ready. The API is still changing though. 
