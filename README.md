# DeeBee ![DeeBee](bee.png)

Embedded database in Go for storing application state.

## Where it can be used?

In all kinds of applications that store their state in RAM and would like to save it to disk - cyclically, on demand or during shutdown. In other words, they would like to save a snapshot of their in-memory data structures to disk and restore them during startup.

## Install

`go get -u github.com/jacekolszak/deebee`

## Quick Start

See [example/json/main.go](example/json/main.go)

## Features:

* atomic write
  * either the state is saved completely or not at all
  * tolerance for killing the app while writing, restarting the machine or loss of power
* integrity verification while reading
  * tolerance for disk problems, buggy drivers or firmware
  * tolerance for accidental file altering
* access to historical data
  * all previous states are available
  * ability to read latest integral file (fail-over to previous version if latest is corrupted)
  * API for deleting historical data - on demand or cyclically
* asynchronous replication
  * ability to copy latest version of state to another file-system (such as NFS)
  * API for reading from multiple replicated stores
* very little use of RAM and CPU
* developer-friendly API
  * small API with just a few functions and small amount of production code
  * no external dependencies
  * extensibility - new data formats can be easily added in a form of custom Codecs
* easy application debugging
  * data is stored on disk as it was saved by the app, so it can be easily read using editor of-choice

## Alternatives

* read/write files using standard `os` package
* use cloud storage services, such as AWS S3

## Project status

MVP almost ready. The API is still changing though. 
