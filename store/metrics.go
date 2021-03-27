// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

type Metrics struct {
	Versions          VersionMetrics
	AvailableVersions VersionMetrics

	ReaderRequests   RequestMetrics
	WriterRequests   RequestMetrics
	VersionsRequests RequestMetrics
	DeleteRequests   RequestMetrics
	ReadTotal        RequestMetrics // Total time spend from opening the Reader to closing it
	WriteTotal       RequestMetrics // Total time spend from opening the Writer to closing it
}

type VersionMetrics struct {
	Count     int
	TotalSize int
}

type RequestMetrics struct {
	Count       int
	ErrorCount  int
	TotalMillis int
}
