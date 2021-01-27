package deebee

import (
	"fmt"
	"strconv"
)

type filename struct {
	name    string
	version int
}

func generateFilename(version int) string {
	return fmt.Sprintf("%d", version)
}

func parseFilename(file string) (filename, error) {
	version, err := strconv.Atoi(file)
	if err != nil {
		return filename{}, err
	}
	return filename{name: file, version: version}, nil
}

func (f filename) youngerThan(filename filename) bool {
	return f.version > filename.version
}

func toFilenames(files []string) []filename {
	var names []filename
	for _, file := range files {
		f, err := parseFilename(file)
		if err == nil {
			names = append(names, f)
		}
	}
	return names
}

func youngestFilename(names []filename) (filename, bool) {
	if len(names) == 0 {
		return filename{}, false
	}
	youngest := names[0]
	for _, name := range names {
		if name.youngerThan(youngest) {
			youngest = name
		}
	}
	return youngest, true
}
