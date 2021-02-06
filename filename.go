package deebee

import (
	"fmt"
	"sort"
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

type filenames []filename

func (f filenames) Len() int {
	return len(f)
}

func (f filenames) Less(i, j int) bool {
	return f[j].version < f[i].version
}

func (f filenames) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func filterDataFiles(files []string) filenames {
	var names []filename
	for _, file := range files {
		f, err := parseFilename(file)
		if err == nil {
			names = append(names, f)
		}
	}
	return names
}

func (f filenames) youngestFilename() (filename, bool) {
	if len(f) == 0 {
		return filename{}, false
	}
	youngest := f[0]
	for _, name := range f {
		if name.youngerThan(youngest) {
			youngest = name
		}
	}
	return youngest, true
}

func sortByVersionDescending(f filenames) {
	sort.Sort(f)
}

func sortByVersionAscending(f filenames) {
	sort.Sort(sort.Reverse(f))
}
