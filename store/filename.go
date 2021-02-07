package store

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type filename struct {
	name    string
	version int
	time    time.Time
}

const timeFormat = time.RFC3339Nano

func generateFilename(version int, now time.Time) string {
	formattedTime := now.Format(timeFormat)
	return fmt.Sprintf("%d-%s", version, formattedTime)
}

func parseFilename(file string) (filename, error) {
	if !strings.Contains(file, "-") {
		return filename{}, errors.New("invalid format")
	}
	array := strings.SplitN(file, "-", 2)
	versionStr, timeStr := array[0], array[1]

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return filename{}, fmt.Errorf("parsing version failed: %w", err)
	}

	creationTime, err := time.Parse(timeFormat, timeStr)
	if err != nil {
		return filename{}, fmt.Errorf("parsing creation time failed: %w", err)
	}

	return filename{
		name:    file,
		version: version,
		time:    creationTime,
	}, nil
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
