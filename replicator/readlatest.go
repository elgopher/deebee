package replicator

import (
	"errors"

	"github.com/jacekolszak/deebee/codec"
	"github.com/jacekolszak/deebee/store"
)

// ReadLatest reads latest version from replicated stores.
//
// If latest version cannot be loaded from the first store it tries to load it from the next one.
func ReadLatest(decoder codec.Decoder, stores ...codec.ReadOnlyStore) (store.Version, error) {
	if decoder == nil {
		return store.Version{}, errors.New("nil decoder")
	}
	if len(stores) == 0 {
		return store.Version{}, errors.New("no stores given")
	}
	for _, s := range stores {
		if s == nil {
			return store.Version{}, errors.New("nil store")
		}
	}
	versions := listStoreVersions(stores)
	for versions.hasMore() {
		storeIndex, version := versions.removeLatestVersion()
		s := stores[storeIndex]
		_, err := codec.Read(s, decoder, store.Time(version.Time))
		if err == nil {
			return version, nil
		}
	}
	return store.Version{}, store.NewVersionNotFoundError("no version can be decoded")
}

type storeVersions [][]store.Version

func listStoreVersions(stores []codec.ReadOnlyStore) storeVersions {
	versions := make(storeVersions, len(stores))
	for i, s := range stores {
		v, err := s.Versions()
		if err != nil {
			v = nil
		}
		versions[i] = v
	}
	return versions
}

const indexNotSet = -1

func (v storeVersions) removeLatestVersion() (int, store.Version) {
	latest := struct {
		storeIndex int
		version    store.Version
	}{
		storeIndex: indexNotSet,
	}

	for i := 0; i < len(v); i++ {
		versions := v[i]
		if len(versions) == 0 {
			continue
		}

		lastVersion := versions[len(versions)-1]
		if latest.storeIndex == indexNotSet || lastVersion.Time.After(latest.version.Time) {
			latest.version = lastVersion
			latest.storeIndex = i
		}
	}

	if latest.storeIndex != indexNotSet {
		v[latest.storeIndex] = removeLast(v[latest.storeIndex])
	}

	return latest.storeIndex, latest.version
}

func removeLast(versions []store.Version) []store.Version {
	return versions[0 : len(versions)-1]
}

func (v storeVersions) hasMore() bool {
	for _, versions := range v {
		if len(versions) > 0 {
			return true
		}
	}
	return false
}
