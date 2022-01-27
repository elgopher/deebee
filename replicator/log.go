package replicator

import "github.com/elgopher/yala/logger"

var log logger.Global

func SetLoggerAdapter(adapter logger.Adapter) {
	log.SetAdapter(adapter)
}
