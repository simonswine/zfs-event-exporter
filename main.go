package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/simonswine/zfs-event-exporter/zfs/pool"
	"github.com/simonswine/zfs-event-exporter/zfs/snapshot"
)

var (
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

	listenAddr = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	logLevel   = flag.String("log-level", "info", "log level (debug, info, warn, error, fatal, panic)")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	prometheus.MustRegister(collectors.NewBuildInfoCollector())

	collectorSnapshot, err := snapshot.NewCollector(ctx, logger)
	if err != nil {
		logger.Fatal().Msgf("error creating collector: %v", err)
	}
	prometheus.MustRegister(collectorSnapshot)
	prometheus.MustRegister(pool.NewCollector(logger))

	flag.Parse()

	// setting log level appropriately
	lvl, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		logger.Fatal().Msgf("invalid log level: %v", err)
	}
	logger = logger.Level(lvl)

	g, ctx := errgroup.WithContext(ctx)

	srv := &http.Server{Addr: *listenAddr}
	mux := http.NewServeMux()
	srv.Handler = mux

	// Expose the registered metrics via HTTP.
	mux.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))

	go func() {
		<-ctx.Done()
		logger.Debug().Msg("shutting down http server")
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error().Msgf("error shutting down http server: %v", err)
		}
	}()

	g.Go(srv.ListenAndServe)

	if err := g.Wait(); err != nil {
		logger.Fatal().Msgf("error running: %v", err)
	}
}
