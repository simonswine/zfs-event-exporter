package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/simonswine/zfs-event-exporter/zfs/pool"
	"github.com/simonswine/zfs-event-exporter/zfs/snapshot"
)

var (
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
)

type httpBuffer struct {
	b          bytes.Buffer
	h          hash.Hash
	tee        io.Writer
	statusCode int
	headers    http.Header
}

func newHTTPBuffer() *httpBuffer {
	b := &httpBuffer{
		headers:    make(http.Header),
		h:          sha256.New(),
		statusCode: 200,
	}
	b.tee = io.MultiWriter(&b.b, b.h)
	return b
}

func (b *httpBuffer) Header() http.Header {
	return b.headers
}

func (b *httpBuffer) WriteHeader(statusCode int) {
	b.statusCode = statusCode
}

func (b *httpBuffer) Write(p []byte) (int, error) {
	return b.tee.Write(p)
}

func (b *httpBuffer) Read(p []byte) (int, error) {
	return b.b.Read(p)
}
func (b *httpBuffer) Sum() string {
	return string(b.h.Sum(nil))
}

func (b *httpBuffer) Reset() {
	b.b.Reset()
	b.h.Reset()
	b.statusCode = 200
	for k := range b.headers {
		delete(b.headers, k)
	}
}

func runTextFileOutput(ctx context.Context, handler http.Handler) (func(), error) {
	var (
		ticker  = time.NewTicker(15 * time.Second)
		buffer  = newHTTPBuffer()
		oldHash = ""
	)

	run := func() error {
		defer buffer.Reset()
		req, err := http.NewRequest("GET", "/metrics", nil)
		if err != nil {
			return fmt.Errorf("error creating request: %w", err)
		}
		handler.ServeHTTP(buffer, req)
		if (buffer.statusCode / 100) != 2 {
			return fmt.Errorf("unexpected status code: %d", buffer.statusCode)
		}

		if hash := buffer.Sum(); hash == oldHash {
			logger.Debug().Msg("no change in metrics")
			return nil
		} else {
			oldHash = hash
		}

		f, err := os.Create(flags.textFileOutput + ".$$")
		if err != nil {
			return fmt.Errorf("error creating text file: %w", err)
		}

		if _, err := io.Copy(f, buffer); err != nil {
			return fmt.Errorf("error writing text file: %w", err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("error closing text file: %w", err)
		}

		if err := os.Rename(flags.textFileOutput+".$$", flags.textFileOutput); err != nil {
			return fmt.Errorf("error renaming text file: %w", err)
		}
		logger.Info().Msgf("wrote text file: %s", flags.textFileOutput)

		return nil
	}

	if err := run(); err != nil {
		return nil, err
	}

	return func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := run(); err != nil {
					logger.Error().Msgf("error writing text file: %v", err)
				}
			}
		}
	}, nil
}

var flags struct {
	listenAddr           string
	logLevel             string
	textFileOutput       string
	excludeSnapshotNames *cli.StringSlice
}

func main() {
	app := &cli.App{
		Name:   "zfs-event-exporter",
		Usage:  "Prometheus metrics for pools and snapshots based on ZFS event history",
		Action: run,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "listen-addr",
				Value:       ":9128",
				Usage:       "listen address for metrics http server",
				Destination: &flags.listenAddr,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Value:       "info",
				Usage:       "log level for daemon",
				Destination: &flags.logLevel,
			},
			&cli.StringFlag{
				Name:        "text-file-output",
				Value:       "",
				Usage:       "file path for node-exporter text file",
				Destination: &flags.textFileOutput,
			},
			&cli.StringSliceFlag{
				Name:        "exclude-snapshot-name",
				Usage:       "exclude snapshots matching regular expression",
				Destination: flags.excludeSnapshotNames,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(c *cli.Context) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewBuildInfoCollector())

	keep := func(_, _ string) bool {
		return true
	}

	if flags.excludeSnapshotNames != nil {
		var match []*regexp.Regexp
		for _, exclude := range flags.excludeSnapshotNames.Value() {
			r, err := regexp.Compile(exclude)
			if err != nil {
				return fmt.Errorf("error compiling exclude regular expression: %w", err)
			}
			match = append(match, r)
		}

		keep = func(dataset, snapshot string) bool {
			for _, r := range match {
				if r.MatchString(dataset + "@" + snapshot) {
					return false
				}
			}
			return true
		}
	}

	collectorSnapshot, err := snapshot.NewCollector(ctx, logger, keep)
	if err != nil {
		logger.Fatal().Msgf("error creating collector: %v", err)
	}
	collectorPool := pool.NewCollector(logger)
	reg.MustRegister(collectorSnapshot)
	reg.MustRegister(collectorPool)

	flag.Parse()

	// setting log level appropriately
	lvl, err := zerolog.ParseLevel(flags.logLevel)
	if err != nil {
		logger.Fatal().Msgf("invalid log level: %v", err)
	}
	logger = logger.Level(lvl)

	g, ctx := errgroup.WithContext(ctx)

	srv := &http.Server{Addr: flags.listenAddr}
	mux := http.NewServeMux()
	srv.Handler = mux

	// Expose the registered metrics via HTTP.
	metricsHandler := promhttp.HandlerFor(
		reg,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	)
	mux.Handle("/metrics", metricsHandler)

	go func() {
		<-ctx.Done()
		logger.Debug().Msg("shutting down http server")
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error().Msgf("error shutting down http server: %v", err)
		}
	}()

	if flags.textFileOutput != "" {
		// create separate registry for text file output
		regTextFile := prometheus.NewRegistry()
		regTextFile.MustRegister(collectorSnapshot)
		regTextFile.MustRegister(collectorPool)
		metricsHandler := promhttp.HandlerFor(
			regTextFile,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
			},
		)

		f, err := runTextFileOutput(ctx, metricsHandler)
		if err != nil {
			logger.Fatal().Msgf("error running text file output: %v", err)
		}
		g.Go(func() error {
			f()
			return nil
		})
	}

	g.Go(srv.ListenAndServe)

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error running: %w", err)
	}

	return nil
}
