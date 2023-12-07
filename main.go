package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

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

	listenAddr     = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	logLevel       = flag.String("log-level", "info", "log level (debug, info, warn, error, fatal, panic)")
	textFileOutput = flag.String("text-file-output", "", "path to write metrics to as text file")
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

		f, err := os.Create(*textFileOutput + ".$$")
		if err != nil {
			return fmt.Errorf("error creating text file: %w", err)
		}

		if _, err := io.Copy(f, buffer); err != nil {
			return fmt.Errorf("error writing text file: %w", err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("error closing text file: %w", err)
		}

		if err := os.Rename(*textFileOutput+".$$", *textFileOutput); err != nil {
			return fmt.Errorf("error renaming text file: %w", err)
		}
		logger.Info().Msgf("wrote text file: %s", *textFileOutput)

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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewBuildInfoCollector())

	collectorSnapshot, err := snapshot.NewCollector(ctx, logger)
	if err != nil {
		logger.Fatal().Msgf("error creating collector: %v", err)
	}
	collectorPool := pool.NewCollector(logger)
	reg.MustRegister(collectorSnapshot)
	reg.MustRegister(collectorPool)

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

	if *textFileOutput != "" {
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
		logger.Fatal().Msgf("error running: %v", err)
	}
}
