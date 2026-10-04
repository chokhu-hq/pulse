package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aelpxy/pulse/metrics"
	"github.com/aelpxy/pulse/server"
	"github.com/charmbracelet/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configFile := flag.String("config", "", "path to config file (default: config.json)")
	port := flag.String("port", "", "server port (overrides config file)")
	maxConns := flag.Int("max-connections", 100000, "maximum concurrent connections")
	debugFlag := flag.Bool("debug", false, "enable debug logging (overrides config file)")
	pprofFlag := flag.Bool("pprof", false, "expose /debug/pprof endpoints")
	flag.Parse()

	configPath := *configFile
	if configPath == "" {
		configPath = getEnv("PULSE_CONFIG", "config.json")
	}

	config := server.Config{
		AppsConfigFile: configPath,
		MaxConnections: *maxConns,
	}

	srv, serverConfig, err := server.New(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create server: %v\n", err)
		os.Exit(1)
	}

	log.SetReportTimestamp(true)
	debugMode := *debugFlag || serverConfig.Debug
	if debugMode {
		log.SetLevel(log.DebugLevel)
		log.Info("debug mode enabled", "from_flag", *debugFlag, "from_config", serverConfig.Debug)
	} else {
		log.SetLevel(log.InfoLevel)
		log.Info("debug mode disabled")
	}

	serverPort := *port
	if serverPort == "" {
		serverPort = getEnv("PULSE_PORT", "")
	}
	if serverPort == "" && serverConfig.Port != "" {
		serverPort = serverConfig.Port
	}
	if serverPort == "" {
		serverPort = "8080"
	}

	metrics.AppsLoaded.Set(float64(srv.GetAppsManager().GetAppCount()))

	mux := http.NewServeMux()
	mux.HandleFunc("/app/", srv.HandleWebSocket)
	mux.HandleFunc("/apps/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 3 {
			switch parts[2] {
			case "events":
				srv.HandleEvents(w, r)
			case "batch_events":
				srv.HandleBatchEvents(w, r)
			case "channels":
				srv.HandleChannels(w, r)
			default:
				http.Error(w, "Not found", http.StatusNotFound)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	})
	mux.HandleFunc("/stats", srv.HandleStats)
	mux.HandleFunc("/apps", srv.HandleApps)
	mux.Handle("/metrics", promhttp.Handler())

	if *pprofFlag {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})

	httpServer := &http.Server{
		Addr:         ":" + serverPort,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("pulse server starting", "port", serverPort)
		log.Info("config file", "path", configPath)
		log.Info("")

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Error("http server shutdown error", "error", err)
	}

	if err := srv.Shutdown(10 * time.Second); err != nil {
		log.Error("pulse server shutdown error", "error", err)
	}

	log.Info("server stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
