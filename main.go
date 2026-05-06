package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", envOr("CONFIG_PATH", "/etc/config/runtime-config.yaml"), "bootstrap config file")
	stateDir := flag.String("state-dir", envOr("STATE_DIR", "/var/lib/k8s-delete-interceptor"), "shared PVC state directory")
	tlsAddr := flag.String("tls-addr", envOr("TLS_ADDR", ":8443"), "TLS webhook address")
	webAddr := flag.String("web-addr", envOr("WEB_ADDR", ":8080"), "web console address")
	certFile := flag.String("tls-cert", envOr("TLS_CERT_FILE", "/tls/tls.crt"), "TLS cert")
	keyFile := flag.String("tls-key", envOr("TLS_KEY_FILE", "/tls/tls.key"), "TLS key")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	app, err := NewApp(ctx, *configPath, *stateDir)
	if err != nil {
		log.Fatalf("init app: %v", err)
	}
	app.startBackground(ctx)

	muxTLS := http.NewServeMux()
	app.RegisterRoutes(muxTLS)
	muxWeb := http.NewServeMux()
	app.RegisterRoutes(muxWeb)

	tlsServer := &http.Server{Addr: *tlsAddr, Handler: muxTLS, ReadHeaderTimeout: 10 * time.Second}
	webServer := &http.Server{Addr: *webAddr, Handler: muxWeb, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		log.Printf("web console listening on %s", *webAddr)
		if err := webServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("web server: %v", err)
		}
	}()
	go func() {
		if _, err := os.Stat(*certFile); err != nil {
			log.Printf("TLS cert not found, webhook TLS server disabled: %v", err)
			return
		}
		log.Printf("admission webhook TLS listening on %s", *tlsAddr)
		if err := tlsServer.ListenAndServeTLS(*certFile, *keyFile); err != nil && err != http.ErrServerClosed {
			log.Fatalf("tls server: %v", err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = tlsServer.Shutdown(shutdown)
	_ = webServer.Shutdown(shutdown)
	if app.mongo != nil {
		app.mongo.Disconnect(shutdown)
	}
	log.Printf("shutdown complete")
}
