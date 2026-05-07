package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type App struct {
	configValue      atomic.Value // *RuntimeConfig
	local            *LocalStore
	mongo            *MongoStore
	mongoURI         string
	mongoDatabase    string
	mongoMu          sync.Mutex
	kubeClient       *kubernetes.Clientset
	dynamicClient    dynamic.Interface
	discoveryClient  discovery.DiscoveryInterface
	adminToken       string
	metadataValue    atomic.Value // *ClusterMetadata
	metadataMu       sync.Mutex
	metadataRefresh  atomic.Bool
	telegramOffsetMu sync.Mutex
	telegramOffsets  map[string]int64
}

func NewApp(ctx context.Context, bootstrapPath, stateDir string) (*App, error) {
	cfg, err := loadBootstrapConfig(bootstrapPath)
	if err != nil {
		return nil, err
	}
	if stateDir != "" {
		cfg.Storage.RootDir = stateDir
	}
	local, err := NewLocalStore(cfg.Storage.RootDir)
	if err != nil {
		return nil, err
	}
	// 优先使用本地 last-good 配置。
	if localCfg, err := local.LoadLatestConfig(); err == nil {
		cfg = localCfg
	} else {
		log.Printf("local config unavailable, fallback to bootstrap/mongo: %v", err)
	}
	uri, db := resolveMongoURI(cfg)
	var mongoStore *MongoStore
	if uri != "" {
		if m, err := NewMongoStore(ctx, uri, db); err == nil {
			mongoStore = m
			_ = mongoStore.EnsureConfig(ctx, cfg)
			_ = mongoStore.EnsureTelegramConfig(ctx, cfg.Telegram)
			if mc, err := mongoStore.GetActiveConfig(ctx); err == nil && mc.Version >= cfg.Version {
				cfg = mc
			}
		} else {
			log.Printf("mongo unavailable at startup: %v", err)
		}
	}
	_ = local.SaveConfig(cfg, "startup")
	a := &App{local: local, mongo: mongoStore, mongoURI: uri, mongoDatabase: db, adminToken: os.Getenv("WEB_ADMIN_TOKEN"), telegramOffsets: map[string]int64{}}
	a.configValue.Store(cfg)
	a.initKubeClients()
	a.loadPersistedMetadata(ctx)
	return a, nil
}

func (a *App) Config() *RuntimeConfig {
	v := a.configValue.Load()
	if v == nil {
		return nil
	}
	cfg, _ := v.(*RuntimeConfig)
	return cfg
}

func (a *App) SetConfig(cfg *RuntimeConfig, source string) error {
	if err := validateRuntimeConfig(cfg); err != nil {
		return err
	}
	if err := a.local.SaveConfig(cfg, source); err != nil {
		return err
	}
	a.configValue.Store(cfg)
	return nil
}

func (a *App) initKubeClients() {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
			cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		}
	}
	if err != nil {
		log.Printf("kubernetes client unavailable: %v", err)
		return
	}
	if c, err := kubernetes.NewForConfig(cfg); err == nil {
		a.kubeClient = c
	} else {
		log.Printf("kube client error: %v", err)
	}
	if c, err := dynamic.NewForConfig(cfg); err == nil {
		a.dynamicClient = c
	} else {
		log.Printf("dynamic client error: %v", err)
	}
	if c, err := discovery.NewDiscoveryClientForConfig(cfg); err == nil {
		a.discoveryClient = c
	} else {
		log.Printf("discovery client error: %v", err)
	}
}

func (a *App) startBackground(ctx context.Context) {
	go a.configSyncLoop(ctx)
	go a.spoolFlushLoop(ctx)
	go a.mongoHealthLoop(ctx)
	go a.metadataRefreshLoop(ctx)
	go a.telegramNotificationLoop(ctx)
	go a.telegramCallbackPollingLoop(ctx)
	go a.retentionMaintenanceLoop(ctx)
	go a.telegramQueueCleanupLoop(ctx)
	go func() {
		waitContext(ctx, 2*time.Second)
		a.emitStartupNotification(context.Background())
		if a.mongo == nil || !a.mongo.Healthy() {
			a.emitMongoStatusNotification(context.Background(), false, "startup mongo is unavailable")
		}
	}()
}

func (a *App) configSyncLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.mongo == nil || !a.mongo.Healthy() {
			continue
		}
		cfg, err := a.mongo.GetActiveConfig(ctx)
		if err != nil {
			continue
		}
		cur := a.Config()
		if cur == nil || cfg.Version > cur.Version {
			if err := a.SetConfig(cfg, "mongo-sync"); err == nil {
				log.Printf("runtime config hot loaded version=%d", cfg.Version)
				a.reconcileMongoConnection(ctx, cfg)
			}
		}
	}
}

func (a *App) spoolFlushLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.mongo != nil && a.mongo.Healthy() {
			_ = a.local.FlushEventsToMongo(ctx, a.mongo, 200)
			_ = a.flushSystemNotifications(ctx)
		}
	}
}

func (a *App) mongoHealthLoop(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	lastHealthy := a.mongo != nil && a.mongo.Healthy()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.mongo != nil {
			err := a.mongo.Ping(ctx)
			nowHealthy := err == nil && a.mongo.Healthy()
			if lastHealthy && !nowHealthy {
				detail := ""
				if err != nil {
					detail = err.Error()
				}
				log.Printf("mongo became unavailable: %s", detail)
				a.emitMongoStatusNotification(context.Background(), false, detail)
			}
			if !lastHealthy && nowHealthy {
				log.Printf("mongo recovered")
				a.emitMongoStatusNotification(context.Background(), true, "")
				_ = a.flushSystemNotifications(ctx)
			}
			lastHealthy = nowHealthy
			if nowHealthy {
				continue
			}
		}
		cfg := a.Config()
		if cfg == nil {
			continue
		}
		uri, db := resolveMongoURI(cfg)
		if uri == "" {
			continue
		}
		m, err := NewMongoStore(ctx, uri, db)
		if err == nil {
			a.mongo = m
			a.mongoURI, a.mongoDatabase = uri, db
			_ = a.mongo.EnsureConfig(ctx, cfg)
			_ = a.mongo.EnsureTelegramConfig(ctx, cfg.Telegram)
			lastHealthy = true
			log.Printf("mongo reconnected")
			a.emitMongoStatusNotification(context.Background(), true, "")
			_ = a.flushSystemNotifications(ctx)
		} else if lastHealthy {
			lastHealthy = false
			log.Printf("mongo reconnect failed after healthy state: %v", err)
			a.emitMongoStatusNotification(context.Background(), false, err.Error())
		}
	}
}

func (a *App) latestConfigFromStore(ctx context.Context) (*RuntimeConfig, error) {
	cur := a.Config()
	if a.mongo != nil && a.mongo.Healthy() {
		cfg, err := a.mongo.GetActiveConfig(ctx)
		if err == nil && cfg != nil {
			if cur == nil || cfg.Version > cur.Version {
				if setErr := a.SetConfig(cfg, "mongo-refresh"); setErr == nil {
					log.Printf("runtime config refreshed before write version=%d", cfg.Version)
					a.reconcileMongoConnection(ctx, cfg)
				}
			}
			return cfg, nil
		}
		if err != nil {
			return cur, err
		}
	}
	return cur, nil
}

func (a *App) reconcileMongoConnection(ctx context.Context, cfg *RuntimeConfig) {
	if cfg == nil {
		return
	}
	uri, db := resolveMongoURI(cfg)
	a.mongoMu.Lock()
	defer a.mongoMu.Unlock()
	if uri == "" {
		if a.mongo != nil {
			log.Printf("mongo config cleared; keeping local runtime only")
		}
		a.mongo = nil
		a.mongoURI, a.mongoDatabase = "", ""
		return
	}
	if a.mongo != nil && a.mongo.Healthy() && uri == a.mongoURI && db == a.mongoDatabase {
		return
	}
	m, err := NewMongoStore(ctx, uri, db)
	if err != nil {
		log.Printf("mongo reconnect failed uri_source=%s db=%s err=%v", mongoURISource(cfg), db, err)
		return
	}
	a.mongo = m
	a.mongoURI, a.mongoDatabase = uri, db
	_ = a.mongo.EnsureConfig(ctx, cfg)
	_ = a.mongo.EnsureTelegramConfig(ctx, cfg.Telegram)
	_ = a.mongo.SaveConfig(ctx, cfg, "mongo-reconcile", true)
	log.Printf("mongo connection reconciled db=%s uri_source=%s", db, mongoURISource(cfg))
}

func mongoURISource(cfg *RuntimeConfig) string {
	if cfg == nil {
		return "none"
	}
	for _, ds := range cfg.DataSources {
		if !ds.Enabled || !ds.Active {
			continue
		}
		if ds.URIEnv != "" {
			return "env:" + ds.URIEnv
		}
		if ds.URI != "" {
			return "inline"
		}
	}
	if os.Getenv("MONGO_URI") != "" {
		return "env:MONGO_URI"
	}
	return "none"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
