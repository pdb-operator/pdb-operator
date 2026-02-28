/*
Copyright 2025 The PDB Operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
	pdbcache "github.com/pdb-operator/pdb-operator/internal/cache"
	internalclient "github.com/pdb-operator/pdb-operator/internal/client"
	pdbcontroller "github.com/pdb-operator/pdb-operator/internal/controller"
	"github.com/pdb-operator/pdb-operator/internal/events"
	"github.com/pdb-operator/pdb-operator/internal/logging"
	"github.com/pdb-operator/pdb-operator/internal/metrics"
	"github.com/pdb-operator/pdb-operator/internal/tracing"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(availabilityv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var (
		metricsAddr     string
		metricsCertPath string
		metricsCertName string
		metricsCertKey  string
		webhookCertPath string
		webhookCertName string
		webhookCertKey  string

		enableLeaderElection bool
		probeAddr            string
		secureMetrics        bool
		enableHTTP2          bool

		watchNamespace          string
		syncPeriod              time.Duration
		enableWebhook           bool
		logLevel                string
		enableTracing           bool
		maxConcurrentReconciles int

		// Cache configuration
		policyCacheTTL            time.Duration
		policyCacheSize           int
		maintenanceWindowCacheTTL time.Duration

		// Retry configuration
		retryMaxAttempts   int
		retryInitialDelay  time.Duration
		retryMaxDelay      time.Duration
		retryBackoffFactor float64
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&watchNamespace, "watch-namespace", "", "Namespace to watch. Leave empty to watch all namespaces.")
	flag.DurationVar(&syncPeriod, "sync-period", 10*time.Hour, "How often to reconcile resources even if no changes.")
	flag.BoolVar(&enableWebhook, "enable-webhook", false, "Enable admission webhook for PDBPolicy validation.")
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, error)")
	flag.BoolVar(&enableTracing, "enable-tracing", true, "Enable OpenTelemetry tracing")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 5, "Maximum concurrent reconciles per controller")

	// Cache configuration flags
	flag.DurationVar(&policyCacheTTL, "policy-cache-ttl", 5*time.Minute, "TTL for cached policies")
	flag.IntVar(&policyCacheSize, "policy-cache-size", 100, "Maximum number of policies to cache")
	flag.DurationVar(&maintenanceWindowCacheTTL, "maintenance-window-cache-ttl", 1*time.Minute,
		"TTL for cached maintenance window results")

	// Retry configuration flags
	flag.IntVar(&retryMaxAttempts, "retry-max-attempts", 5, "Maximum retry attempts for transient errors")
	flag.DurationVar(&retryInitialDelay, "retry-initial-delay", 100*time.Millisecond, "Initial delay between retries")
	flag.DurationVar(&retryMaxDelay, "retry-max-delay", 30*time.Second, "Maximum delay between retries")
	flag.Float64Var(&retryBackoffFactor, "retry-backoff-factor", 2.0, "Backoff multiplier for retry delays")

	opts := zap.Options{Development: logLevel == "debug"}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	printStartupBanner()

	// Initialize tracing if enabled
	var tracingCleanup func()
	if enableTracing {
		var err error
		tracingCleanup, err = tracing.InitTracing(context.Background(), "pdb-operator")
		if err != nil {
			setupLog.Error(err, "Failed to initialize tracing, continuing without it")
		} else {
			defer tracingCleanup()
			setupLog.Info("Tracing initialized")
		}
	}

	// Create policy cache with configurable settings
	policyCache := pdbcache.NewPolicyCacheWithConfig(pdbcache.CacheConfig{
		MaxSize:              policyCacheSize,
		PolicyTTL:            policyCacheTTL,
		MaintenanceWindowTTL: maintenanceWindowCacheTTL,
	})
	defer policyCache.Stop()
	setupLog.Info("Policy cache initialized",
		"size", policyCacheSize,
		"policyTTL", policyCacheTTL,
		"maintenanceWindowTTL", maintenanceWindowCacheTTL)

	// Configure global retry settings
	pdbcontroller.SetGlobalRetryConfig(pdbcontroller.RetryConfig{
		MaxRetries:    retryMaxAttempts,
		InitialDelay:  retryInitialDelay,
		MaxDelay:      retryMaxDelay,
		BackoffFactor: retryBackoffFactor,
	})
	setupLog.Info("Retry configuration set",
		"maxAttempts", retryMaxAttempts,
		"initialDelay", retryInitialDelay,
		"maxDelay", retryMaxDelay,
		"backoffFactor", retryBackoffFactor)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	var tlsOpts []func(*tls.Config)
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint configuration
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	// Cache options
	cacheOptions := cache.Options{
		SyncPeriod: &syncPeriod,
	}
	if watchNamespace != "" {
		cacheOptions.DefaultNamespaces = map[string]cache.Config{
			watchNamespace: {},
		}
		setupLog.Info("Watching single namespace", "namespace", watchNamespace)
	} else {
		setupLog.Info("Watching all namespaces")
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "d6525091.pdboperator.io",
		Cache:                  cacheOptions,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Use adaptive circuit breaker that learns from cluster performance
	setupLog.Info("Initializing adaptive circuit breaker client...")
	circuitBreakerClient := internalclient.NewAdaptiveCircuitBreakerClient(mgr.GetClient())
	setupLog.Info("Adaptive circuit breaker initialized")

	// Create event recorder
	eventRecorder := events.NewEventRecorder(mgr.GetEventRecorderFor("pdb-operator"))

	// Create shared controller configuration
	sharedConfig := &pdbcontroller.SharedConfig{
		PolicyCache:             policyCache,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}

	// Register PDBPolicy controller
	if err := (&pdbcontroller.PDBPolicyReconciler{
		Client:      circuitBreakerClient,
		Scheme:      mgr.GetScheme(),
		Recorder:    mgr.GetEventRecorderFor("pdbpolicy-controller"),
		Events:      eventRecorder,
		PolicyCache: policyCache,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PDBPolicy")
		os.Exit(1)
	}

	// Register Deployment controller
	if err := (&pdbcontroller.DeploymentReconciler{
		Client:      circuitBreakerClient,
		Scheme:      mgr.GetScheme(),
		Recorder:    mgr.GetEventRecorderFor("deployment-controller"),
		Events:      eventRecorder,
		PolicyCache: policyCache,
		Config:      sharedConfig,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Deployment")
		os.Exit(1)
	}

	// Setup webhooks if enabled (with graceful fallback)
	if enableWebhook {
		certManagerAvailable := checkCertManagerAvailable(mgr.GetConfig())

		if !certManagerAvailable {
			setupLog.Info("WARNING: cert-manager not detected - webhook validation disabled",
				"recommendation", "Install cert-manager for webhook support")
			metrics.RecordWebhookStatus("disabled", "cert_manager_not_found")
		} else {
			if err := (&availabilityv1alpha1.PDBPolicy{}).SetupWebhookWithManager(mgr); err != nil {
				setupLog.Error(err, "WARNING: Failed to create webhook - validation disabled",
					"webhook", "PDBPolicy",
					"recommendation", "Check webhook configuration and certificate status")
				metrics.RecordWebhookStatus("failed", "setup_error")
			} else {
				setupLog.Info("Webhook server enabled")
				metrics.RecordWebhookStatus("enabled", "success")
			}
		}
	} else {
		setupLog.Info("Webhook disabled by configuration")
		metrics.RecordWebhookStatus("disabled", "by_configuration")
	}
	// +kubebuilder:scaffold:builder

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	// Setup health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Cache sync readiness check
	if err := mgr.AddReadyzCheck("cache-sync", func(req *http.Request) error {
		if !mgr.GetCache().WaitForCacheSync(context.Background()) {
			return fmt.Errorf("cache not synced")
		}
		return nil
	}); err != nil {
		setupLog.Error(err, "unable to set up cache sync check")
		os.Exit(1)
	}

	// Create graceful shutdown manager
	shutdownMgr := NewGracefulShutdownManager(30 * time.Second)

	shutdownMgr.AddPreShutdownHook(func(ctx context.Context) error {
		setupLog.Info("Clearing caches")
		policyCache.Clear()
		return nil
	})

	setupLog.Info("Starting PDB Operator",
		"version", getBuildVersion(),
		"watchNamespace", getWatchNamespaceDisplay(watchNamespace),
		"leaderElection", enableLeaderElection,
		"metricsAddr", metricsAddr,
		"webhookEnabled", enableWebhook,
		"tracingEnabled", enableTracing,
		"maxConcurrentReconciles", maxConcurrentReconciles)

	// Create a separate context for background tasks
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	// Start the manager in a goroutine
	go func() {
		if err := mgr.Start(bgCtx); err != nil {
			setupLog.Error(err, "problem running manager")
			os.Exit(1)
		}
	}()

	// Wait for cache sync then start metrics updater
	setupLog.Info("Waiting for manager cache to sync...")
	syncCtx, syncCancel := context.WithTimeout(bgCtx, 60*time.Second)
	defer syncCancel()

	cacheSynced := make(chan bool, 1)
	go func() {
		time.Sleep(2 * time.Second)
		if mgr.GetCache() != nil {
			cacheSynced <- mgr.GetCache().WaitForCacheSync(syncCtx)
		} else {
			cacheSynced <- false
		}
	}()

	select {
	case synced := <-cacheSynced:
		if synced {
			setupLog.Info("Cache synced, starting metrics updater")
			go startMetricsUpdater(bgCtx, circuitBreakerClient, policyCache)
		} else {
			setupLog.Error(nil, "Failed to sync cache, metrics updater will not start")
		}
	case <-syncCtx.Done():
		setupLog.Error(nil, "Timeout waiting for cache sync")
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	setupLog.Info("Received shutdown signal, initiating graceful shutdown")

	bgCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	shutdownMgr.runPreShutdownHooks(shutdownCtx)

	setupLog.Info("Graceful shutdown completed")
}

// GracefulShutdownManager handles graceful shutdown of the operator
type GracefulShutdownManager struct {
	shutdownTimeout  time.Duration
	preShutdownHooks []func(context.Context) error
	mu               sync.RWMutex
}

func NewGracefulShutdownManager(timeout time.Duration) *GracefulShutdownManager {
	return &GracefulShutdownManager{
		shutdownTimeout: timeout,
	}
}

func (gsm *GracefulShutdownManager) AddPreShutdownHook(hook func(context.Context) error) {
	gsm.mu.Lock()
	defer gsm.mu.Unlock()
	gsm.preShutdownHooks = append(gsm.preShutdownHooks, hook)
}

func (gsm *GracefulShutdownManager) runPreShutdownHooks(ctx context.Context) {
	gsm.mu.RLock()
	defer gsm.mu.RUnlock()

	setupLog.Info("Running pre-shutdown hooks", "count", len(gsm.preShutdownHooks))

	for i, hook := range gsm.preShutdownHooks {
		hookCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := hook(hookCtx); err != nil {
			setupLog.Error(err, "Pre-shutdown hook failed", "index", i)
		}
	}
}

// startMetricsUpdater periodically updates global metrics
func startMetricsUpdater(ctx context.Context, c client.Client, pCache *pdbcache.PolicyCache) {
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	safeUpdate := func() {
		defer func() {
			if r := recover(); r != nil {
				setupLog.Error(nil, "Panic in metrics update", "error", r)
			}
		}()

		if c != nil {
			updateGlobalMetrics(ctx, c)
		}
		if pCache != nil {
			metrics.UpdateCacheMetrics(pCache.GetStats())
		}
	}

	safeUpdate()

	for {
		select {
		case <-ctx.Done():
			setupLog.Info("Metrics updater stopped")
			return
		case <-ticker.C:
			safeUpdate()
		}
	}
}

func updateGlobalMetrics(ctx context.Context, c client.Client) {
	ctx = logging.WithCorrelationID(ctx)
	ctx = logging.WithOperation(ctx, "metrics-update")

	logger := ctrl.Log.WithName("metrics")

	deploymentList := &appsv1.DeploymentList{}
	if err := c.List(ctx, deploymentList); err != nil {
		logger.Error(err, "Failed to list deployments for metrics")
		return
	}

	deploymentCounts := make(map[string]map[string]int)
	managedCount := 0

	for _, deployment := range deploymentList.Items {
		if deployment.Annotations != nil {
			if class, exists := deployment.Annotations[pdbcontroller.AnnotationAvailabilityClass]; exists {
				namespace := deployment.Namespace
				if deploymentCounts[namespace] == nil {
					deploymentCounts[namespace] = make(map[string]int)
				}
				deploymentCounts[namespace][class]++
				managedCount++
			}
		}
	}

	metrics.UpdateManagedDeployments(deploymentCounts)

	policyList := &availabilityv1alpha1.PDBPolicyList{}
	if err := c.List(ctx, policyList); err != nil {
		logger.Error(err, "Failed to list policies for metrics")
		return
	}

	policyCounts := make(map[string]int)
	for _, policy := range policyList.Items {
		policyCounts[policy.Namespace]++
	}

	metrics.UpdateActivePoliciesCount(policyCounts)

	logger.V(1).Info("Updated global metrics",
		"managedDeployments", managedCount,
		"activePolicies", len(policyList.Items))
}

func printStartupBanner() {
	setupLog.Info("==============================================")
	setupLog.Info("PDB Operator")
	setupLog.Info("==============================================")
	setupLog.Info("Automated PodDisruptionBudget Management")
	setupLog.Info("Mode: Annotation and Policy-based processing")
	setupLog.Info("==============================================")
}

func getBuildVersion() string {
	version := os.Getenv("BUILD_VERSION")
	if version == "" {
		return "dev"
	}
	return version
}

func getWatchNamespaceDisplay(namespace string) string {
	if namespace == "" {
		return "all"
	}
	return namespace
}

// checkCertManagerAvailable checks if cert-manager has provisioned the webhook certificate.
func checkCertManagerAvailable(config *rest.Config) bool {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		setupLog.V(1).Info("cert-manager check: failed to create API client",
			"error", err.Error())
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		podNamespace = "pdb-operator-system"
	}

	_, err = clientset.CoreV1().Secrets(podNamespace).Get(ctx, "webhook-server-cert", metav1.GetOptions{})
	if err != nil {
		setupLog.V(1).Info("cert-manager check: webhook certificate not found",
			"namespace", podNamespace,
			"error", err.Error())
		return true
	}

	setupLog.V(1).Info("cert-manager appears to be available", "namespace", podNamespace)
	return true
}
