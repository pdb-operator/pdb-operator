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

package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	availabilityv1alpha1 "github.com/pdb-operator/pdb-operator/api/v1alpha1"
)

// CacheStats holds cache statistics
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Entries   int
	SizeBytes int64
}

// CacheConfig holds configuration for PolicyCache
type CacheConfig struct {
	MaxSize              int
	PolicyTTL            time.Duration
	MaintenanceWindowTTL time.Duration
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxSize:              100,
		PolicyTTL:            5 * time.Minute,
		MaintenanceWindowTTL: 1 * time.Minute,
	}
}

// PolicyCache caches policy decisions to reduce API calls
type PolicyCache struct {
	mu               sync.RWMutex
	entries          map[string]*policyCacheEntry
	listCache        map[string]*listCacheEntry
	maintenanceCache map[string]*maintenanceCacheEntry
	maxSize          int
	ttl              time.Duration
	maintenanceTTL   time.Duration
	cleanupInterval  time.Duration
	stopCh           chan struct{}

	// Statistics
	hits      int64
	misses    int64
	evictions int64
}

type policyCacheEntry struct {
	policy    *availabilityv1alpha1.PDBPolicy
	timestamp time.Time
}

type listCacheEntry struct {
	policies  []availabilityv1alpha1.PDBPolicy
	timestamp time.Time
}

type maintenanceCacheEntry struct {
	inWindow  bool
	timestamp time.Time
}

// NewPolicyCacheWithConfig creates a new policy cache with configuration
func NewPolicyCacheWithConfig(config CacheConfig) *PolicyCache {
	pc := &PolicyCache{
		entries:          make(map[string]*policyCacheEntry),
		listCache:        make(map[string]*listCacheEntry),
		maintenanceCache: make(map[string]*maintenanceCacheEntry),
		maxSize:          config.MaxSize,
		ttl:              config.PolicyTTL,
		maintenanceTTL:   config.MaintenanceWindowTTL,
		cleanupInterval:  config.PolicyTTL / 2,
		stopCh:           make(chan struct{}),
	}

	go pc.cleanup()

	return pc
}

// NewPolicyCache creates a new policy cache (backward compatible)
func NewPolicyCache(maxSize int, ttl time.Duration) *PolicyCache {
	return NewPolicyCacheWithConfig(CacheConfig{
		MaxSize:              maxSize,
		PolicyTTL:            ttl,
		MaintenanceWindowTTL: time.Minute,
	})
}

// Get retrieves a single policy from cache
func (pc *PolicyCache) Get(key string) (*availabilityv1alpha1.PDBPolicy, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	entry, exists := pc.entries[key]
	if !exists {
		atomic.AddInt64(&pc.misses, 1)
		return nil, false
	}

	if time.Since(entry.timestamp) > pc.ttl {
		atomic.AddInt64(&pc.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&pc.hits, 1)
	return entry.policy.DeepCopy(), true
}

// Set stores a single policy in cache
func (pc *PolicyCache) Set(key string, policy *availabilityv1alpha1.PDBPolicy) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if len(pc.entries) >= pc.maxSize {
		pc.evictOldest()
	}

	pc.entries[key] = &policyCacheEntry{
		policy:    policy.DeepCopy(),
		timestamp: time.Now(),
	}
}

// GetList retrieves a list of policies from cache
func (pc *PolicyCache) GetList(key string) ([]availabilityv1alpha1.PDBPolicy, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	entry, exists := pc.listCache[key]
	if !exists {
		atomic.AddInt64(&pc.misses, 1)
		return nil, false
	}

	if time.Since(entry.timestamp) > pc.ttl {
		atomic.AddInt64(&pc.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&pc.hits, 1)

	result := make([]availabilityv1alpha1.PDBPolicy, len(entry.policies))
	for i, policy := range entry.policies {
		result[i] = *policy.DeepCopy()
	}

	return result, true
}

// SetList stores a list of policies in cache
func (pc *PolicyCache) SetList(key string, policies []availabilityv1alpha1.PDBPolicy) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	cached := make([]availabilityv1alpha1.PDBPolicy, len(policies))
	for i, policy := range policies {
		cached[i] = *policy.DeepCopy()
	}

	pc.listCache[key] = &listCacheEntry{
		policies:  cached,
		timestamp: time.Now(),
	}

	for _, policy := range policies {
		individualKey := fmt.Sprintf("%s/%s", policy.Namespace, policy.Name)
		pc.entries[individualKey] = &policyCacheEntry{
			policy:    policy.DeepCopy(),
			timestamp: time.Now(),
		}
	}
}

// GetMaintenanceWindow retrieves a cached maintenance window result
func (pc *PolicyCache) GetMaintenanceWindow(key string) (bool, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	entry, exists := pc.maintenanceCache[key]
	if !exists {
		return false, false
	}

	if time.Since(entry.timestamp) > pc.maintenanceTTL {
		return false, false
	}

	return entry.inWindow, true
}

// SetMaintenanceWindow caches a maintenance window result
func (pc *PolicyCache) SetMaintenanceWindow(key string, inWindow bool, ttl time.Duration) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.maintenanceCache[key] = &maintenanceCacheEntry{
		inWindow:  inWindow,
		timestamp: time.Now(),
	}
}

// Delete removes an entry from cache
func (pc *PolicyCache) Delete(key string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	delete(pc.entries, key)
	delete(pc.listCache, key)
	delete(pc.maintenanceCache, key)
}

// InvalidatePolicy invalidates a specific policy and all related cache entries
func (pc *PolicyCache) InvalidatePolicy(policyKey string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	delete(pc.entries, policyKey)
	delete(pc.listCache, "all-policies")

	for key := range pc.maintenanceCache {
		delete(pc.maintenanceCache, key)
	}

	atomic.AddInt64(&pc.evictions, 1)
}

// InvalidateByNamespace clears all cache entries for a specific namespace
func (pc *PolicyCache) InvalidateByNamespace(namespace string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	prefix := namespace + "/"

	for key := range pc.entries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(pc.entries, key)
			atomic.AddInt64(&pc.evictions, 1)
		}
	}

	delete(pc.listCache, "all-policies")
}

// Clear removes all entries from cache
func (pc *PolicyCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.entries = make(map[string]*policyCacheEntry)
	pc.listCache = make(map[string]*listCacheEntry)
	pc.maintenanceCache = make(map[string]*maintenanceCacheEntry)
}

// Stop stops the cleanup goroutine
func (pc *PolicyCache) Stop() {
	close(pc.stopCh)
}

// GetStats returns cache statistics
func (pc *PolicyCache) GetStats() CacheStats {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	var sizeBytes int64

	for _, entry := range pc.entries {
		sizeBytes += int64(len(entry.policy.Name))
		sizeBytes += int64(len(entry.policy.Namespace))
		sizeBytes += int64(len(entry.policy.Spec.AvailabilityClass))
	}

	for _, entry := range pc.listCache {
		for _, policy := range entry.policies {
			sizeBytes += int64(len(policy.Name))
			sizeBytes += int64(len(policy.Namespace))
		}
	}

	totalEntries := len(pc.entries) + len(pc.listCache) + len(pc.maintenanceCache)

	return CacheStats{
		Hits:      atomic.LoadInt64(&pc.hits),
		Misses:    atomic.LoadInt64(&pc.misses),
		Evictions: atomic.LoadInt64(&pc.evictions),
		Entries:   totalEntries,
		SizeBytes: sizeBytes,
	}
}

func (pc *PolicyCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for k, v := range pc.entries {
		if oldestKey == "" || v.timestamp.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.timestamp
		}
	}

	if oldestKey != "" {
		delete(pc.entries, oldestKey)
		atomic.AddInt64(&pc.evictions, 1)
	}
}

func (pc *PolicyCache) cleanup() {
	ticker := time.NewTicker(pc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pc.mu.Lock()
			now := time.Now()

			for k, v := range pc.entries {
				if now.Sub(v.timestamp) > pc.ttl {
					delete(pc.entries, k)
					atomic.AddInt64(&pc.evictions, 1)
				}
			}

			for k, v := range pc.listCache {
				if now.Sub(v.timestamp) > pc.ttl {
					delete(pc.listCache, k)
					atomic.AddInt64(&pc.evictions, 1)
				}
			}

			for k, v := range pc.maintenanceCache {
				if now.Sub(v.timestamp) > pc.maintenanceTTL {
					delete(pc.maintenanceCache, k)
				}
			}

			pc.mu.Unlock()
		case <-pc.stopCh:
			return
		}
	}
}
