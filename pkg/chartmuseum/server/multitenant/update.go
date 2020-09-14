/*
Copyright The Helm Authors.

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

package multitenant

import (
	"time"

	cm_logger "helm.sh/chartmuseum/pkg/chartmuseum/logger"

	cm_storage "github.com/chartmuseum/storage"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"helm.sh/helm/v3/pkg/repo"
)

type (
	event struct {
		RepoName     string             `json:"repo_name"`
		OpType       operationType      `json:"operation_type"`
		ChartVersion *repo.ChartVersion `json:"chart_version"`
	}
)
type operationType int

const (
	updateChart operationType = 0
	addChart    operationType = 1
	deleteChart operationType = 2
)

func (server *MultiTenantServer) initCacheTimer() {
	if server.CacheInterval > 0 {
		// delta update the cache every X duration
		// (in case the files on the disk are manually manipulated)
		go func() {
			t := time.NewTicker(server.CacheInterval)
			for _ = range t.C {
				server.RebuildIndex()
			}
		}()
	} else {
		server.RebuildIndex()
	}
}

func (server *MultiTenantServer) emitEvent(repo string, operationType operationType, chart *repo.ChartVersion) {
	e := event{
		RepoName:     repo,
		OpType:       operationType,
		ChartVersion: chart,
	}
	server.EventChan <- e
	log := server.Logger.ContextLoggingFn(&gin.Context{})
	log(cm_logger.DebugLevel, "event emitted", zap.Any("event", e))
}

func (server *MultiTenantServer) receiveEvents() {
	server.Router.Logger.Debug("start receiver")
	for {
		log := server.Logger.ContextLoggingFn(&gin.Context{})
		e := <-server.EventChan

		repo := e.RepoName
		log(cm_logger.DebugLevel, "event received", zap.Any("event", e))

		entry, err := server.initCacheEntry(log, repo)
		if err != nil {
			log(cm_logger.ErrorLevel, "initCacheEntry fail", zap.Error(err), zap.String("repo", repo))
			continue
		}
		index := entry.RepoIndex

		tenant := server.Tenants[e.RepoName]
		tenant.RegenerationLock.Lock()

		if e.ChartVersion == nil {
			log(cm_logger.WarnLevel, "event does not contain chart version", zap.String("repo", repo),
				"operation_type", e.OpType)
			tenant.RegenerationLock.Unlock()
			continue
		}

		switch e.OpType {
		case updateChart:
			index.UpdateEntry(e.ChartVersion)
		case addChart:
			index.AddEntry(e.ChartVersion)
		case deleteChart:
			index.RemoveEntry(e.ChartVersion)
		default:
			log(cm_logger.ErrorLevel, "invalid operation type", zap.String("repo", repo),
				"operation_type", e.OpType)
			tenant.RegenerationLock.Unlock()
			continue
		}

		err = index.Regenerate()
		if err != nil {
			log(cm_logger.ErrorLevel, "regenerate fail", zap.Error(err), zap.String("repo", repo))
			tenant.RegenerationLock.Unlock()
			continue
		}
		entry.RepoIndex = index

		err = server.saveCacheEntry(log, entry)
		if err != nil {
			log(cm_logger.ErrorLevel, "saveCacheEntry fail", zap.Error(err), zap.String("repo", repo))
			tenant.RegenerationLock.Unlock()
			continue
		}

		if server.UseStatefiles {
			// Dont wait, save index-cache.yaml to storage in the background.
			// It is not crucial if this does not succeed, we will just log any errors
			go server.saveStatefile(log, e.RepoName, entry.RepoIndex.Raw)
		}

		tenant.RegenerationLock.Unlock()
		log(cm_logger.DebugLevel, "event handled successfully", zap.Any("event", e))
	}
}

func (server *MultiTenantServer) RebuildIndex() {
	for repo, _ := range server.Tenants {
		go func(repo string) {
			log := server.Logger.ContextLoggingFn(&gin.Context{})
			log(cm_logger.InfoLevel, "begin to rebuild index", zap.String("repo", repo))
			entry, err := server.initCacheEntry(log, repo)
			if err != nil {
				errStr := err.Error()
				log(cm_logger.ErrorLevel, errStr,
					"repo", repo,
				)
				return
			}

			fo := <-server.getChartList(log, repo)

			if fo.err != nil {
				errStr := fo.err.Error()
				log(cm_logger.ErrorLevel, errStr,
					"repo", repo,
				)
				return
			}

			objects := server.getRepoObjectSlice(entry)
			diff := cm_storage.GetObjectSliceDiff(objects, fo.objects, server.TimestampTolerance)

			// return fast if no changes
			if !diff.Change {
				log(cm_logger.DebugLevel, "No change detected between cache and storage",
					"repo", repo,
				)
				return
			}

			log(cm_logger.DebugLevel, "Change detected between cache and storage",
				"repo", repo,
			)

			ir := <-server.regenerateRepositoryIndex(log, entry, diff)
			if ir.err != nil {
				errStr := ir.err.Error()
				log(cm_logger.ErrorLevel, errStr,
					"repo", repo,
				)
				return
			}
			entry.RepoIndex = ir.index

			if server.UseStatefiles {
				// Dont wait, save index-cache.yaml to storage in the background.
				// It is not crucial if this does not succeed, we will just log any errors
				go server.saveStatefile(log, repo, ir.index.Raw)
			}

		}(repo)
	}
}
