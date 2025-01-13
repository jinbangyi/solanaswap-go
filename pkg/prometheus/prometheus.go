package prometheus

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/jinbangyi/solanaswap-go/pkg/goroutine"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
)

func Start() {
	promHandler := promhttp.Handler()
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/metrics", func(c *gin.Context) {
		// prometheus metrics 采集
		promHandler.ServeHTTP(c.Writer, c.Request)
	})

	r.GET("/", func(c *gin.Context) {
		// prometheus metrics 采集
		promHandler.ServeHTTP(c.Writer, c.Request)
	})

	srv := &http.Server{
		Addr:    ":9000",
		Handler: r,
	}
	goroutine.Go(func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("Prometheus server", zap.Error(err))
		}
		log.Info("Prometheus stopped")
	})
}
