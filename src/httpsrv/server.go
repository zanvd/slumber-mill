package httpsrv

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/tenant"
)

const readyTimeout = 2 * time.Second

type tenantService interface {
	Authenticate(ctx context.Context, apiKey string) (tenant.Tenant, error)
}

type assetService interface {
	Stage(ctx context.Context, r io.Reader) (asset.Upload, error)
	Discard(ctx context.Context, u asset.Upload)
	Create(ctx context.Context, p asset.CreateParams) (a asset.Asset, replayed bool, err error)
	Get(ctx context.Context, tenantID uint64, id string) (asset.Asset, error)
	List(ctx context.Context, tenantID uint64, limit int, cursor string) (asset.Page, error)
	OpenRendition(ctx context.Context, tenantID uint64, id, profile string) (asset.Rendition, io.ReadSeekCloser, error)
}

type pinger interface {
	PingContext(ctx context.Context) error
}

type Config struct {
	// TrustedProxies are the CIDRs or addresses whose X-Forwarded-For is believed. Empty trusts none.
	TrustedProxies []string
	// Readiness is pinged by /readyz; *sql.DB satisfies it.
	Readiness pinger
	// MaxUploadBytes caps an upload's whole request body. Zero means DefaultMaxUploadBytes.
	MaxUploadBytes int64
	// MaxUploadsPerTenant caps each tenant's concurrent uploads. Zero means DefaultMaxUploadsPerTenant.
	MaxUploadsPerTenant int
}

type Server struct {
	log            *slog.Logger
	tenants        tenantService
	assets         assetService
	readiness      pinger
	maxUploadBytes int64
	uploads        *uploadSlots
	engine         *gin.Engine
	draining       atomic.Bool
}

func NewServer(log *slog.Logger, tenants tenantService, assets assetService, cfg Config) (*Server, error) {
	if log == nil || tenants == nil || assets == nil || cfg.Readiness == nil {
		return nil, errors.New("error creating HTTP server: logger, services and readiness check are required")
	}
	if cfg.MaxUploadBytes < 0 || cfg.MaxUploadsPerTenant < 0 {
		return nil, errors.New("error creating HTTP server: upload limits must not be negative")
	}
	s := &Server{
		log:            log,
		tenants:        tenants,
		assets:         assets,
		readiness:      cfg.Readiness,
		maxUploadBytes: cmp.Or(cfg.MaxUploadBytes, DefaultMaxUploadBytes),
		uploads:        newUploadSlots(cmp.Or(cfg.MaxUploadsPerTenant, DefaultMaxUploadsPerTenant)),
		engine:         gin.New(),
	}
	// Gin trusts every proxy until this is called; nil trusts none.
	if err := s.engine.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return nil, fmt.Errorf("error setting trusted proxies: %w", err)
	}

	s.engine.Use(s.logRequest, s.recover)
	s.engine.NoRoute(s.noRoute)
	s.engine.GET("/healthz", s.healthz)
	s.engine.GET("/readyz", s.readyz)

	v1 := s.engine.Group("/v1", s.authenticate)
	v1.POST("/assets", s.createAsset)
	v1.GET("/assets", s.listAssets)
	v1.GET("/assets/:id", s.getAsset)
	v1.GET("/assets/:id/renditions/:profile", s.getRendition)
	return s, nil
}

func (s *Server) Handler() http.Handler {
	return releaseUnreadBody(s.engine)
}

// BeginShutdown makes /readyz fail while every other route keeps serving, so a load balancer
// can take the node out before the listener closes.
func (s *Server) BeginShutdown() {
	s.draining.Store(true)
}

func (s *Server) healthz(c *gin.Context) {
	c.Status(http.StatusOK)
}

func (s *Server) readyz(c *gin.Context) {
	if s.draining.Load() {
		s.writeError(c, errShuttingDown)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), readyTimeout)
	defer cancel()
	if err := s.readiness.PingContext(ctx); err != nil {
		s.writeError(c, fmt.Errorf("%w: %w", errNotReady, err))
		return
	}
	c.Status(http.StatusOK)
}

func (s *Server) noRoute(c *gin.Context) {
	s.writeError(c, errNoRoute)
}
