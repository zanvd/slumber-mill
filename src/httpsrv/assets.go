package httpsrv

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/validation"
)

func (s *Server) getAsset(c *gin.Context) {
	a, err := s.assets.Get(c.Request.Context(), tenantFrom(c).ID, c.Param("id"))
	if err != nil {
		s.writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, apiv1.NewAsset(a))
}

func (s *Server) listAssets(c *gin.Context) {
	limit := asset.DefaultListLimit
	if raw, ok := c.GetQuery("limit"); ok {
		n, err := strconv.Atoi(raw)
		if err != nil {
			var v validation.Collector
			v.Add("limit", "must be an integer between 1 and 100")
			s.writeError(c, v.Err())
			return
		}
		limit = n
	}
	page, err := s.assets.List(c.Request.Context(), tenantFrom(c).ID, limit, c.Query("cursor"))
	if err != nil {
		s.writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, apiv1.NewAssetList(page))
}

func (s *Server) getRendition(c *gin.Context) {
	r, f, err := s.assets.OpenRendition(c.Request.Context(), tenantFrom(c).ID, c.Param("id"), c.Param("profile"))
	if err != nil {
		s.writeError(c, err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			s.logger(c).Warn("error closing rendition", "error", err)
		}
	}()

	h := c.Writer.Header()
	h.Set("Content-Type", "audio/mp4")
	h.Set("ETag", `"`+hex.EncodeToString(r.SHA256[:])+`"`)
	h.Set("Cache-Control", "private, max-age=31536000, immutable")
	http.ServeContent(&serveContentWriter{ResponseWriter: c.Writer, s: s, c: c}, c.Request, "", r.CreatedAt, f)
}

// serveContentWriter turns the error responses http.ServeContent writes itself (412, 416 and 500)
// into the API's error body.
type serveContentWriter struct {
	http.ResponseWriter
	s      *Server
	c      *gin.Context
	failed bool
}

func (w *serveContentWriter) WriteHeader(status int) {
	if status < http.StatusBadRequest {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.failed = true
	h := w.Header()
	for _, k := range []string{"Content-Type", "Content-Length", "Cache-Control", "ETag", "Last-Modified"} {
		h.Del(k)
	}
	var err error
	switch status {
	case http.StatusPreconditionFailed:
		err = errPreconditionFailed
	case http.StatusRequestedRangeNotSatisfiable:
		err = errRangeNotSatisfiable
	default:
		err = fmt.Errorf("error serving rendition: status %d", status)
	}
	w.s.writeError(w.c, err)
}

func (w *serveContentWriter) Write(p []byte) (int, error) {
	if w.failed {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}
