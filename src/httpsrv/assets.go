package httpsrv

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
	w := &serveContentWriter{ResponseWriter: c.Writer, s: s, c: c}
	http.ServeContent(w, c.Request, "", r.CreatedAt, f)
	w.flush()
}

// serveContentWriter turns the error responses http.ServeContent writes itself (412, 416 and 500)
// into the API's error body.
type serveContentWriter struct {
	http.ResponseWriter
	s      *Server
	c      *gin.Context
	failed bool
	status int
}

const maxCauseLen = 200

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
	switch status {
	case http.StatusPreconditionFailed:
		w.s.writeError(w.c, errPreconditionFailed)
	case http.StatusRequestedRangeNotSatisfiable:
		w.s.writeError(w.c, errRangeNotSatisfiable)
	default:
		// http.Error writes the cause after the status, so the error is reported on the first Write.
		w.status = status
	}
}

func (w *serveContentWriter) report(cause string) {
	status := w.status
	w.status = 0
	if len(cause) > maxCauseLen {
		cause = cause[:maxCauseLen]
	}
	w.s.writeError(w.c, fmt.Errorf("error serving rendition: status %d: %s", status, strings.TrimSpace(cause)))
}

func (w *serveContentWriter) flush() {
	if w.status != 0 {
		w.report("")
	}
}

func (w *serveContentWriter) Write(p []byte) (int, error) {
	if w.failed {
		if w.status != 0 {
			w.report(string(p))
		}
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}
