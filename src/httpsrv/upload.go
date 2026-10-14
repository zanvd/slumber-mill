package httpsrv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/validation"
)

const (
	DefaultMaxUploadBytes      int64 = 2 << 30
	DefaultMaxUploadsPerTenant       = 4

	// uploadIdleTimeout is how long an upload may go without the server receiving any of it.
	uploadIdleTimeout = 30 * time.Second
	// maxTitleBytes bounds the title part, which is read into memory; the service enforces the real limit.
	maxTitleBytes = 4 << 10
)

func (s *Server) createAsset(c *gin.Context) {
	if c.Request.ContentLength > s.maxUploadBytes {
		s.rejectUpload(c, errUploadTooLarge)
		return
	}
	t := tenantFrom(c)
	if !s.uploads.acquire(t.ID) {
		s.rejectUpload(c, errTooManyUploads)
		return
	}
	defer s.uploads.release(t.ID)

	rc := http.NewResponseController(c.Writer)
	if err := rc.SetReadDeadline(time.Now().Add(uploadIdleTimeout)); err != nil {
		s.rejectUpload(c, fmt.Errorf("error setting the upload's read deadline: %w", err))
		return
	}
	body := &uploadBody{ReadCloser: http.MaxBytesReader(c.Writer, c.Request.Body, s.maxUploadBytes), rc: rc}
	c.Request.Body = body

	ctx := c.Request.Context()
	var form uploadForm
	if err := s.readForm(ctx, c.Request, body, &form); err != nil {
		if form.staged {
			s.assets.Discard(context.WithoutCancel(ctx), form.upload)
		}
		s.rejectUpload(c, err)
		return
	}

	a, replayed, err := s.assets.Create(ctx, asset.CreateParams{
		TenantID:       t.ID,
		TargetLUFS:     t.TargetLUFS,
		Title:          form.title,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
		Upload:         form.upload,
	})
	if err != nil {
		s.writeError(c, err)
		return
	}
	c.Header("Location", "/v1/assets/"+a.ID.String())
	if replayed {
		c.Header("Idempotent-Replayed", "true")
	}
	c.JSON(http.StatusAccepted, apiv1.NewAsset(a))
}

// rejectUpload answers before the body has been read to its end. It closes the connection,
// so that net/http doesn't wait to drain the rest of a body that may never arrive.
func (s *Server) rejectUpload(c *gin.Context, err error) {
	c.Header("Connection", "close")
	s.writeError(c, err)
}

type uploadForm struct {
	title  string
	upload asset.Upload
	// staged reports that upload holds a staged file, which must be passed to Create or Discard.
	staged bool
}

// readForm walks the multipart body into form: it reads the title, streams the file part to Stage
// and skips any other part. It returns once the whole body has been read.
func (s *Server) readForm(ctx context.Context, r *http.Request, body *uploadBody, form *uploadForm) error {
	mr, err := r.MultipartReader()
	if err != nil {
		var v validation.Collector
		v.Add("Content-Type", "must be multipart/form-data")
		return v.Err()
	}
	titled := false
	for {
		part, err := mr.NextPart()
		// NextPart wraps io.EOF when the body ends where a boundary should be, which is no clean end.
		if err == io.EOF {
			break
		}
		if err != nil {
			return body.cause(err)
		}
		switch part.FormName() {
		case "title":
			if titled {
				var v validation.Collector
				v.Add("title", "must be sent once")
				return v.Err()
			}
			titled = true
			b, err := io.ReadAll(io.LimitReader(part, maxTitleBytes+1))
			if err != nil {
				return body.cause(err)
			}
			if len(b) > maxTitleBytes {
				var v validation.Collector
				v.Add("title", "must be at most 255 characters")
				return v.Err()
			}
			form.title = string(b)
		case "file":
			if form.staged {
				var v validation.Collector
				v.Add("file", "must be sent once")
				return v.Err()
			}
			pr := &errReader{r: part}
			u, err := s.assets.Stage(ctx, pr)
			if err != nil {
				if pr.err != nil || body.err != nil {
					return body.cause(pr.err)
				}
				return err
			}
			form.upload, form.staged = u, true
		default:
			if _, err := io.Copy(io.Discard, part); err != nil {
				return body.cause(err)
			}
		}
	}
	if !form.staged {
		var v validation.Collector
		v.Add("file", "is required")
		return v.Err()
	}
	return body.finish()
}

// uploadBody limits the request body and pushes its idle-read deadline forward on every read.
// It records why reading failed, so the client's side of a failure is answered for
// before the service's error is mapped.
type uploadBody struct {
	io.ReadCloser
	rc  *http.ResponseController
	err error
}

func (b *uploadBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.fail(bodyError(err))
		return n, err
	}
	if n > 0 {
		if derr := b.rc.SetReadDeadline(time.Now().Add(uploadIdleTimeout)); derr != nil {
			derr = fmt.Errorf("error extending the upload's read deadline: %w", derr)
			b.fail(derr)
			return n, derr
		}
	}
	return n, err
}

func (b *uploadBody) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// cause says why reading the upload stopped: the body's own failure when it had one,
// or else err, which the multipart parser returned for a body it couldn't parse.
func (b *uploadBody) cause(err error) error {
	if b.err != nil {
		return b.err
	}
	return fmt.Errorf("%w: %w", errMalformedUpload, err)
}

// finish reads the body to its end, past any multipart epilogue, and lifts the deadline.
// Once the body is read, net/http watches the connection for the client going away,
// and the deadline expiring there would cancel the request's context while the asset is created.
func (b *uploadBody) finish() error {
	if _, err := io.Copy(io.Discard, b); err != nil {
		return b.cause(err)
	}
	if err := b.rc.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("error clearing the upload's read deadline: %w", err)
	}
	return nil
}

func bodyError(err error) error {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return fmt.Errorf("%w: %w", errUploadTooLarge, err)
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%w: %w", errUploadStalled, err)
	}
	return fmt.Errorf("%w: %w", errMalformedUpload, err)
}

// errReader records the first error other than io.EOF that r returns.
type errReader struct {
	r   io.Reader
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && err != io.EOF && e.err == nil {
		e.err = err
	}
	return n, err
}

// uploadSlots counts each tenant's uploads in progress against a cap.
type uploadSlots struct {
	mu    sync.Mutex
	limit int
	inUse map[uint64]int
}

func newUploadSlots(limit int) *uploadSlots {
	return &uploadSlots{limit: limit, inUse: make(map[uint64]int)}
}

// acquire takes one of the tenant's slots, or reports false at once when they are all taken.
func (u *uploadSlots) acquire(tenantID uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.inUse[tenantID] >= u.limit {
		return false
	}
	u.inUse[tenantID]++
	return true
}

func (u *uploadSlots) release(tenantID uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.inUse[tenantID]--
	if u.inUse[tenantID] <= 0 {
		delete(u.inUse, tenantID)
	}
}
