package httpsrv

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/validation"
)

var (
	errNoRoute             = errors.New("no such route")
	errNotReady            = errors.New("not ready")
	errShuttingDown        = errors.New("shutting down")
	errPreconditionFailed  = errors.New("precondition failed")
	errRangeNotSatisfiable = errors.New("range not satisfiable")
	errUploadTooLarge      = errors.New("upload too large")
	errUploadStalled       = errors.New("upload stalled")
	errTooManyUploads      = errors.New("too many concurrent uploads")
	errMalformedUpload     = errors.New("malformed multipart body")
)

// writeError is the one place errors become responses. It aborts the handler chain.
func (s *Server) writeError(c *gin.Context, err error) {
	status, body := s.mapError(c, err)
	c.AbortWithStatusJSON(status, body)
}

func (s *Server) mapError(c *gin.Context, err error) (int, apiv1.ErrorResponse) {
	if verr, ok := errors.AsType[*validation.Error](err); ok {
		return http.StatusBadRequest, apiv1.NewValidationError("invalid_request", "invalid request", verr)
	}
	switch {
	case errors.Is(err, asset.ErrNotFound):
		return http.StatusNotFound, apiv1.NewError("not_found", "no such asset")
	case errors.Is(err, errNoRoute):
		return http.StatusNotFound, apiv1.NewError("not_found", "no such route")
	case errors.Is(err, tenant.ErrUnauthenticated):
		c.Header("WWW-Authenticate", "Bearer")
		return http.StatusUnauthorized, apiv1.NewError("unauthorized", "missing or invalid API key")
	case errors.Is(err, asset.ErrIdempotencyKeyReused):
		return http.StatusUnprocessableEntity,
			apiv1.NewError("idempotency_key_reused", "idempotency key already used for a different upload")
	case errors.Is(err, errUploadTooLarge):
		return http.StatusRequestEntityTooLarge, apiv1.NewError("file_too_large", "upload exceeds the size limit")
	case errors.Is(err, errUploadStalled):
		return http.StatusRequestTimeout, apiv1.NewError("request_timeout", "timed out waiting for the upload")
	case errors.Is(err, errTooManyUploads):
		return http.StatusTooManyRequests, apiv1.NewError("too_many_uploads", "too many uploads in progress")
	case errors.Is(err, errMalformedUpload):
		return http.StatusBadRequest, apiv1.NewError("invalid_request", "malformed multipart body")
	case errors.Is(err, errPreconditionFailed):
		return http.StatusPreconditionFailed, apiv1.NewError("precondition_failed", "precondition failed")
	case errors.Is(err, errRangeNotSatisfiable):
		return http.StatusRequestedRangeNotSatisfiable,
			apiv1.NewError("range_not_satisfiable", "requested range not satisfiable")
	case errors.Is(err, errShuttingDown):
		return http.StatusServiceUnavailable, apiv1.NewError("unavailable", "not ready")
	case errors.Is(err, errNotReady):
		s.logger(c).Warn("readiness check failed", "error", err)
		return http.StatusServiceUnavailable, apiv1.NewError("unavailable", "not ready")
	}

	// A client that hung up cancels the request's context, and the error that follows isn't a server fault.
	level := slog.LevelError
	if c.Request.Context().Err() != nil {
		level = slog.LevelInfo
	}
	s.logger(c).Log(c.Request.Context(), level, "request failed", "error", err)
	return http.StatusInternalServerError, internalError()
}

func internalError() apiv1.ErrorResponse {
	return apiv1.NewError("internal", "internal server error")
}

func (s *Server) logger(c *gin.Context) *slog.Logger {
	return logging.LoggerFromContextOr(c.Request.Context(), s.log)
}
