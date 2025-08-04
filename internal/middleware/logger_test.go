package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)
	handler := Logger(baseLogger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zerolog.Ctx(r.Context()).Info().Msg("ok")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if !strings.Contains(buf.String(), "\"message\":\"ok\"") {
		t.Fatalf("log message not written: %s", buf.String())
	}
}
