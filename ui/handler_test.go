package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerIndex(t *testing.T) {
	handler := NewHandler("/graphql")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), "/graphql") {
		t.Fatalf("index missing graphql path")
	}
}

func TestHandlerStatic(t *testing.T) {
	handler := NewHandler("/graphql")
	req := httptest.NewRequest(http.MethodGet, "/style.css", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), "body") {
		t.Fatalf("style.css missing expected content")
	}
}
