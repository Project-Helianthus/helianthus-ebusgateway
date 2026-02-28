package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := ebusgateway.SmokeOptions{
		GraphQLCheck: smokeGraphQLCheck,
	}

	if err := ebusgateway.RunSmokeFromEnv(ctx, opts); err != nil {
		log.Fatalf("smoke: %v", err)
	}
}

func smokeGraphQLCheck(ctx context.Context, gateway *ebusgateway.Gateway) ebusgateway.SmokeCheckResult {
	if gateway == nil || gateway.Registry == nil {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: "graphql check missing gateway registry",
		}
	}

	builder := graphql.NewBuilder(gateway.Registry, nil)
	if err := builder.Start(ctx); err != nil {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: fmt.Sprintf("graphql builder start: %v", err),
		}
	}
	handler, err := graphql.NewHandler(builder)
	if err != nil {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: fmt.Sprintf("graphql handler build: %v", err),
		}
	}

	const payload = `{"query":"{ __typename }"}`
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: fmt.Sprintf("graphql status %d: %s", recorder.Code, strings.TrimSpace(recorder.Body.String())),
		}
	}

	var response struct {
		Data struct {
			Typename string `json:"__typename"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: fmt.Sprintf("graphql decode: %v", err),
		}
	}
	if len(response.Errors) > 0 {
		message := strings.TrimSpace(response.Errors[0].Message)
		if message == "" {
			message = "unknown graphql error"
		}
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: message,
		}
	}
	if strings.TrimSpace(response.Data.Typename) == "" {
		return ebusgateway.SmokeCheckResult{
			OK:    false,
			Error: "graphql response missing __typename",
		}
	}

	return ebusgateway.SmokeCheckResult{
		OK:      true,
		Details: fmt.Sprintf("query={__typename} typename=%s", response.Data.Typename),
	}
}
