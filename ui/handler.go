package ui

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var assets embed.FS

var staticFS = func() fs.FS {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
	return sub
}()

var indexTemplate = template.Must(template.ParseFS(assets, "static/index.html"))

type handler struct {
	graphqlPath string
	files       http.Handler
}

func NewHandler(graphqlPath string) http.Handler {
	if graphqlPath == "" {
		graphqlPath = "/graphql"
	}
	return &handler{
		graphqlPath: graphqlPath,
		files:       http.FileServer(http.FS(staticFS)),
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "request missing", http.StatusBadRequest)
		return
	}
	if r.URL == nil {
		http.Error(w, "request url missing", http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	if path == "/" || strings.EqualFold(path, "/index.html") {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = indexTemplate.Execute(w, map[string]any{
			"GraphQLPath": h.graphqlPath,
		})
		return
	}
	h.files.ServeHTTP(w, r)
}
