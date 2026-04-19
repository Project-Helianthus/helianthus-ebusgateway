// Package ebus_standard implements the gateway MCP surfaces for the
// ebus_standard L7 namespace (M4_GATEWAY_MCP). RED stub — impl lands in
// the next commit.
package ebus_standard

import (
	"errors"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

const (
	ToolServicesList = "stub.services.list"
	ToolCommandsList = "stub.commands.list"
	ToolCommandGet   = "stub.command.get"
	ToolDecode       = "stub.decode"
)

var ErrUnknownCommand = errors.New("ebus_standard: unknown command (stub)")
var ErrInvalidPayload = errors.New("ebus_standard: invalid payload (stub)")

type Server struct{}

func NewServer(_ ebusstd.Catalog) *Server { return &Server{} }

func (s *Server) ServicesList() map[string]any { return nil }

func (s *Server) CommandsList(_ *uint8) (map[string]any, error) { return nil, ErrUnknownCommand }

func (s *Server) CommandGet(_ string) (map[string]any, error) { return nil, ErrUnknownCommand }

type DecodeInput struct {
	PB         uint8
	SB         uint8
	Direction  string
	FrameType  string
	PayloadHex string
}

func (s *Server) Decode(_ DecodeInput) (map[string]any, error) { return nil, ErrUnknownCommand }
