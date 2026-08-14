package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	eebusAdminSecretMinBytes = 32
	eebusAdminSecretMaxBytes = 256
)

func bindEEBusAdminFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	fs.BoolVar(&cfg.EEBusAdminConfig.Enabled, "eebus-admin-enabled", cfg.EEBusAdminConfig.Enabled, "enable the authenticated eeBUS owner/HA admin boundary")
	fs.StringVar(&cfg.EEBusAdminConfig.OwnerUsername, "eebus-admin-owner-username", cfg.EEBusAdminConfig.OwnerUsername, "owner username for the eeBUS admin boundary")
	fs.StringVar(&cfg.EEBusAdminConfig.OwnerSecretPath, "eebus-admin-owner-secret-file", cfg.EEBusAdminConfig.OwnerSecretPath, "protected owner credential file (regular 0600 file)")
	fs.StringVar(&cfg.EEBusAdminConfig.HASecretPath, "eebus-admin-ha-secret-file", cfg.EEBusAdminConfig.HASecretPath, "protected HA machine credential file (regular 0600 file)")
	fs.StringVar(&cfg.EEBusAdminConfig.OwnerOrigin, "eebus-admin-origin", cfg.EEBusAdminConfig.OwnerOrigin, "exact same-origin Portal origin for CSRF validation")
	fs.DurationVar(&cfg.EEBusAdminConfig.SessionTTL, "eebus-admin-session-ttl", cfg.EEBusAdminConfig.SessionTTL, "bounded owner session lifetime")
}

func loadEEBusAdminAuthConfig(config ebusgateway.EEBusAdminConfig) (eebusadmin.AuthConfig, error) {
	if !config.Enabled {
		return eebusadmin.AuthConfig{}, nil
	}
	if !validEEBusAdminUsername(config.OwnerUsername) {
		return eebusadmin.AuthConfig{}, errors.New("eeBUS admin owner username is invalid")
	}
	if !validEEBusAdminOrigin(config.OwnerOrigin) || config.SessionTTL <= 0 || config.SessionTTL > 24*time.Hour {
		return eebusadmin.AuthConfig{}, errors.New("eeBUS admin session boundary is invalid")
	}
	ownerSecret, err := readEEBusAdminSecret(config.OwnerSecretPath)
	if err != nil {
		return eebusadmin.AuthConfig{}, errors.New("eeBUS admin owner credential is unavailable")
	}
	haSecret, err := readEEBusAdminSecret(config.HASecretPath)
	if err != nil {
		return eebusadmin.AuthConfig{}, errors.New("eeBUS admin HA credential is unavailable")
	}
	if subtle.ConstantTimeCompare(ownerSecret, haSecret) == 1 {
		return eebusadmin.AuthConfig{}, errors.New("eeBUS admin credentials must be distinct")
	}
	return eebusadmin.AuthConfig{
		OwnerUsername: config.OwnerUsername,
		OwnerSecret:   ownerSecret,
		HASecret:      haSecret,
		OwnerOrigin:   strings.TrimSuffix(config.OwnerOrigin, "/"),
		SessionTTL:    config.SessionTTL,
		Now:           time.Now,
		Random:        rand.Reader,
	}, nil
}

func validEEBusAdminOrigin(value string) bool {
	origin, err := url.Parse(value)
	return err == nil && (origin.Scheme == "http" || origin.Scheme == "https") && origin.Host != "" &&
		origin.User == nil && origin.RawQuery == "" && origin.Fragment == "" && (origin.Path == "" || origin.Path == "/")
}

func readEEBusAdminSecret(path string) ([]byte, error) {
	if path == "" || strings.ContainsRune(path, '\x00') || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("credential path is invalid")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("open credential file")
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open credential file")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("credential file is not a protected regular file")
	}
	value, err := io.ReadAll(io.LimitReader(file, eebusAdminSecretMaxBytes+2))
	if err != nil {
		return nil, errors.New("read credential file")
	}
	if len(value) != 0 && value[len(value)-1] == '\n' {
		value = value[:len(value)-1]
	}
	if len(value) < eebusAdminSecretMinBytes || len(value) > eebusAdminSecretMaxBytes || !visibleASCII(value, false) {
		return nil, errors.New("credential value is invalid")
	}
	return append([]byte(nil), value...), nil
}

func validEEBusAdminUsername(value string) bool {
	return len(value) > 0 && len(value) <= 64 && visibleASCII([]byte(value), true)
}

func visibleASCII(value []byte, rejectColon bool) bool {
	for _, character := range value {
		if character < 0x21 || character > 0x7e || (rejectColon && character == ':') {
			return false
		}
	}
	return true
}

func validateEEBusAdminRuntimeConfig(config ebusgateway.Config) error {
	if !config.EEBusAdminConfig.Enabled {
		return nil
	}
	if !config.EEBusConfig.Enabled {
		return fmt.Errorf("eeBUS admin boundary requires enabled eeBUS runtime")
	}
	return nil
}

func startEEBusAdminAwareRuntime(ctx context.Context, config ebusgateway.Config) (*eebusRuntimeAdapter, eebusruntime.AdminV1, eebusadmin.AuthConfig, bool, error) {
	if config.EEBusAdminConfig.Enabled {
		if validationErr := validateEEBusAdminRuntimeConfig(config); validationErr != nil {
			log.Printf("eeBUS admin boundary unavailable reason=configuration")
		} else if auth, authErr := loadEEBusAdminAuthConfig(config.EEBusAdminConfig); authErr != nil {
			log.Printf("eeBUS admin boundary unavailable reason=credentials")
		} else {
			adapter, admin, runtimeErr := startEEBusOperatorRuntime(ctx, config.EEBusConfig, resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn)
			if runtimeErr == nil {
				return adapter, admin, auth, true, nil
			}
			log.Printf("eeBUS admin boundary unavailable reason=operator_runtime")
		}
	}
	adapter, err := startEEBusRuntime(ctx, config.EEBusConfig, resolveEEBusInterfaceAddressesFn, newEEBusRuntimeFn)
	if err != nil {
		return nil, nil, eebusadmin.AuthConfig{}, false, err
	}
	return adapter, nil, eebusadmin.AuthConfig{}, false, nil
}
