package main

import (
	"errors"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func validateM2MGraphQLConfig(config ebusgateway.M2MGraphQLConfig) error {
	if err := config.Validate(); err != nil {
		return errors.New("M2M GraphQL configuration is invalid")
	}
	return nil
}
