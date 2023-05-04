// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryconfig

import (
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"gopkg.in/yaml.v2"
)

// Config is a structure that allows pointing to various HTTP and GRCP query endpoints, e.g ruler connecting to queriers.
type Config struct {
	HTTPConfig httpconfig.Config `yaml:",inline"`
	GRPCConfig GrpcConfig        `yaml:"grpc_config"`
}

func DefaultConfig() Config {
	return Config{
		HTTPConfig: httpconfig.DefaultConfig(),
		GRPCConfig: GrpcConfig{
			EndpointAddrs:      []string{},
			EndpointGroupAddrs: []string{},
			ClientConfig:       ClientConfig{},
		},
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig()
	type plain Config
	return unmarshal((*plain)(c))
}

// LoadConfigs loads a list of Config from YAML data.
func LoadConfigs(confYAML []byte) ([]Config, error) {
	var queryCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, &queryCfg); err != nil {
		return nil, err
	}
	return queryCfg, nil
}

// BuildConfig returns a configuration from a static addresses.
func BuildConfig(addrs []string) ([]Config, error) {
	httpConfigs, err := httpconfig.BuildConfig(addrs)
	if err != nil {
		return nil, err
	}
	configs := make([]Config, 0, len(addrs))
	for _, c := range httpConfigs {
		configs = append(configs, Config{
			HTTPConfig: c,
		})
	}
	return configs, nil
}
