// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryconfig

type GrpcConfig struct {
	EndpointAddrs      []string     `yaml:"endpoint_addresses"`
	EndpointGroupAddrs []string     `yaml:"endpoint_group_addresses"`
	ClientConfig       ClientConfig `yaml:"client_config"`
}

type ClientConfig struct {
	Secure          bool   `yaml:"secure"`
	SkipVerify      bool   `yaml:"skip_verify"`
	Cert            string `yaml:"cert"`
	Key             string `yaml:"key"`
	CaCert          string `yaml:"ca_cert"`
	ServerName      string `yaml:"server_name"`
	GrpcCompression string `yaml:"grpc_compression"`
}
