//go:build integration
// +build integration

package dynamo

import "flag"

var flDSN = flag.String("dsn", "", "DSN of test dynamo instance")
