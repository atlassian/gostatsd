package providers

import (
	_ "github.com/jtblin/gostatsd/cloudprovider/providers/aws" // imports provider to avoid cycle error
)
