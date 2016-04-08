package providers

import (
	_ "github.com/atlassian/gostatsd/cloudprovider/providers/aws" // imports provider to avoid cycle error
)
