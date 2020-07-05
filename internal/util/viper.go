package util

import (
	"strings"

	"github.com/spf13/viper"
)

// EnvPrefix is the prefix of the inspected environment variables.
const EnvPrefix = "GSD" //Go Stats D

func GetSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	InitViper(n, key)
	return n
}

// InitViper sets up env var handling for a viper. This must be run on every created sub viper as these settings
// are not persisted to nested viper instances.
func InitViper(v *viper.Viper, subViperName string) {
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	if subViperName != "" {
		// Sub viper environment variables are accessed via <EnvPrefix>_<subViperName>_<varName>
		v.SetEnvPrefix(EnvPrefix + "_" + strings.ToUpper(subViperName))
	} else {
		v.SetEnvPrefix(EnvPrefix)
	}
	v.SetTypeByDefaultValue(true)
	v.AutomaticEnv()
}
