package main

import "runtime/debug"

func GetVersion() string {
	if build, ok := debug.ReadBuildInfo(); ok {
		return build.Main.Version
	}
	return "unknown"
}
