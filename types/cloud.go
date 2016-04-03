package types

// Instance represents a cloud instance.
type Instance struct {
	ID     string
	Region string
	Tags   Tags
}
