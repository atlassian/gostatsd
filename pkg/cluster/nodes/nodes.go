package nodes

import (
	"context"
	"net"
)

// A magic identifier to indicate the node selected is the current node
const NodeNameSelf = "__SELF__"

// NodePicker is an interface to pick a tracked node in a cluster given a key.  It does not manage expiry.
type NodePicker interface {
	// Runs the node tracker until the context is closed.
	Run(ctx context.Context)

	// List returns a list of all nodes being tracked.  The list returned will be
	// a copy of the underlying list of nodes.  Intended for admin interfaces,
	// not performance critical code.  Thread safe.
	List() []string

	// Select will use the provided key to pick a node from the list of tracked
	// nodes and return it.
	//
	// Returns an error if there are no nodes available.
	// Returns NodeNameSelf if the selected node is the current node.
	//
	// Thread safe.
	Select(key string) (string, error)

	// Add will add the node to the list of nodes tracked.  Thread safe.
	Add(node string)

	// Remove will remove the node from the list of nodes tracked. Thread safe.
	Remove(node string)
}

// LocalAddress is a helper function to return the local IP address that would
// be used to connect to a specified target.  Useful to get the IP that should
// be advertised externally.
func LocalAddress(target string) (net.IP, error) {
	// Mostly lifted from https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
	conn, err := net.Dial("udp", target)
	if err != nil {
		return nil, err
	}

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	_ = conn.Close()
	return localAddr.IP, nil
}
