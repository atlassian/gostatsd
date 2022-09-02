package nodes

import (
	"context"
	"net"
)

// NodePicker is an interface for tracking and selecting nodes in a cluster.
// It does not manage expiry.
type NodePicker interface {
	// Run will run the node tracker until the context is closed.  The caller must
	// ensure that Run returns, and not just cancel the context, as the NodePicker
	// may have cleanup
	Run(ctx context.Context)

	// List returns a list of all nodes being tracked.  The list returned will be
	// a copy of the underlying list of nodes.  Intended for admin interfaces,
	// not performance critical code.  Thread safe.
	List() []string

	// Select will use the provided key to pick a node from the list of tracked
	// nodes and return it.
	//
	// self indicates if the returned node is the local node
	//
	// Returns an error if there are no nodes available.
	//
	// Thread safe.
	Select(key string) (node string, self bool, err error)

	// Add will add the node to the list of nodes tracked.  Thread safe.
	Add(node string)

	// Remove will remove the node from the list of nodes tracked. Thread safe.
	Remove(node string)
}

// NodeTracker will track a list of nodes, it is responsible for tracking life
// cycle updates, and will typically pass them on to a NodePicker.
type NodeTracker interface {
	// Run will run the node tracker until the context is closed.  The caller must
	// ensure that Run returns, and not just cancel the context, as the NodeTracker
	// may have cleanup.
	Run(ctx context.Context)
}

// LocalAddress is a helper function to return the local IP address that would
// be used to connect to a specified target.  Useful to get the IP that should
// be advertised externally.
func LocalAddress(target string) (net.IP, error) {
	conn, err := net.Dial("udp", target)
	if err != nil {
		return nil, err
	}

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	_ = conn.Close()
	return localAddr.IP, nil
}
