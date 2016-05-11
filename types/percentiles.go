package types

import (
	"bytes"
	"fmt"
	"strings"
)

// Percentile is used to store the aggregation for a percentile.
type Percentile struct {
	float float64
	str   string
}

// Percentiles represents an array of percentiles.
type Percentiles []*Percentile

// Set append a percentile aggregation to the percentiles.
func (p *Percentiles) Set(s string, f float64) {
	*p = append(*p, &Percentile{f, strings.Replace(s, ".", "_", -1)})
}

// String returns the string value of percentiles.
func (p *Percentiles) String() string {
	buf := new(bytes.Buffer)
	for _, pct := range *p {
		fmt.Fprintf(buf, "%s:%f ", pct.String(), pct.Float())
	}
	return buf.String()
}

// String returns the string value of a percentile.
func (p *Percentile) String() string {
	return p.str
}

// Float returns the float value of a percentile.
func (p *Percentile) Float() float64 {
	return p.float
}
