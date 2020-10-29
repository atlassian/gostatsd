// +build gofuzz

package lexer

import (
	"fmt"

	"github.com/atlassian/gostatsd/internal/pool"
)

func Fuzz(data []byte) int {
	l := Lexer{
		MetricPool: pool.NewMetricPool(0),
	}
	metric, event, err := l.run(data, "")
	if err != nil {
		return 0
	}
	if (metric != nil && event == nil) || (metric == nil && event != nil) {
		return 1
	}
	// Either both are nil or both are not nil
	panic(fmt.Errorf("metric: %+v\nevent: %+v", metric, event))
}
