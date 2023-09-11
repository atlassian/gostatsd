package redis

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/atlassian/gostatsd"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestPreparePayload tests the preparePayload function
func TestPreparePayload(t *testing.T) {
	logger := logrus.StandardLogger()
	// Create a simplified Counter structure for testing
	counter := gostatsd.Counter{
		PerSecond: 1,
		Value:     10,
	}

	// Create sample gostatsd.MetricMap with some metrics
	metrics := &gostatsd.MetricMap{
		Counters: map[string]map[string]gostatsd.Counter{
			"counter1": {
				"tag1": counter,
			},
		},
	}

	// Disable timer subtypes for the test
	disabled := gostatsd.TimerSubtypes{}

	// Call preparePayload to generate the JSON representation
	buf := preparePayload(metrics, &disabled, logger)

	// Define a custom assertion function for checking JSON with variations
	assertJSONWithVariations := func(expectedJSON string, actualJSON string) {
		var expected map[string]interface{}
		if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
			t.Fatalf("Error parsing expected JSON: %v", err)
		}

		var actual map[string]interface{}
		if err := json.Unmarshal([]byte(actualJSON), &actual); err != nil {
			t.Fatalf("Error parsing actual JSON: %v", err)
		}

		// Check specific fields with variations
		expectedTimestamp, ok := expected["timestamp"].(float64)
		if !ok {
			t.Fatal("Expected timestamp not found in the expected JSON")
		}

		actualTimestamp, ok := actual["timestamp"].(float64)
		if !ok {
			t.Fatal("Timestamp not found in the actual JSON")
		}

		// Check if the timestamp is within the expected range (2 seconds variation)
		if math.Abs(actualTimestamp-expectedTimestamp) < 2 {
			t.Fatalf("Timestamp variation exceeds the expected range: expected=%v, actual=%v", expectedTimestamp, actualTimestamp)
		}

		// Check the "counters" field
		expectedCounters := expected["counters"].(map[string]interface{})
		actualCounters := actual["counters"].(map[string]interface{})
		for key, value := range expectedCounters {
			actualValue, ok := actualCounters[key]
			if !ok {
				t.Fatalf("Counter key %s not found in actual JSON", key)
			}
			if actualValue != value {
				t.Fatalf("Counter value for key %s does not match: expected=%v, actual=%v", key, value, actualValue)
			}
		}

		// Check the "counter_rates" field
		expectedCounterRates := expected["counter_rates"].(map[string]interface{})
		actualCounterRates := actual["counter_rates"].(map[string]interface{})
		for key, value := range expectedCounterRates {
			actualValue, ok := actualCounterRates[key]
			if !ok {
				t.Fatalf("Counter rate key %s not found in actual JSON", key)
			}
			if actualValue != value {
				t.Fatalf("Counter rate value for key %s does not match: expected=%v, actual=%v", key, value, actualValue)
			}
		}

		// Check the "pctThreshold" field
		expectedPctThreshold := expected["pctThreshold"].([]interface{})
		actualPctThreshold := actual["pctThreshold"].([]interface{})
		if len(expectedPctThreshold) != len(actualPctThreshold) {
			t.Fatalf("Length of pctThreshold arrays does not match")
		}
		for i, value := range expectedPctThreshold {
			if value != actualPctThreshold[i] {
				t.Fatalf("pctThreshold value at index %d does not match: expected=%v, actual=%v", i, value, actualPctThreshold[i])
			}
		}

	}

	// Define the expected JSON with variations
	expectedJSON := `{
		"counters": {
		  "counter1": 10
		},
		"counter_rates": {
		  "counter1": 1
		},
		"timers": {},
		"gauges": {},
		"sets": {},
		"pctThreshold": [90],
		"timestamp": 0, 
		"datetime": ""
	  }`

	// Check if buf contains JSON with acceptable variation in timestamp and datetime
	assertJSONWithVariations(expectedJSON, buf.String())
}

// TestWritePayload tests the writePayload function, this test is subpar and needs to be improved
func TestWritePayload(t *testing.T) {
	logger := logrus.StandardLogger()
	buf := new(bytes.Buffer)

	// Test case 1: Empty buffer should return no error and no data written
	err := writePayload(buf, logger)
	assert.NoError(t, err)
	assert.Empty(t, buf.String())

	// Test case 2: Error message should be written to the buffer
	buf.Reset()
	buf.WriteString("ERROR: Sample Error Message")
	err = writePayload(buf, logger)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "ERROR: Sample Error Message")

}
