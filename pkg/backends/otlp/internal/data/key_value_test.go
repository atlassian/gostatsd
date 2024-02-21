package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestNewKeyValue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		value   string
		inserts []string
		expect  *v1common.KeyValue
	}{
		{
			name:  "Single Value",
			value: "my-awesome-service",
			expect: &v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			},
		},
		{
			name:  "Repeated Values",
			value: "my-awesome-service",
			inserts: []string{
				"my-awesome-service",
				"my-awesome-service",
			},
			expect: &v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			},
		},
		{
			name:  "Different Values",
			value: "my-awesome-service",
			inserts: []string{
				"not-so-awesome-service",
				"my-local-service",
			},
			expect: &v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_ArrayValue{
						ArrayValue: &v1common.ArrayValue{
							Values: []*v1common.AnyValue{
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-awesome-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-local-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "not-so-awesome-service",
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			kv := newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: tc.value,
					},
				},
			})
			for _, v := range tc.inserts {
				kv.InsertValue(v)
			}
			assert.Equal(t, tc.expect.Value, kv.raw.Value, "Must match the expected value")
		})
	}
}

func TestKeyValueCompare(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		a, b KeyValue
		cmp  int
	}{
		{
			name: "Same entries",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			}),
			cmp: 0,
		},
		{
			name: "Different Values",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-other-service",
					},
				},
			}),
			cmp: -1,
		},
		{
			name: "Inverse Different Values",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-other-service",
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-awesome-service",
					},
				},
			}),
			cmp: 1,
		},
		{
			name: "Mismatched Types Values",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-other-service",
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_ArrayValue{
						ArrayValue: &v1common.ArrayValue{
							Values: []*v1common.AnyValue{
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-local-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-test-service",
									},
								},
							},
						},
					},
				},
			}),
			cmp: -1,
		},
		{
			name: "Inverse Mismatched Types Values",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_ArrayValue{
						ArrayValue: &v1common.ArrayValue{
							Values: []*v1common.AnyValue{
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-local-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-test-service",
									},
								},
							},
						},
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_StringValue{
						StringValue: "my-other-service",
					},
				},
			}),
			cmp: 1,
		},
		{
			name: "Array Types Match",
			a: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_ArrayValue{
						ArrayValue: &v1common.ArrayValue{
							Values: []*v1common.AnyValue{
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-local-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-test-service",
									},
								},
							},
						},
					},
				},
			}),
			b: newKeyValue(&v1common.KeyValue{
				Key: "service.name",
				Value: &v1common.AnyValue{
					Value: &v1common.AnyValue_ArrayValue{
						ArrayValue: &v1common.ArrayValue{
							Values: []*v1common.AnyValue{
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-local-service",
									},
								},
								{
									Value: &v1common.AnyValue_StringValue{
										StringValue: "my-test-service",
									},
								},
							},
						},
					},
				},
			}),
			cmp: 0,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, 0, tc.a.Compare(tc.a), "Must be equal to itself")
			assert.Equal(t, 0, tc.b.Compare(tc.b), "Must be equal to itself")
			assert.Equal(t, tc.cmp, tc.a.Compare(tc.b), "Must be equal to the expected value")
		})
	}
}
