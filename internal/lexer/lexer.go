package lexer

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/pool"
)

type Lexer struct {
	// any field added must be considered in Lexer.reset
	input         []byte
	len           uint32
	start         uint32
	pos           uint32
	eventTitleLen uint32
	eventTextLen  uint32
	m             *gostatsd.Metric
	e             *gostatsd.Event
	tags          gostatsd.Tags
	namespace     string
	err           error
	sampling      float64

	MetricPool *pool.MetricPool
}

// assumes we don't have \x00 bytes in input.
const eof byte = 0

var (
	errMissingKeySep     = errors.New("missing key separator")
	errEmptyKey          = errors.New("key zero len")
	errMissingValueSep   = errors.New("missing value separator")
	errInvalidType       = errors.New("invalid type")
	errInvalidFormat     = errors.New("invalid format")
	errInvalidAttributes = errors.New("invalid event attributes")
	errOverflow          = errors.New("overflow")
	errNotEnoughData     = errors.New("not enough data")
	errNaN               = errors.New("invalid value NaN")
)

var escapedNewline = []byte("\\n")
var newline = []byte("\n")

var priorityNormal = []byte("normal")
var priorityLow = []byte("low")

var alertInfo = []byte("info")
var alertError = []byte("error")
var alertWarning = []byte("warning")
var alertSuccess = []byte("success")

func (l *Lexer) next() byte {
	if l.pos >= l.len {
		return eof
	}
	b := l.input[l.pos]
	l.pos++
	return b
}

func (l *Lexer) reset() {
	// l.input = nil       // re-initialized by Run
	// l.len = 0           // re-initialized by Run
	// l.eventTitleLen = 0 // re-initialized by lexDatadogSpecial before lexEventBody
	// l.eventTextLen = 0  // re-initialized by lexDatadogSpecial before lexEventBody
	// l.namespace = ""    // re-initialized by Run
	// l.sampling = 1      // re-initialized by Run

	l.start = 0
	l.pos = 0
	l.m = nil
	l.e = nil
	l.tags = nil
	l.err = nil
}

func (l *Lexer) appendTag(data []byte) {
	if len(data) > 0 {
		l.tags = append(l.tags, string(data))
	}
}

func (l *Lexer) Run(input []byte, namespace string) (*gostatsd.Metric, *gostatsd.Event, error) {
	l.reset()
	l.input = input
	l.namespace = namespace
	l.len = uint32(len(l.input))
	l.sampling = float64(1)

	for state := lexSpecial; state != nil; {
		state = state(l)
	}
	if l.err != nil {
		return nil, nil, l.err
	}
	if l.m != nil {
		l.m.Rate = l.sampling
		if l.m.Type != gostatsd.SET {
			// Count number of values by checking colons to preallocate array
			var values []float64
			if l.m.StringValue == "" {
				values = make([]float64, 0, 0)
			} else {
				count := 1
				for i := 0; i < len(l.m.StringValue); i++ {
					if l.m.StringValue[i] == ':' {
						count++
					}
				}
				values = make([]float64, 0, count)
			}
			for _, stringValue := range strings.Split(l.m.StringValue, ":") {
				if stringValue == "" {
					// SKip the value, it could be something like a.packing:1:2:|ms|#|:|c:xyz
					continue
				}
				v, err := strconv.ParseFloat(stringValue, 64)
				if err != nil {
					return nil, nil, err
				}
				if math.IsNaN(v) {
					return nil, nil, errNaN
				}
				values = append(values, v)
			}
			l.m.Values = values
			l.m.StringValue = ""
		}
		l.m.Tags = l.tags
	} else {
		l.e.Tags = l.tags
	}
	return l.m, l.e, nil
}

type stateFn func(*Lexer) stateFn

// check the first byte for special Datadog type.
func lexSpecial(l *Lexer) stateFn {
	switch b := l.next(); b {
	case '_':
		return lexDatadogSpecial
	case eof:
		l.err = errInvalidType
		return nil
	default:
		l.pos--
		l.m = l.MetricPool.Get()
		// Pull the tags from the metric, because it may have an empty buffer we can reuse.
		l.tags = l.m.Tags
		return lexKeySep
	}
}

// lex until we find the colon separator between key and value.
func lexKeySep(l *Lexer) stateFn {
	for {
		switch b := l.next(); b {
		case '/':
			l.input[l.pos-1] = '-'
		case ' ', '\t':
			l.input[l.pos-1] = '_'
		case ':':
			return lexKey
		case eof:
			l.err = errMissingKeySep
			return nil
		case '.', '-', '_':
			continue
		default:
			r := rune(b)
			if (97 <= r && 122 >= r) || (65 <= r && 90 >= r) || (48 <= r && 57 >= r) {
				continue
			}
			l.input = append(l.input[0:l.pos-1], l.input[l.pos:]...)
			l.len--
			l.pos--
		}
	}
}

// lex Datadog special type.
func lexDatadogSpecial(l *Lexer) stateFn {
	switch b := l.next(); b {
	// _e{title.length,text.length}:title|text|d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2
	case 'e':
		l.e = new(gostatsd.Event)
		return lexAssert('{',
			lexUint32(&l.eventTitleLen,
				lexAssert(',',
					lexUint32(&l.eventTextLen,
						lexAssert('}', lexAssert(':', lexEventBody))))))
	default:
		l.err = errInvalidType
		return nil
	}
}

func lexEventBody(l *Lexer) stateFn {
	if l.len-l.pos < l.eventTitleLen+1+l.eventTextLen {
		l.err = errNotEnoughData
		return nil
	}
	if l.input[l.pos+l.eventTitleLen] != '|' {
		l.err = errInvalidFormat
		return nil
	}
	l.e.Title = string(l.input[l.pos : l.pos+l.eventTitleLen])
	l.pos += l.eventTitleLen + 1
	l.e.Text = string(bytes.Replace(l.input[l.pos:l.pos+l.eventTextLen], escapedNewline, newline, -1))
	l.pos += l.eventTextLen
	return lexEventAttributes
}

// lexEventAttributes lex through any event attributes separated by '|'
func lexEventAttributes(l *Lexer) stateFn {
	switch b := l.next(); b {
	case '|':
		return lexEventAttribute
	case eof:
	default:
		l.err = errInvalidAttributes
	}
	return nil
}

// lexEventAttribute
func lexEventAttribute(l *Lexer) stateFn {
	// d:date_happened|h:hostname|p:priority|t:alert_type|#tag1,tag2|c:container
	switch b := l.next(); b {
	case 'd':
		return lexAssert(':', lexUint(func(l *Lexer, value uint64) stateFn {
			if value > math.MaxInt64 {
				l.err = errOverflow
				return nil
			}
			l.e.DateHappened = int64(value)
			return lexEventAttributes
		}))
	case 'h':
		return lexAssert(':', func(lexer *Lexer) stateFn {
			data := seekUntil(l, '|')
			l.e.Source = gostatsd.Source(data)
			return lexEventAttributes
		})
	case 'k':
		return lexAssert(':', func(lexer *Lexer) stateFn {
			data := seekUntil(l, '|')
			l.e.AggregationKey = string(data)
			return lexEventAttributes
		})
	case 'p':
		return lexAssert(':', func(lexer *Lexer) stateFn {
			data := seekUntil(l, '|')
			if bytes.Equal(data, priorityLow) {
				l.e.Priority = gostatsd.PriLow
			} else if bytes.Equal(data, priorityNormal) {
				// Normal is default
			} else {
				l.err = errInvalidAttributes
				return nil
			}
			return lexEventAttributes
		})
	case 's':
		return lexAssert(':', func(lexer *Lexer) stateFn {
			data := seekUntil(l, '|')
			l.e.SourceTypeName = string(data)
			return lexEventAttributes
		})
	case 't':
		return lexAssert(':', func(lexer *Lexer) stateFn {
			data := seekUntil(l, '|')
			if bytes.Equal(data, alertError) {
				l.e.AlertType = gostatsd.AlertError
			} else if bytes.Equal(data, alertWarning) {
				l.e.AlertType = gostatsd.AlertWarning
			} else if bytes.Equal(data, alertSuccess) {
				l.e.AlertType = gostatsd.AlertSuccess
			} else if bytes.Equal(data, alertInfo) {
				// Info is default
			} else {
				l.err = errInvalidAttributes
				return nil
			}
			return lexEventAttributes
		})
	case '#':
		for {
			data, complete := seekDelimited(l, '|', ',')
			l.appendTag(data)
			if complete {
				break
			}
		}
		return lexEventAttributes
	default:
		// error no longer raised allow new fields to be sent but ignored
		_ = seekUntil(l, '|')
		return lexEventAttributes
	}
}

func lexUint32(target *uint32, next stateFn) stateFn {
	return lexUint(func(l *Lexer, value uint64) stateFn {
		if value > math.MaxUint32 {
			l.err = errOverflow
			return nil
		}
		*target = uint32(value)
		return next
	})
}

func lexUint(handler func(*Lexer, uint64) stateFn) stateFn {
	return func(l *Lexer) stateFn {
		var value uint64
		start := l.pos
	loop:
		for {
			switch b := l.next(); {
			case '0' <= b && b <= '9':
				n := value*10 + uint64(b-'0')
				if n < value {
					l.err = errOverflow
					return nil
				}
				value = n
			case b == eof:
				break loop
			default:
				l.pos--
				break loop
			}
		}
		if start == l.pos {
			l.err = errInvalidFormat
			return nil
		}
		return handler(l, value)
	}
}

// lexAssert returns a function that checks if the next byte matches the provided byte and returns next in that case.
func lexAssert(nextByte byte, next stateFn) stateFn {
	return func(l *Lexer) stateFn {
		switch b := l.next(); b {
		case nextByte:
			return next
		default:
			l.err = errInvalidFormat
			return nil
		}
	}
}

// seekUntil Seeks the next stop or eof byte and then return the byte slice between
// the start position and the end byte.
func seekUntil(l *Lexer, stop byte) []byte {
	l.start = l.pos
	p := bytes.IndexByte(l.input[l.pos:], stop)
	switch p {
	case -1:
		l.pos = l.len
	default:
		l.pos += uint32(p)
	}
	return l.input[l.start:l.pos]
}

// seekDelimited Seeks the next stop, delimiter or eof byte and then return the byte slice between
// the start position and the end byte. It does not consume the stop byte, but will consume the delimiter.
// Returns a boolean indicating whether the stop, or eof byte was detected.
func seekDelimited(l *Lexer, stop, delimiter byte) ([]byte, bool) {
	l.start = l.pos
	for {
		switch b := l.next(); b {
		case delimiter:
			return l.input[l.start : l.pos-1], false
		case stop:
			l.pos--
			return l.input[l.start:l.pos], true //reverse one position so as not to consume stop byte
		case eof:
			return l.input[l.start:l.pos], true // next does not increment pos when at eof
		}
	}
}

// lex the key.
func lexKey(l *Lexer) stateFn {
	if l.start == l.pos-1 {
		l.err = errEmptyKey
		return nil
	}
	l.m.Name = string(l.input[l.start : l.pos-1])
	if l.namespace != "" {
		l.m.Name = l.namespace + "." + l.m.Name
	}
	l.start = l.pos
	return lexValueSep
}

// lex until we find the pipe separator between value and modifier.
func lexValueSep(l *Lexer) stateFn {
	for {
		// cheap check here. ParseFloat will do it.
		switch b := l.next(); b {
		case '|':
			return lexValue
		case eof:
			l.err = errMissingValueSep
			return nil
		}
	}
}

// lex the value.
func lexValue(l *Lexer) stateFn {
	l.m.StringValue = string(l.input[l.start : l.pos-1])
	l.start = l.pos
	return lexType
}

// lex the type.
func lexType(l *Lexer) stateFn {
	b := l.next()
	switch b {
	case 'c':
		l.m.Type = gostatsd.COUNTER
		l.start = l.pos
		return lexMetricAttributes
	case 'g':
		l.m.Type = gostatsd.GAUGE
		l.start = l.pos
		return lexMetricAttributes
	case 'm':
		if b := l.next(); b != 's' {
			l.err = errInvalidType
			return nil
		}
		l.start = l.pos
		l.m.Type = gostatsd.TIMER
		return lexMetricAttributes
	case 'h':
		l.start = l.pos
		l.m.Type = gostatsd.TIMER
		return lexMetricAttributes
	case 's':
		l.m.Type = gostatsd.SET
		l.start = l.pos
		return lexMetricAttributes
	default:
		l.err = errInvalidType
		return nil
	}
}

// lexMetricAttributes iterate through any metric attributes separated by '|'
func lexMetricAttributes(l *Lexer) stateFn {
	b := l.next()
	switch b {
	case '|':
		l.start = l.pos
		return lexMetricAttribute
	case eof:
	default:
		l.err = errInvalidType
	}
	return nil
}

// lexMetricAttribute lex optional fields sample rate or tags. Will ignore unrecognised fields.
func lexMetricAttribute(l *Lexer) stateFn {
	b := l.next()
	switch b {
	case '@':
		input := seekUntil(l, '|')
		v, err := strconv.ParseFloat(string(input), 64)
		if err != nil {
			l.err = err
			return nil
		}
		l.sampling = v
	case '#':
		for {
			data, complete := seekDelimited(l, '|', ',')
			l.appendTag(data)
			if complete {
				break
			}
		}
	default:
		// error no longer raised allow new fields to be sent but ignored
		_ = seekUntil(l, '|')
	}
	return lexMetricAttributes
}
