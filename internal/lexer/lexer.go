package lexer

import (
	"bytes"
	"errors"
	"math"
	"strconv"

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
	container     string
	MetricPool    *pool.MetricPool
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
	l.container = ""
}

func (l *Lexer) appendTag(start, end uint32) {
	data := l.input[start:end]
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
			v, err := strconv.ParseFloat(l.m.StringValue, 64)
			if err != nil {
				return nil, nil, err
			}
			if math.IsNaN(v) {
				return nil, nil, errNaN
			}
			l.m.Value = v
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

func lexEventAttributes(l *Lexer) stateFn {
	switch b := l.next(); b {
	case '|':
		return lexEventAttribute(l)
	case eof:
	default:
		l.err = errInvalidAttributes
	}
	return nil
}

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
		return lexAssert(':', lexUntil('|', func(l *Lexer, data []byte) stateFn {
			l.e.Source = gostatsd.Source(data)
			return lexEventAttributes
		}))
	case 'k':
		return lexAssert(':', lexUntil('|', func(l *Lexer, data []byte) stateFn {
			l.e.AggregationKey = string(data)
			return lexEventAttributes
		}))
	case 'p':
		return lexAssert(':', lexUntil('|', func(l *Lexer, data []byte) stateFn {
			if bytes.Equal(data, priorityLow) {
				l.e.Priority = gostatsd.PriLow
			} else if bytes.Equal(data, priorityNormal) {
				// Normal is default
			} else {
				l.err = errInvalidAttributes
				return nil
			}
			return lexEventAttributes
		}))
	case 's':
		return lexAssert(':', lexUntil('|', func(l *Lexer, data []byte) stateFn {
			l.e.SourceTypeName = string(data)
			return lexEventAttributes
		}))
	case 't':
		return lexAssert(':', lexUntil('|', func(l *Lexer, data []byte) stateFn {
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
		}))
	case '#':
		return lexTags(l, lexEventAttributes)
	case 'c':
		return lexContainer(l, lexEventAttributes)
	case eof:
	default:
		// error no longer raised allow new fields to be sent but ignored
		return lexUnknown(l, lexEventAttributes)
	}
	return nil
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

// lexUntil invokes handler with all bytes up to the stop byte or an eof.
// The stop byte is not consumed.
func lexUntil(stop byte, handler func(*Lexer, []byte) stateFn) stateFn {
	return func(l *Lexer) stateFn {
		start := l.pos
		p := bytes.IndexByte(l.input[l.pos:], stop)
		switch p {
		case -1:
			l.pos = l.len
		default:
			l.pos += uint32(p)
		}
		return handler(l, l.input[start:l.pos])
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
		return lexMetricFields
	case 'g':
		l.m.Type = gostatsd.GAUGE
		l.start = l.pos
		return lexMetricFields
	case 'm':
		if b := l.next(); b != 's' {
			l.err = errInvalidType
			return nil
		}
		l.start = l.pos
		l.m.Type = gostatsd.TIMER
		return lexMetricFields
	case 'h':
		l.start = l.pos
		l.m.Type = gostatsd.TIMER
		return lexMetricFields
	case 's':
		l.m.Type = gostatsd.SET
		l.start = l.pos
		return lexMetricFields
	default:
		l.err = errInvalidType
		return nil
	}
}

// lex the possible separator between type and sampling rate.
func lexMetricFields(l *Lexer) stateFn {
	b := l.next()
	switch b {
	case '|':
		l.start = l.pos
		return lexMetricField
	case eof:
	default:
		l.err = errInvalidType
	}
	return nil
}

// lexMetricFields lex optional fields sample rate, tags, and/or container. Will ignore unrecognised fields.
// To avoid unnecessary func pointer dereferences while supporting reuse, lex functions are being called statically
// rather than being returned to the Run function
func lexMetricField(l *Lexer) stateFn {
	b := l.next()
	switch b {
	case '@':
		return lexSampleRate(l, lexMetricFields)
	case '#':
		return lexTags(l, lexMetricFields)
	case 'c':
		return lexContainer(l, lexMetricFields)
	default:
		// error no longer raised allow new fields to be sent but ignored
		return lexUnknown(l, lexMetricFields)
	}
}

// lexSampleRate Expects a float value which will be used to set lexer.sampling and returns the parameter next.
// If value cannot be parsed, l.err will be set and nil returned.
// Consumes all bytes up to the stop byte ('|') or an eof. The stop byte is not consumed.
func lexSampleRate(l *Lexer, next stateFn) stateFn {
	l.start = l.pos
	for {
		switch b := l.next(); b {
		case '|':
			return lexSampleRateValue(l, lexMetricFields)
		case eof:
			l.pos++
			return lexSampleRateValue(l, lexMetricFields)
		}
	}
}

// lexSampleRateValue Expects a float value which will be used to set lexer.sampling and returns the parameter next.
// If value cannot be parsed, l.err will be set and nil returned.
// Does not consume the stop value '|'.
func lexSampleRateValue(l *Lexer, next stateFn) stateFn {
	v, err := strconv.ParseFloat(string(l.input[l.start:l.pos-1]), 64)
	if err != nil {
		l.err = err
		return nil
	}
	l.sampling = v
	l.pos--
	return next
}

// lexTags Expects comma separated list of tags. Tags have no defined format.
// An empty list or a tag with an empty value is simply ignored.
// Will end processing by returning nil if eof is reached.
// Consumes all bytes up to the stop byte ('|') or an eof. The stop byte is not consumed.
func lexTags(l *Lexer, next stateFn) stateFn {
	l.start = l.pos
	for {
		switch b := l.next(); b {
		case ',':
			l.appendTag(l.start, l.pos-1)
			l.start = l.pos

		case '|':
			l.appendTag(l.start, l.pos-1)
			l.pos-- //reverse one position to support same pattern as lexUntil
			return next

		case eof:
			l.appendTag(l.start, l.pos) // next does not increment pos when at eof
			return nil
		}
	}
}

// lexContainer Expects a string value prefixed by ':'. The value is used to set in lexer.container with
// prefix omitted, then returns the parameter next. If prefix is not found sets lexer.error and returns nil.
// Consumes all bytes up to the stop byte ('|') or an eof. The stop byte is not consumed.
// Avoids returning a function pointer such as using
// lexAssert(':', lexUntil('|', ...
// to keep performance inline with Sample Rate processing
func lexContainer(l *Lexer, next stateFn) stateFn {
	switch b := l.next(); b {
	case ':':
		start := l.pos
		p := bytes.IndexByte(l.input[l.pos:], '|')
		switch p {
		case -1:
			l.pos = l.len
		default:
			l.pos += uint32(p)
		}
		l.container = string(l.input[start:l.pos])
		return next
	default:
		l.err = errInvalidFormat
		return nil
	}
}

// lexUnknown Consumes and discards all bytes up to the stop byte ('|') or an eof,
// then returns the parameter next. The stop byte is not consumed.
// Avoids returning a function pointer such as using
// lexUntil('|', ...
// to keep performance inline with Sample Rate processing
func lexUnknown(l *Lexer, next stateFn) stateFn {
	p := bytes.IndexByte(l.input[l.pos:], '|')
	switch p {
	case -1:
		l.pos = l.len
	default:
		l.pos += uint32(p)
	}
	return next
}
