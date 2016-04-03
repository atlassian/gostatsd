package statsd

import (
	"errors"
	"math"
	"strconv"

	"github.com/jtblin/gostatsd/types"
)

type lexer struct {
	input     []byte
	len       int
	start     int
	pos       int
	m         *types.Metric
	namespace string
	err       error
	sampling  float64
}

// assumes we don't have \x00 bytes in input.
const eof = 0

var (
	errMissingKeySep         = errors.New("missing key separator")
	errEmptyKey              = errors.New("key zero len")
	errMissingValueSep       = errors.New("missing value separator")
	errInvalidType           = errors.New("invalid type")
	errInvalidSamplingOrTags = errors.New("invalid sampling or tags")
	errInvalidTags           = errors.New("invalid tags")
	errNaN                   = errors.New("invalid value NaN")
)

func (l *lexer) next() byte {
	if l.pos >= l.len {
		return eof
	}
	b := l.input[l.pos]
	l.pos++
	return b
}

func (l *lexer) run() (*types.Metric, error) {
	l.sampling = float64(1)

	for state := lexKeySep; state != nil; {
		state = state(l)
	}
	if l.err != nil {
		return nil, l.err
	}
	if l.m.Type != types.SET {
		v, err := strconv.ParseFloat(l.m.StringValue, 64)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(v) {
			return nil, errNaN
		}
		l.m.Value = v
		l.m.StringValue = ""
	}
	if l.m.Type == types.COUNTER {
		l.m.Value = l.m.Value / l.sampling
	}

	return l.m, nil
}

type stateFn func(*lexer) stateFn

// lex until we find the colon separator between key and value.
func lexKeySep(l *lexer) stateFn {
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

// lex the key.
func lexKey(l *lexer) stateFn {
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
func lexValueSep(l *lexer) stateFn {
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
func lexValue(l *lexer) stateFn {
	l.m.StringValue = string(l.input[l.start : l.pos-1])
	l.start = l.pos
	return lexType
}

// lex the type.
func lexType(l *lexer) stateFn {
	b := l.next()
	switch b {
	case 'c':
		l.m.Type = types.COUNTER
		l.start = l.pos
		return lexTypeSep
	case 'g':
		l.m.Type = types.GAUGE
		l.start = l.pos
		return lexTypeSep
	case 'm':
		if b := l.next(); b != 's' {
			l.err = errInvalidType
			return nil
		}
		l.start = l.pos
		l.m.Type = types.TIMER
		return lexTypeSep
	case 's':
		l.m.Type = types.SET
		l.start = l.pos
		return lexTypeSep
	default:
		l.err = errInvalidType
		return nil

	}
}

// lex the possible separator between type and sampling rate.
func lexTypeSep(l *lexer) stateFn {
	b := l.next()
	switch b {
	case eof:
		return nil
	case '|':
		l.start = l.pos
		return lexSampleRateOrTags
	}
	l.err = errInvalidType
	return nil
}

// lex the sample rate or the tags.
func lexSampleRateOrTags(l *lexer) stateFn {
	b := l.next()
	switch b {
	case '@':
		l.start = l.pos
		for {
			switch b := l.next(); b {
			case '|':
				return lexSampleRate
			case eof:
				l.pos++
				return lexSampleRate
			}
		}
	case '#':
		l.pos--
		return lexTags
	default:
		l.err = errInvalidSamplingOrTags
		return nil
	}
}

// lex the sample rate.
func lexSampleRate(l *lexer) stateFn {
	v, err := strconv.ParseFloat(string(l.input[l.start:l.pos-1]), 64)
	if err != nil {
		l.err = err
		return nil
	}
	l.sampling = v
	if l.pos >= l.len {
		return nil
	}
	return lexTags
}

// lex the tags.
func lexTags(l *lexer) stateFn {
	l.start = l.pos
	if l.next() != '#' {
		l.err = errInvalidTags
		return nil
	}
	l.start = l.pos
	for {
		switch b := l.next(); b {
		case ',':
			l.m.Tags = append(l.m.Tags, string(l.input[l.start:l.pos-1]))
			l.start = l.pos
		case eof:
			l.pos++
			l.m.Tags = append(l.m.Tags, string(l.input[l.start:l.pos-1]))
			return nil
		case '.', ':', '-', '_':
			continue
		case '/':
			l.input[l.pos-1] = '-'
		case ' ', '\t':
			l.input[l.pos-1] = '_'
		default:
			r := rune(b)
			if (97 <= r && 122 >= r) || (48 <= r && 57 >= r) {
				continue
			}
			if 65 <= r && 90 >= r {
				l.input[l.pos-1] = byte(r + 32)
				continue
			}
			l.input = append(l.input[0:l.pos-1], l.input[l.pos:]...)
			l.len--
			l.pos--
		}
	}
}
