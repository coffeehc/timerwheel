package timerwheel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	// Set the top bit if a star was included in the expression.
	starBit = 1 << 63
)

// getField returns an Int with the bits set representing all of the times that
// the field represents or error parsing field value.  A "field" is a comma-separated
// list of "ranges".
func ParseCronField(field string, maxSlot uint64) (uint64, error) {
	var bits uint64
	ranges := strings.FieldsFunc(field, func(r rune) bool { return r == ',' })
	for _, expr := range ranges {
		bit, err := getRange(expr, maxSlot)
		if err != nil {
			return bits, err
		}
		bits |= bit
	}
	return bits, nil
}

// getRange returns the bits indicated by the given expression:
//   number | number "-" number [ "/" number ]
// or error parsing range.
func getRange(expr string, maxSlot uint64) (uint64, error) {
	var (
		start, end, step uint64
		rangeAndStep     = strings.Split(expr, "/")
		lowAndHigh       = strings.Split(rangeAndStep[0], "-")
		singleDigit      = len(lowAndHigh) == 1
		err              error
	)
	var extra uint64
	if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
		start = 0
		end = maxSlot
		extra = starBit
	} else {
		start, err = parseInt(lowAndHigh[0])
		if err != nil {
			return 0, err
		}
		switch len(lowAndHigh) {
		case 1:
			end = start
		case 2:
			end, err = parseInt(lowAndHigh[1])
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("Too many hyphens: %s", expr)
		}
	}
	switch len(rangeAndStep) {
	case 1:
		step = 1
	case 2:
		step, err = parseInt(rangeAndStep[1])
		if err != nil {
			return 0, err
		}

		// Special handling: "N/step" means "N-max/step".
		if singleDigit {
			end = maxSlot
		}
	default:
		return 0, fmt.Errorf("Too many slashes: %s", expr)
	}

	if start < 0 {
		return 0, fmt.Errorf("Beginning of range (%d) below minimum (%d): %s", start, 0, expr)
	}
	if end > maxSlot {
		return 0, fmt.Errorf("End of range (%d) above maximum (%d): %s", end, maxSlot, expr)
	}
	if start > end {
		return 0, fmt.Errorf("Beginning of range (%d) beyond end of range (%d): %s", start, end, expr)
	}
	if step == 0 {
		return 0, fmt.Errorf("Step of range should be a positive number: %s", expr)
	}

	return getBits(start, end, step) | extra, nil
}

// mustParseInt parses the given expression as an int or returns an error.
func parseInt(expr string) (uint64, error) {
	num, err := strconv.ParseUint(expr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Failed to parse int from %s: %s", expr, err)
	}
	if num < 0 {
		return 0, fmt.Errorf("Negative number (%d) not allowed: %s", num, expr)
	}

	return uint64(num), nil
}

// getBits sets all bits in the range [min, max], modulo the given step size.
func getBits(min, max, step uint64) uint64 {
	var bits uint64
	// If step is 1, use shifts.
	if step == 1 {
		return ^(math.MaxUint64 << (max + 1)) & (math.MaxUint64 << min)
	}

	// Else, use a simple loop.
	for i := min; i <= max; i += step {
		bits |= 1 << i
	}
	return bits
}

// all returns all bits within the given bounds.  (plus the star bit)
func all(maxSlot uint64) uint64 {
	return getBits(0, maxSlot, 1) | starBit
}
