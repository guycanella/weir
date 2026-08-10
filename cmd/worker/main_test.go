package main

import "testing"

func TestParseConcurrency(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
	}{
		{name: "unset", in: "", want: 0},
		{name: "blank", in: "   ", want: 0},
		{name: "not a number", in: "abc", want: 0},
		{name: "zero", in: "0", want: 0},
		{name: "negative", in: "-3", want: 0},
		{name: "valid", in: "8", want: 8},
		{name: "valid with surrounding whitespace", in: " 4 ", want: 4},
		{name: "at max", in: "100", want: 100},
		{name: "above max clamps to max", in: "2000000000", want: 100},
		{name: "positive overflow clamps to max", in: "99999999999999999999", want: 100},
		{name: "negative overflow returns zero", in: "-99999999999999999999", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseConcurrency(tc.in); got != tc.want {
				t.Errorf("parseConcurrency(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
