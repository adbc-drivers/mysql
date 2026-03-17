// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
)

// CSVConfig defines the configuration for Arrow-to-CSV/TSV conversion.
type CSVConfig struct {
	FieldDelimiter  byte
	LineTerminator  byte
	NullValue       string
	EscapeBackslash bool
}

// arrowToCSV reads from a RowBufferIterator and streams data in CSV/TSV format into the provided io.Writer.
func arrowToCSV(ctx context.Context, w io.Writer, it *sqlwrapper.RowBufferIterator, numCols int, config CSVConfig) error {
	var buf strings.Builder

	for it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, rowCount := it.CurrentBatch()

		buf.Reset()
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			for colIdx := 0; colIdx < numCols; colIdx++ {
				if colIdx > 0 {
					buf.WriteByte(config.FieldDelimiter)
				}

				val := rows[rowIdx*numCols+colIdx]
				buf.WriteString(formatValueForCSV(val, config))
			}
			buf.WriteByte(config.LineTerminator)
		}
		if _, err := io.WriteString(w, buf.String()); err != nil {
			return fmt.Errorf("failed to write batch to pipe: %w", err)
		}
	}

	return it.Err()
}

// escapeCSV escapes special characters based on the provided CSVConfig.
func escapeCSV(s string, config CSVConfig) string {
	if config.EscapeBackslash {
		s = strings.ReplaceAll(s, "\\", "\\\\")
		s = strings.ReplaceAll(s, "\b", "\\b")
		s = strings.ReplaceAll(s, "\x1a", "\\Z")
		s = strings.ReplaceAll(s, "\x00", "\\0")
		// Always escape \r if we are escaping backslashes, as it's a common special char
		s = strings.ReplaceAll(s, "\r", "\\r")
	}

	if config.FieldDelimiter == '\t' {
		s = strings.ReplaceAll(s, "\t", "\\t")
	}
	if config.LineTerminator == '\n' {
		s = strings.ReplaceAll(s, "\n", "\\n")
	}

	return s
}

// formatValueForCSV converts a Go interface{} to a string suitable for CSV/TSV, handling escaping.
func formatValueForCSV(val any, config CSVConfig) string {
	if val == nil {
		return config.NullValue
	}

	switch v := val.(type) {
	case string:
		return escapeCSV(v, config)
	case []byte:
		return escapeCSV(string(v), config)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}
