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
	"errors"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/go-sql-driver/mysql"
)

type MySQLErrorInspector struct{}

// InspectError examines a MySQL error and formats it as an ADBC error
// mysql error codes: https://www.fromdual.com/mysql-error-codes-and-messages
func (m MySQLErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	status := defaultStatus

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1045: // ER_ACCESS_DENIED_ERROR
			status = adbc.StatusUnauthenticated
		case 1044, 1142, 1143, 1227: // Permission errors
			status = adbc.StatusUnauthorized
		case 1146: // ER_NO_SUCH_TABLE
			status = adbc.StatusNotFound
		case 1049: // ER_BAD_DB_ERROR
			status = adbc.StatusNotFound
		case 1050: // ER_TABLE_EXISTS_ERROR
			status = adbc.StatusAlreadyExists
		case 1007: // ER_DB_CREATE_EXISTS
			status = adbc.StatusAlreadyExists
		case 1062: // ER_DUP_ENTRY
			status = adbc.StatusIntegrity
		case 1451: // ER_ROW_IS_REFERENCED_2 (foreign key constraint)
			status = adbc.StatusIntegrity
		case 1452: // ER_NO_REFERENCED_ROW_2 (foreign key constraint)
			status = adbc.StatusIntegrity
		case 1048: // ER_BAD_NULL_ERROR
			status = adbc.StatusIntegrity
		case 1364: // ER_NO_DEFAULT_FOR_FIELD
			status = adbc.StatusIntegrity
		case 1064: // ER_PARSE_ERROR
			status = adbc.StatusInvalidArgument
		case 1054: // ER_BAD_FIELD_ERROR
			status = adbc.StatusInvalidArgument
		case 1052: // ER_NON_UNIQ_ERROR
			status = adbc.StatusInvalidArgument
		case 1366: // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
			status = adbc.StatusInvalidData
		case 1292: // ER_TRUNCATED_WRONG_VALUE
			status = adbc.StatusInvalidData
		case 1264: // ER_WARN_DATA_OUT_OF_RANGE
			status = adbc.StatusInvalidData
		case 1205: // ER_LOCK_WAIT_TIMEOUT
			status = adbc.StatusTimeout
		case 1213: // ER_LOCK_DEADLOCK
			status = adbc.StatusCancelled
		case 2002, 2003, 2006, 2013: // Various connection errors
			status = adbc.StatusIO
		case 1105: // ER_UNKNOWN_ERROR
			status = adbc.StatusInternal
		}

		// If status still not determined, use SQLSTATE prefix as fallback.
		if mysqlErr.SQLState[0] != 0 && status == defaultStatus {
			switch string(mysqlErr.SQLState[:2]) {
			case "02": // No data
				status = adbc.StatusNotFound
			case "07": // Dynamic SQL/Connection errors
				status = adbc.StatusIO
			case "08": // Connection exception
				status = adbc.StatusIO
			case "21", "22": // Cardinality/Data exception
				status = adbc.StatusInvalidData
			case "23": // Integrity constraint violation
				status = adbc.StatusIntegrity
			case "28": // Invalid authorization
				status = adbc.StatusUnauthenticated
			case "34": // Invalid cursor name
				status = adbc.StatusInvalidArgument
			case "42": // Syntax error or access rule violation
				status = adbc.StatusInvalidArgument
			case "44": // WITH CHECK OPTION violation
				status = adbc.StatusIntegrity
			case "55", "57": // Object not in prerequisite state / Operator intervention
				status = adbc.StatusInvalidState
			case "58": // System error
				status = adbc.StatusInternal
			}
		}
	}

	return adbc.Error{
		Code: status,
		Msg:  err.Error(),
	}
}
