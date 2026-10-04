// Package sqlerr defines the errors the SQL layer reports to clients: a
// SQLSTATE code, a message, optional detail and hint, and the character
// position in the query. See docs/design/09-sql-frontend.md, section 2.1.
package sqlerr

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// SQLSTATE codes used by NoVacDB, with PostgreSQL's meanings.
const (
	SuccessfulCompletion         = "00000"
	FeatureNotSupported          = "0A000"
	CardinalityViolation         = "21000"
	DataException                = "22000"
	StringDataRightTruncation    = "22001"
	NumericValueOutOfRange       = "22003"
	InvalidDatetimeFormat        = "22007"
	DatetimeFieldOverflow        = "22008"
	DivisionByZero               = "22012"
	InvalidParameterValue        = "22023"
	InvalidEscapeSequence        = "22025"
	CharacterNotInRepertoire     = "22021"
	InvalidTextRepresentation    = "22P02"
	InvalidBinaryRepresentation  = "22P03"
	NullValueNotAllowed          = "22004"
	InvalidRegularExpression     = "2201B"
	InvalidRowCountInLimit       = "2201W"
	InvalidRowCountInOffset      = "2201X"
	NotNullViolation             = "23502"
	UniqueViolation              = "23505"
	SyntaxError                  = "42601"
	InsufficientPrivilege        = "42501"
	UndefinedColumn              = "42703"
	UndefinedTable               = "42P01"
	UndefinedObject              = "42704"
	UndefinedFunction            = "42883"
	AmbiguousFunction            = "42725"
	WrongObjectType              = "42809"
	UndefinedParameter           = "42P02"
	DuplicateCursor              = "42P03"
	DuplicatePreparedStatement   = "42P05"
	InvalidCursorName            = "34000"
	InvalidSQLStatementName      = "26000"
	DuplicateTable               = "42P07"
	DuplicateObject              = "42710"
	DuplicateColumn              = "42701"
	AmbiguousColumn              = "42702"
	DatatypeMismatch             = "42804"
	CannotCoerce                 = "42846"
	InvalidColumnReference       = "42P10"
	InvalidTableDefinition       = "42P16"
	NameTooLong                  = "42622"
	ReservedName                 = "42939"
	ProgramLimitExceeded         = "54000"
	TooManyColumns               = "54011"
	DependentObjectsStillExist   = "2BP01"
	StatementTooComplex          = "54001"
	ObjectNotInPrerequisiteState = "55000"
	InvalidTransactionState      = "25000"
	InFailedSQLTransaction       = "25P02"
	SerializationFailure         = "40001"
	QueryCanceled                = "57014"
	AdminShutdown                = "57P01"
	CannotConnectNow             = "57P03"
	IdleSessionTimeout           = "57P05"
	TooManyConnections           = "53300"
	IOError                      = "58030"
	ProtocolViolation            = "08P01"
	InvalidAuthorization         = "28000"
	InternalError                = "XX000"
	DataCorrupted                = "XX001"
)

// Error is an error reported to a SQL client.
type Error struct {
	Code     string
	Message  string
	Detail   string
	Hint     string
	Position int // 1-based character position in the query; 0 if none

	cause error
}

// Error formats the error for logs and tests.
func (e *Error) Error() string {
	s := fmt.Sprintf("%s (SQLSTATE %s)", e.Message, e.Code)
	if e.Position > 0 {
		s += fmt.Sprintf(" at character %d", e.Position)
	}
	return s
}

// Unwrap returns the underlying error, if any.
func (e *Error) Unwrap() error { return e.cause }

// New returns an error with the given code and message.
func New(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap returns an error with the given code and message that wraps cause.
func Wrap(cause error, code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), cause: cause}
}

// WithHint sets the hint and returns e.
func (e *Error) WithHint(format string, args ...any) *Error {
	e.Hint = fmt.Sprintf(format, args...)
	return e
}

// WithDetail sets the detail and returns e.
func (e *Error) WithDetail(format string, args ...any) *Error {
	e.Detail = fmt.Sprintf(format, args...)
	return e
}

// At sets the position from a byte offset into sql and returns e. A
// negative offset leaves the position unset.
func (e *Error) At(sql string, byteOffset int) *Error {
	if byteOffset >= 0 {
		e.Position = CharPos(sql, byteOffset)
	}
	return e
}

// CharPos converts a byte offset into sql to PostgreSQL's 1-based character
// position. An offset past the end gives the position after the last
// character.
func CharPos(sql string, byteOffset int) int {
	if byteOffset > len(sql) {
		byteOffset = len(sql)
	}
	return utf8.RuneCountInString(sql[:byteOffset]) + 1
}

// From returns err as an *Error: itself if it is one (or wraps one),
// otherwise an internal error wrapping it.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return Wrap(err, InternalError, "%v", err)
}

// Code returns the SQLSTATE of err, InternalError for errors that are not
// *Error, and "" for nil.
func Code(err error) string {
	if err == nil {
		return ""
	}
	return From(err).Code
}
