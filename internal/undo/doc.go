// Package undo is the undo log (docs/design/14-undo-log.md): the previous
// versions of rows, kept in undo pages of the data file, one segment of
// pages per transaction. Every change to undo pages and to the segment
// table is WAL-logged, so the log survives crashes like any other page.
package undo
