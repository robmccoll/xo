package models

// IndexColumn represents index column info.
type IndexColumn struct {
	SeqNo      int    // seq_no
	Cid        int    // cid
	ColumnName string // column_name

	// Extra Fields for Expression Indices
	Function  string
	Collation string
	Columns   []*Column
}
