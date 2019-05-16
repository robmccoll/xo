package models

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// PgNodeTree is a root PGNode and its children
type PgNodeTree struct {
	PGNode
}

var _ ScannerValuer = &PgNodeTree{}

// ParsePGNodeString attempts to parse a string into a tree of generic PGNodes
func ParsePGNodeString(s string) (*PGNode, error) {
	return ParsePGNodeBytes([]byte(s))
}

// ParsePGNodeBytes attempts to parse a byte string into a generic PGNodes
func ParsePGNodeBytes(b []byte) (*PGNode, error) {
	return ReadPGNode(bytes.NewBuffer(b))
}

// ReadPGNode attempts to parse a generic PGNodes from the input reader
func ReadPGNode(r io.Reader) (*PGNode, error) {
	return ParsePGNode(bufio.NewReader(r))
}

// ParsePGNode attempts to recursively parse a generic PGNodes from the input
// buffered reader
func ParsePGNode(r *bufio.Reader) (*PGNode, error) {
	token, err := nextToken(r)
	if err != nil {
		return nil, err
	}

	switch token {
	case "{":
		return ParsePGNodeStruct(r)
	case "(":
		return ParsePGNodeList(r)
	case ")", "}":
		return nil, fmt.Errorf("unexpected terminal token %s", token)
	default:
		return &PGNode{Val: token}, nil
	}
}

// Scan satisfies the sql.Scanner interface for PGNode.
func (n *PGNode) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	buf, ok := src.([]byte)
	if !ok {
		return errors.New("invalid PGNode")
	}

	out, err := ParsePGNodeBytes(buf)
	if out != nil {
		*n = *out
	}
	return err
}

// Value satisfies the driver.Valuer interface for PGNode.
func (n PGNode) Value() (driver.Value, error) {
	return string(n.RawValue), nil
}

// PGNode represents a generic parsed Postgres Node
// https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=src/backend/nodes
// Information about indexes over expressions contain serialized FuncExpr nodes
type PGNode struct {
	Type   string             `json:",omitempty"`
	Val    string             `json:",omitempty"`
	List   []*PGNode          `json:",omitempty"`
	Fields map[string]*PGNode `json:",omitempty"`

	RawValue []byte `json:",omitempty"`
}

// AsInt converts the value to an integer or panics
func (n *PGNode) AsInt() int {
	i, err := strconv.Atoi(n.Val)
	if err != nil {
		panic(err)
	}
	return i
}

// nextToken is a Go reimplementation of pg_strtok()
func nextToken(r *bufio.Reader) (string, error) {
	var next []byte
	var err error

	// eat whitespace
	for {
		if next, err = r.Peek(1); err != nil {
			return "", err
		}
		switch next[0] {
		case ' ', '\n', '\r', '\t':
			if _, err := r.Discard(1); err != nil {
				return "", err
			}
			continue
		}
		break
	}

	// special tokens
	switch next[0] {
	case '(', ')', '{', '}':
		_, err = r.Discard(1)
		return string(next), err
	}

	var token []byte

	// else read to next unescaped whitespace or special token
	for {
		if next, err = r.Peek(1); err != nil {
			return "", err
		}
		switch next[0] {
		case ' ', '\n', '\r', '\t', '(', ')', '{', '}':
			out := string(token)
			if out == "<>" {
				return "", nil
			}
			return out, nil
		case '\\':
			if _, err = r.Discard(1); err != nil {
				return "", err
			}
			if next, err = r.Peek(1); err != nil {
				return "", err
			}
		}
		token = append(token, next[0])
		if _, err = r.Discard(1); err != nil {
			return "", err
		}
	}
}

// ParsePGNodeStruct recursively parses the portions of a note structure after
// the initial '{' which is assumed to have been consumed by the caller
func ParsePGNodeStruct(r *bufio.Reader) (*PGNode, error) {
	token, err := nextToken(r)
	if err != nil {
		return nil, err
	}

	switch token {
	case "{", "(", ")", "}", "":
		return nil, fmt.Errorf("unexpected token %s looking for node type", token)
	}

	var out = &PGNode{
		Type:   token,
		Fields: make(map[string]*PGNode),
	}

	for token, err = nextToken(r); token != "}" && err == nil; token, err = nextToken(r) {
		switch token {
		case "{", "(", ")", "":
			return nil, fmt.Errorf("unexpected token %s looking for field name", token)
		}
		if token[0] != ':' {
			return nil, fmt.Errorf("found %s looking for field name - field names begin with ':'", token)
		}
		out.Fields[string(token[1:])], err = ParsePGNode(r)
		if err != nil {
			return nil, err
		}
	}

	return out, err
}

// ParsePGNodeList recursively parses the portions of a list after
// the initial '(' which is assumed to have been consumed by the caller
func ParsePGNodeList(r *bufio.Reader) (*PGNode, error) {
	token, err := nextToken(r)
	if err != nil {
		return nil, err
	}

	var out = &PGNode{
		Type: "List:nodes",
		List: []*PGNode{},
	}
	switch token {
	case "o":
		out.Type = "List:oids"
		token, err = nextToken(r)
		if err != nil {
			return nil, err
		}
	case "i":
		out.Type = "List:ints"
		token, err = nextToken(r)
		if err != nil {
			return nil, err
		}
	}

	for {
		switch token {
		case "{":
			node, err := ParsePGNodeStruct(r)
			if err != nil {
				return nil, err
			}
			out.List = append(out.List, node)
		case "(":
			node, err := ParsePGNodeList(r)
			if err != nil {
				return nil, err
			}
			out.List = append(out.List, node)
		case ")":
			return out, nil
		case "}":
			return nil, fmt.Errorf("unexpected terminal token %s", token)
		default:
			out.List = append(out.List, &PGNode{Val: token})
		}

		token, err = nextToken(r)
		if err != nil {
			return nil, err
		}
	}
}
