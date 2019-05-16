package internal

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/template"
	"unicode"

	"github.com/knq/snaker"

	"github.com/xo/xo/models"
)

// NewTemplateFuncs returns a set of template funcs bound to the supplied args.
func (a *ArgType) NewTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"colcount":           a.colcount,
		"colnames":           a.colnames,
		"colnamesmulti":      a.colnamesmulti,
		"colnamesquery":      a.colnamesquery,
		"colnamesquerymulti": a.colnamesquerymulti,
		"colprefixnames":     a.colprefixnames,
		"colvals":            a.colvals,
		"colvalsmulti":       a.colvalsmulti,
		"fieldnames":         a.fieldnames,
		"fieldnamesmulti":    a.fieldnamesmulti,
		"goparamlist":        a.goparamlist,
		"reniltype":          a.reniltype,
		"retype":             a.retype,
		"shortname":          a.shortname,
		"convext":            a.convext,
		"schema":             a.schemafn,
		"colname":            a.colname,
		"hascolumn":          a.hascolumn,
		"hasfield":           a.hasfield,
		"getstartcount":      a.getstartcount,
		"slice":              slice,
		"sum":                sum,
		"lowerfirst":         lowerfirst,
		"lower":              lower,
		"contains":           contains,
		"cameltosnake":       snaker.CamelToSnake,
		"snaketocamel":       snaker.SnakeToCamel,
	}
}

// retype checks typ against known types, and prefixing
// ArgType.CustomTypePackage (if applicable).
func (a *ArgType) retype(typ string) string {
	if strings.Contains(typ, ".") {
		return typ
	}

	prefix := ""
	for strings.HasPrefix(typ, "[]") {
		typ = typ[2:]
		prefix = prefix + "[]"
	}

	if _, ok := a.KnownTypeMap[typ]; !ok {
		pkg := a.CustomTypePackage
		if pkg != "" {
			pkg = pkg + "."
		}

		return prefix + pkg + typ
	}

	return prefix + typ
}

// reniltype checks typ against known nil types (similar to retype), prefixing
// ArgType.CustomTypePackage (if applicable).
func (a *ArgType) reniltype(typ string) string {
	if strings.Contains(typ, ".") {
		return typ
	}

	if strings.HasSuffix(typ, "{}") {
		if _, ok := a.KnownTypeMap[typ[:len(typ)-2]]; ok {
			return typ
		}

		pkg := a.CustomTypePackage
		if pkg != "" {
			pkg = pkg + "."
		}

		return pkg + typ
	}

	return typ
}

// shortname generates a safe Go identifier for typ. typ is first checked
// against ArgType.ShortNameTypeMap, and if not found, then the value is
// calculated and stored in the ShortNameTypeMap for future use.
//
// A shortname is the concatentation of the lowercase of the first character in
// the words comprising the name. For example, "MyCustomName" will have have
// the shortname of "mcn".
//
// If a generated shortname conflicts with a Go reserved name, then the
// corresponding value in goReservedNames map will be used.
//
// Generated shortnames that have conflicts with any scopeConflicts member will
// have ArgType.NameConflictSuffix appended.
//
// Note: recognized types for scopeConflicts are string, []*Field,
// []*QueryParam.
func (a *ArgType) shortname(typ string, scopeConflicts ...interface{}) string {
	var v string
	var ok bool

	// check short name map
	if v, ok = a.ShortNameTypeMap[typ]; !ok {
		// calc the short name
		u := []string{}
		for _, s := range strings.Split(strings.ToLower(snaker.CamelToSnake(typ)), "_") {
			if len(s) > 0 && s != "id" {
				u = append(u, s[:1])
			}
		}
		v = strings.Join(u, "")

		// check go reserved names
		if n, ok := goReservedNames[v]; ok {
			v = n
		}

		// store back to short name map
		a.ShortNameTypeMap[typ] = v
	}

	// initial conflicts are the default imported packages from
	// xo_package.go.tpl
	conflicts := map[string]bool{
		"sql":     true,
		"driver":  true,
		"csv":     true,
		"errors":  true,
		"fmt":     true,
		"regexp":  true,
		"strings": true,
		"time":    true,
	}

	// add scopeConflicts to conflicts
	for _, c := range scopeConflicts {
		switch k := c.(type) {
		case string:
			conflicts[k] = true

		case []*Field:
			for _, f := range k {
				conflicts[f.Name] = true
			}
		case []*QueryParam:
			for _, f := range k {
				conflicts[f.Name] = true
			}

		default:
			panic("not implemented")
		}
	}

	// append suffix if conflict exists
	if _, ok := conflicts[v]; ok {
		v = v + a.NameConflictSuffix
	}

	return v
}

// colnames creates a list of the column names found in fields, excluding any
// Field with Name contained in ignoreNames.
//
// Used to present a comma separated list of column names, that can be used in
// a SELECT, or UPDATE, or other SQL clause requiring an list of identifiers
// (ie, "field_1, field_2, field_3, ...").
func (a *ArgType) colnames(fields []*Field, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + a.colname(f.Col)
		i++
	}

	return str
}

// colnamesmulti creates a list of the column names found in fields, excluding any
// Field with Name contained in ignoreNames.
//
// Used to present a comma separated list of column names, that can be used in
// a SELECT, or UPDATE, or other SQL clause requiring an list of identifiers
// (ie, "field_1, field_2, field_3, ...").
func (a *ArgType) colnamesmulti(fields []*Field, ignoreNames []*Field) string {
	ignore := map[string]bool{}
	for _, f := range ignoreNames {
		ignore[f.Name] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + a.colname(f.Col)
		i++
	}

	return str
}

// colnamesquery creates a list of the column names in fields as a query and
// joined by sep, excluding any Field with Name contained in ignoreNames.
//
// Used to create a list of column names in a WHERE clause (ie, "field_1 = $1
// AND field_2 = $2 AND ...") or in an UPDATE clause (ie, "field = $1, field =
// $2, ...").
func (a *ArgType) colnamesquery(fields []*Field, sep string, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + sep
		}
		str = str + a.colname(f.Col) + " = " + a.Loader.NthParam(i)
		i++
	}

	return str
}

// colnamesquerymulti creates a list of the column names in fields as a query and
// joined by sep, excluding any Field with Name contained in the slice of fields in ignoreNames.
//
// Used to create a list of column names in a WHERE clause (ie, "field_1 = $1
// AND field_2 = $2 AND ...") or in an UPDATE clause (ie, "field = $1, field =
// $2, ...").
func (a *ArgType) colnamesquerymulti(fields []*Field, sep string, startCount int, ignoreNames []*Field) string {
	ignore := map[string]bool{}
	for _, f := range ignoreNames {
		ignore[f.Name] = true
	}

	str := ""
	i := startCount
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i > startCount {
			str = str + sep
		}
		str = str + a.colname(f.Col) + " = " + a.Loader.NthParam(i)
		i++
	}

	return str
}

// colprefixnames creates a list of the column names found in fields with the
// supplied prefix, excluding any Field with Name contained in ignoreNames.
//
// Used to present a comma separated list of column names with a prefix. Used in
// a SELECT, or UPDATE (ie, "t.field_1, t.field_2, t.field_3, ...").
func (a *ArgType) colprefixnames(fields []*Field, prefix string, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + prefix + "." + a.colname(f.Col)
		i++
	}

	return str
}

// colvals creates a list of value place holders for fields excluding any Field
// with Name contained in ignoreNames.
//
// Used to present a comma separated list of column place holders, used in a
// SELECT or UPDATE statement (ie, "$1, $2, $3 ...").
func (a *ArgType) colvals(fields []*Field, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + a.Loader.NthParam(i)
		i++
	}

	return str
}

// colvalsmulti creates a list of value place holders for fields excluding any Field
// with Name contained in ignoreNames.
//
// Used to present a comma separated list of column place holders, used in a
// SELECT or UPDATE statement (ie, "$1, $2, $3 ...").
func (a *ArgType) colvalsmulti(fields []*Field, ignoreNames []*Field) string {
	ignore := map[string]bool{}
	for _, f := range ignoreNames {
		ignore[f.Name] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + a.Loader.NthParam(i)
		i++
	}

	return str
}

// fieldnames creates a list of field names from fields of the adding the
// provided prefix, and excluding any Field with Name contained in ignoreNames.
//
// Used to present a comma separated list of field names, ie in a Go statement
// (ie, "t.Field1, t.Field2, t.Field3 ...")
func (a *ArgType) fieldnames(fields []*Field, prefix string, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + prefix + "." + f.Name
		i++
	}

	return str
}

// fieldnamesmulti creates a list of field names from fields of the adding the
// provided prefix, and excluding any Field with the slice contained in ignoreNames.
//
// Used to present a comma separated list of field names, ie in a Go statement
// (ie, "t.Field1, t.Field2, t.Field3 ...")
func (a *ArgType) fieldnamesmulti(fields []*Field, prefix string, ignoreNames []*Field) string {
	ignore := map[string]bool{}
	for _, f := range ignoreNames {
		ignore[f.Name] = true
	}

	str := ""
	i := 0
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		if i != 0 {
			str = str + ", "
		}
		str = str + prefix + "." + f.Name
		i++
	}

	return str
}

// colcount returns the 1-based count of fields, excluding any Field with Name
// contained in ignoreNames.
//
// Used to get the count of fields, and useful for specifying the last SQL
// parameter.
func (a *ArgType) colcount(fields []*Field, ignoreNames ...string) int {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	i := 1
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		i++
	}
	return i
}

// goReservedNames is a map of of go reserved names to "safe" names.
var goReservedNames = map[string]string{
	"break":       "brk",
	"case":        "cs",
	"chan":        "chn",
	"const":       "cnst",
	"continue":    "cnt",
	"default":     "def",
	"defer":       "dfr",
	"else":        "els",
	"fallthrough": "flthrough",
	"for":         "fr",
	"func":        "fn",
	"go":          "goVal",
	"goto":        "gt",
	"if":          "ifVal",
	"import":      "imp",
	"interface":   "iface",
	"map":         "mp",
	"package":     "pkg",
	"range":       "rnge",
	"return":      "ret",
	"select":      "slct",
	"struct":      "strct",
	"switch":      "swtch",
	"type":        "typ",
	"var":         "vr",

	// go types
	"error":      "e",
	"bool":       "b",
	"string":     "str",
	"byte":       "byt",
	"rune":       "r",
	"uintptr":    "uptr",
	"int":        "i",
	"int8":       "i8",
	"int16":      "i16",
	"int32":      "i32",
	"int64":      "i64",
	"uint":       "u",
	"uint8":      "u8",
	"uint16":     "u16",
	"uint32":     "u32",
	"uint64":     "u64",
	"float32":    "z",
	"float64":    "f",
	"complex64":  "c",
	"complex128": "c128",
}

// goparamlist converts a list of fields into their named Go parameters,
// skipping any Field with Name contained in ignoreNames. addType will cause
// the go Type to be added after each variable name. addPrefix will cause the
// returned string to be prefixed with ", " if the generated string is not
// empty.
//
// Any field name encountered will be checked against goReservedNames, and will
// have its name substituted by its corresponding looked up value.
//
// Used to present a comma separated list of Go variable names for use with as
// either a Go func parameter list, or in a call to another Go func.
// (ie, ", a, b, c, ..." or ", a T1, b T2, c T3, ...").
func (a *ArgType) goparamlist(fields []*Field, addPrefix bool, addType bool, ignoreNames ...string) string {
	ignore := map[string]bool{}
	for _, n := range ignoreNames {
		ignore[n] = true
	}

	i := 0
	vals := []string{}
	for _, f := range fields {
		if ignore[f.Name] {
			continue
		}

		s := "v" + strconv.Itoa(i)
		if len(f.Name) > 0 {
			n := strings.Split(snaker.CamelToSnake(f.Name), "_")
			s = strings.ToLower(n[0]) + f.Name[len(n[0]):]
		}

		// check go reserved names
		if r, ok := goReservedNames[strings.ToLower(s)]; ok {
			s = r
		}

		// add the go type
		if addType {
			s += " " + a.retype(f.Type)
		}

		// add to vals
		vals = append(vals, s)

		i++
	}

	// concat generated values
	str := strings.Join(vals, ", ")
	if addPrefix && str != "" {
		return ", " + str
	}

	return str
}

// convext generates the Go conversion for f in order for it to be assignable
// to t.
//
// FIXME: this should be a better name, like "goconversion" or some such.
func (a *ArgType) convext(prefix string, f *Field, t *Field) string {
	expr := prefix + "." + f.Name
	if f.Type == t.Type {
		return expr
	}

	ft := f.Type
	if strings.HasPrefix(ft, "sql.Null") {
		expr = expr + "." + f.Type[8:]
		ft = strings.ToLower(f.Type[8:])
	}

	if t.Type != ft {
		expr = t.Type + "(" + expr + ")"
	}

	return expr
}

// schemafn takes a series of names and joins them with the schema name.
func (a *ArgType) schemafn(s string, names ...string) string {
	// escape table names
	if a.EscapeTableNames {
		for i, t := range names {
			names[i] = a.Loader.Escape(TableEsc, t)
		}
	}

	n := strings.Join(names, ".")

	if s == "" && n == "" {
		return ""
	}

	if s != "" && n != "" {
		if a.EscapeSchemaName {
			s = a.Loader.Escape(SchemaEsc, s)
		}
		s = s + "."
	}

	return s + n
}

// colname returns the ColumnName of col, optionally escaping it if
// ArgType.EscapeColumnNames is toggled.
func (a *ArgType) colname(col *models.Column) string {
	if a.EscapeColumnNames {
		return a.Loader.Escape(ColumnEsc, col.ColumnName)
	}

	return col.ColumnName
}

// hascolumn takes a list of fields and determines if field with the specified
// column name is in the list.
func (a *ArgType) hascolumn(fields []*Field, name string) bool {
	for _, f := range fields {
		if f.Col.ColumnName == name {
			return true
		}
	}

	return false
}

// hasfield takes a list of fields and determines if field with the specified
// field name is in the list.
func (a *ArgType) hasfield(fields []*Field, name string) bool {
	for _, f := range fields {
		if f.Name == name {
			return true
		}
	}

	return false
}

// getstartcount returns a starting count for numbering columsn in queries
func (a *ArgType) getstartcount(fields []*Field, pkFields []*Field) int {
	return len(fields) - len(pkFields)
}

// slice returns the result of slicing its first argument by the following
// arguments. Thus "slice x 1 5" is, in Go syntax, x[1:5]. Each
// sliced item must be a slice or array.
//
// This method is based on text/template.index
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// https://raw.githubusercontent.com/golang/go/master/LICENSE
func slice(item reflect.Value, indices ...reflect.Value) (reflect.Value, error) {
	v := indirectInterface(item)
	if !v.IsValid() {
		return reflect.Value{}, fmt.Errorf("slice of untyped nil")
	}

	if len(indices) > 2 {
		return reflect.Value{}, fmt.Errorf("slicing with greater than 2 indices is not supported")
	}
	if len(indices) < 1 {
		return v, nil
	}
	var min = 0
	var max = -1
	for c, i := range indices {
		index := indirectInterface(i)
		var isNil bool
		if v, isNil = indirect(v); isNil {
			return reflect.Value{}, fmt.Errorf("slice of nil pointer")
		}
		switch v.Kind() {
		case reflect.Array, reflect.Slice, reflect.String:
			var x int64
			switch index.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				x = index.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				x = int64(index.Uint())
			case reflect.Invalid:
				return reflect.Value{}, fmt.Errorf("cannot slice slice/array with nil")
			default:
				return reflect.Value{}, fmt.Errorf("cannot slice slice/array with type %s", index.Type())
			}
			if x < 0 || x > int64(v.Len()) {
				return reflect.Value{}, fmt.Errorf("index out of range: %d", x)
			}
			if c == 0 {
				min = int(x)
			} else {
				max = int(x)
			}
		case reflect.Invalid:
			// the loop holds invariant: v.IsValid()
			panic("unreachable")
		default:
			return reflect.Value{}, fmt.Errorf("can't index item of type %s", v.Type())
		}
	}
	if max != -1 {
		return v.Slice(min, max), nil
	}
	return v.Slice(min, v.Len()), nil
}

// sum returns the sum of the given numbers
func sum(nums ...reflect.Value) (reflect.Value, error) {
	var sum int64
	for _, i := range nums {
		num := indirectInterface(i)
		var x int64
		switch num.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			x = num.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			x = int64(num.Uint())
		case reflect.Invalid:
			return reflect.Value{}, fmt.Errorf("cannot slice slice/array with nil")
		default:
			return reflect.Value{}, fmt.Errorf("cannot slice slice/array with type %s", num.Type())
		}
		sum += x
	}
	return reflect.ValueOf(sum), nil
}

// indirectInterface returns the concrete value in an interface value,
// or else the zero reflect.Value.
// That is, if v represents the interface value x, the result is the same as reflect.ValueOf(x):
// the fact that x was an interface value is forgotten.
//
// This method is from text/template.indirectInterface
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// https://raw.githubusercontent.com/golang/go/master/LICENSE
func indirectInterface(v reflect.Value) reflect.Value {
	if v.Kind() != reflect.Interface {
		return v
	}
	if v.IsNil() {
		return reflect.Value{}
	}
	return v.Elem()
}

// indirect returns the item at the end of indirection, and a bool to indicate if it's nil.
//
// This method is from text/template.indirect
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// https://raw.githubusercontent.com/golang/go/master/LICENSE
func indirect(v reflect.Value) (rv reflect.Value, isNil bool) {
	for ; v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface; v = v.Elem() {
		if v.IsNil() {
			return v, true
		}
	}
	return v, false
}

// lowerfirst lowers only the first rune in the string
func lowerfirst(s string) string {
	var first rune
	for i, r := range s {
		if i == 0 {
			first = unicode.ToLower(r)
		} else {
			return string(first) + s[i:]
		}
	}
	return s
}

// lower lowers the string
func lower(s string) string {
	return strings.ToLower(s)
}

// contains containss the string
func contains(str, s string) bool {
	return strings.Contains(str, s)
}
