
package filterexpr

import (
    "fmt"
    "strings"
    "regexp"

    pjson "github.com/pinpt/go-common/json"
)

func toIfaceSlice(v interface{}) []interface{} {
    if v == nil {
        return nil
    }
    return v.([]interface{})
}

type Node struct {
    Key string
    Value string
    RegExp bool
    re *regexp.Regexp
}

func (n *Node) String() string {
    if n.RegExp {
        return fmt.Sprintf("Node[%s=~%s]", n.Key, n.Value)
    }
    return fmt.Sprintf("Node[%s=%s]", n.Key, n.Value)
}

func toString(val interface{}) string {
    if sval, ok := val.(string); ok {
        return sval
    }
    if sval, ok := val.(fmt.Stringer); ok {
        return sval.String()
    }
    return pjson.Stringify(val)
}

func (n *Node) Test(kv map[string]interface{}) bool {
    if val, ok := kv[n.Key]; ok {
        sval := toString(val)
        if n.RegExp {
            return n.re.MatchString(sval)
        }
        return n.Value == sval
    }
    if strings.Contains(n.Key, ".") {
        val, _ := pjson.DottedFind(n.Key, kv)
        if val == nil {
            return false
        }
        sval := toString(val)
        if n.RegExp {
            return n.re.MatchString(sval)
        }
        return n.Value == sval
    }
    return n.Value == ""
}

type Expression struct {
    Left *Node
    Conjunction string
    Right *Expression
}

func (e *Expression) String() string {
    return fmt.Sprintf("Expression[%s,%s,%s]", e.Left, e.Conjunction, e.Right)
}

func (n *Expression) Test(kv map[string]interface{}) bool {
    if n.Right == nil {
        return n.Left.Test(kv)
    }
    if n.Conjunction == "OR" {
            return n.Left.Test(kv) || n.Right.Test(kv)
    }
    return n.Left.Test(kv) && n.Right.Test(kv)
}

type ExpressionGroup struct {
    Left []*Expression
    Conjunction string
    Right []*Expression
}

func (g *ExpressionGroup) String() string {
    if g.Conjunction == "" {
       return fmt.Sprintf("ExpressionGroup[%s]", g.Left)
    }
    return fmt.Sprintf("ExpressionGroup[%s,%s,%s]", g.Left, g.Conjunction, g.Right)
}

func (n *ExpressionGroup) Test(kv map[string]interface{}) bool {
    if len(n.Right) == 0 {
        for _, e := range n.Left {
            if !e.Test(kv) {
                return false
            }
        }
        return true
    }
    left := true
    for _, e := range n.Left {
        if !e.Test(kv) {
            left = false
            break
        }
    }
    right := true
    for _, e := range n.Right {
        if !e.Test(kv) {
            right = false
            break
        }
    }
    if n.Conjunction == "OR" {
        return left || right
    }
    return left && right
}


var g = &grammar {
	rules: []*rule{
{
	name: "Object",
	pos: position{line: 129, col: 1, offset: 2661},
	expr: &actionExpr{
	pos: position{line: 129, col: 10, offset: 2672},
	run: (*parser).callonObject1,
	expr: &seqExpr{
	pos: position{line: 129, col: 10, offset: 2672},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 129, col: 10, offset: 2672},
	name: "_",
},
&labeledExpr{
	pos: position{line: 129, col: 12, offset: 2674},
	label: "val",
	expr: &ruleRefExpr{
	pos: position{line: 129, col: 16, offset: 2678},
	name: "ExpressionList",
},
},
&ruleRefExpr{
	pos: position{line: 129, col: 31, offset: 2693},
	name: "EOF",
},
	},
},
},
},
{
	name: "Value",
	pos: position{line: 133, col: 1, offset: 2722},
	expr: &actionExpr{
	pos: position{line: 133, col: 9, offset: 2732},
	run: (*parser).callonValue1,
	expr: &seqExpr{
	pos: position{line: 133, col: 9, offset: 2732},
	exprs: []interface{}{
&labeledExpr{
	pos: position{line: 133, col: 9, offset: 2732},
	label: "val",
	expr: &ruleRefExpr{
	pos: position{line: 133, col: 15, offset: 2738},
	name: "String",
},
},
&ruleRefExpr{
	pos: position{line: 133, col: 24, offset: 2747},
	name: "_",
},
	},
},
},
},
{
	name: "OrCondition",
	pos: position{line: 137, col: 1, offset: 2774},
	expr: &litMatcher{
	pos: position{line: 137, col: 15, offset: 2790},
	val: "OR",
	ignoreCase: false,
},
},
{
	name: "AndCondition",
	pos: position{line: 138, col: 1, offset: 2795},
	expr: &litMatcher{
	pos: position{line: 138, col: 16, offset: 2812},
	val: "AND",
	ignoreCase: false,
},
},
{
	name: "Condition",
	pos: position{line: 140, col: 1, offset: 2819},
	expr: &choiceExpr{
	pos: position{line: 140, col: 13, offset: 2833},
	alternatives: []interface{}{
&ruleRefExpr{
	pos: position{line: 140, col: 13, offset: 2833},
	name: "OrCondition",
},
&ruleRefExpr{
	pos: position{line: 140, col: 27, offset: 2847},
	name: "AndCondition",
},
	},
},
},
{
	name: "ExpressionList",
	pos: position{line: 142, col: 1, offset: 2861},
	expr: &actionExpr{
	pos: position{line: 142, col: 18, offset: 2880},
	run: (*parser).callonExpressionList1,
	expr: &seqExpr{
	pos: position{line: 142, col: 18, offset: 2880},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 142, col: 18, offset: 2880},
	name: "_",
},
&labeledExpr{
	pos: position{line: 142, col: 20, offset: 2882},
	label: "vals",
	expr: &seqExpr{
	pos: position{line: 142, col: 27, offset: 2889},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 142, col: 27, offset: 2889},
	name: "ExpressionGroup",
},
&zeroOrOneExpr{
	pos: position{line: 142, col: 43, offset: 2905},
	expr: &seqExpr{
	pos: position{line: 142, col: 45, offset: 2907},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 142, col: 45, offset: 2907},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 142, col: 47, offset: 2909},
	name: "Condition",
},
&ruleRefExpr{
	pos: position{line: 142, col: 57, offset: 2919},
	name: "ExpressionGroup",
},
	},
},
},
	},
},
},
	},
},
},
},
{
	name: "ExpressionGroup",
	pos: position{line: 154, col: 1, offset: 3258},
	expr: &actionExpr{
	pos: position{line: 154, col: 19, offset: 3278},
	run: (*parser).callonExpressionGroup1,
	expr: &seqExpr{
	pos: position{line: 154, col: 19, offset: 3278},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 154, col: 19, offset: 3278},
	name: "_",
},
&labeledExpr{
	pos: position{line: 154, col: 21, offset: 3280},
	label: "vals",
	expr: &oneOrMoreExpr{
	pos: position{line: 154, col: 26, offset: 3285},
	expr: &seqExpr{
	pos: position{line: 154, col: 28, offset: 3287},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 154, col: 28, offset: 3287},
	name: "_",
},
&zeroOrOneExpr{
	pos: position{line: 154, col: 30, offset: 3289},
	expr: &ruleRefExpr{
	pos: position{line: 154, col: 30, offset: 3289},
	name: "LeftParen",
},
},
&ruleRefExpr{
	pos: position{line: 154, col: 41, offset: 3300},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 154, col: 43, offset: 3302},
	name: "Expression",
},
&ruleRefExpr{
	pos: position{line: 154, col: 54, offset: 3313},
	name: "_",
},
&zeroOrOneExpr{
	pos: position{line: 154, col: 56, offset: 3315},
	expr: &ruleRefExpr{
	pos: position{line: 154, col: 56, offset: 3315},
	name: "RightParen",
},
},
&ruleRefExpr{
	pos: position{line: 154, col: 68, offset: 3327},
	name: "_",
},
	},
},
},
},
	},
},
},
},
{
	name: "Separator",
	pos: position{line: 165, col: 1, offset: 3626},
	expr: &litMatcher{
	pos: position{line: 165, col: 13, offset: 3640},
	val: ":",
	ignoreCase: false,
},
},
{
	name: "NodeVal",
	pos: position{line: 167, col: 1, offset: 3645},
	expr: &actionExpr{
	pos: position{line: 167, col: 11, offset: 3657},
	run: (*parser).callonNodeVal1,
	expr: &seqExpr{
	pos: position{line: 167, col: 11, offset: 3657},
	exprs: []interface{}{
&labeledExpr{
	pos: position{line: 167, col: 11, offset: 3657},
	label: "val",
	expr: &choiceExpr{
	pos: position{line: 167, col: 17, offset: 3663},
	alternatives: []interface{}{
&ruleRefExpr{
	pos: position{line: 167, col: 17, offset: 3663},
	name: "String",
},
&ruleRefExpr{
	pos: position{line: 167, col: 26, offset: 3672},
	name: "Number",
},
&ruleRefExpr{
	pos: position{line: 167, col: 35, offset: 3681},
	name: "Bool",
},
&ruleRefExpr{
	pos: position{line: 167, col: 42, offset: 3688},
	name: "Regexp",
},
	},
},
},
&ruleRefExpr{
	pos: position{line: 167, col: 51, offset: 3697},
	name: "_",
},
	},
},
},
},
{
	name: "Node",
	pos: position{line: 171, col: 1, offset: 3724},
	expr: &actionExpr{
	pos: position{line: 171, col: 8, offset: 3733},
	run: (*parser).callonNode1,
	expr: &seqExpr{
	pos: position{line: 171, col: 8, offset: 3733},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 171, col: 8, offset: 3733},
	name: "_",
},
&labeledExpr{
	pos: position{line: 171, col: 10, offset: 3735},
	label: "vals",
	expr: &seqExpr{
	pos: position{line: 171, col: 17, offset: 3742},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 171, col: 17, offset: 3742},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 171, col: 19, offset: 3744},
	name: "Label",
},
&ruleRefExpr{
	pos: position{line: 171, col: 25, offset: 3750},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 171, col: 27, offset: 3752},
	name: "Separator",
},
&ruleRefExpr{
	pos: position{line: 171, col: 37, offset: 3762},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 171, col: 39, offset: 3764},
	name: "NodeVal",
},
&ruleRefExpr{
	pos: position{line: 171, col: 47, offset: 3772},
	name: "_",
},
	},
},
},
	},
},
},
},
{
	name: "Expression",
	pos: position{line: 184, col: 1, offset: 4040},
	expr: &actionExpr{
	pos: position{line: 184, col: 14, offset: 4055},
	run: (*parser).callonExpression1,
	expr: &seqExpr{
	pos: position{line: 184, col: 14, offset: 4055},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 184, col: 14, offset: 4055},
	name: "_",
},
&labeledExpr{
	pos: position{line: 184, col: 16, offset: 4057},
	label: "vals",
	expr: &seqExpr{
	pos: position{line: 184, col: 23, offset: 4064},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 184, col: 23, offset: 4064},
	name: "Node",
},
&zeroOrMoreExpr{
	pos: position{line: 184, col: 28, offset: 4069},
	expr: &seqExpr{
	pos: position{line: 184, col: 30, offset: 4071},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 184, col: 30, offset: 4071},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 184, col: 32, offset: 4073},
	name: "Condition",
},
&ruleRefExpr{
	pos: position{line: 184, col: 42, offset: 4083},
	name: "_",
},
&ruleRefExpr{
	pos: position{line: 184, col: 44, offset: 4085},
	name: "Node",
},
	},
},
},
	},
},
},
	},
},
},
},
{
	name: "Label",
	pos: position{line: 213, col: 1, offset: 4983},
	expr: &actionExpr{
	pos: position{line: 213, col: 9, offset: 4993},
	run: (*parser).callonLabel1,
	expr: &oneOrMoreExpr{
	pos: position{line: 213, col: 11, offset: 4995},
	expr: &charClassMatcher{
	pos: position{line: 213, col: 11, offset: 4995},
	val: "[a-zA-Z0-9\\\\._\\\\-]",
	chars: []rune{'\\','.','_','\\','-',},
	ranges: []rune{'a','z','A','Z','0','9',},
	ignoreCase: false,
	inverted: false,
},
},
},
},
{
	name: "String",
	pos: position{line: 217, col: 1, offset: 5053},
	expr: &actionExpr{
	pos: position{line: 217, col: 10, offset: 5064},
	run: (*parser).callonString1,
	expr: &seqExpr{
	pos: position{line: 217, col: 10, offset: 5064},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 217, col: 10, offset: 5064},
	val: "\"",
	ignoreCase: false,
},
&zeroOrMoreExpr{
	pos: position{line: 217, col: 14, offset: 5068},
	expr: &choiceExpr{
	pos: position{line: 217, col: 16, offset: 5070},
	alternatives: []interface{}{
&seqExpr{
	pos: position{line: 217, col: 16, offset: 5070},
	exprs: []interface{}{
&notExpr{
	pos: position{line: 217, col: 16, offset: 5070},
	expr: &ruleRefExpr{
	pos: position{line: 217, col: 17, offset: 5071},
	name: "EscapedChar",
},
},
&anyMatcher{
	line: 217, col: 29, offset: 5083,
},
	},
},
&seqExpr{
	pos: position{line: 217, col: 33, offset: 5087},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 217, col: 33, offset: 5087},
	val: "\\",
	ignoreCase: false,
},
&ruleRefExpr{
	pos: position{line: 217, col: 38, offset: 5092},
	name: "EscapeSequence",
},
	},
},
	},
},
},
&litMatcher{
	pos: position{line: 217, col: 56, offset: 5110},
	val: "\"",
	ignoreCase: false,
},
	},
},
},
},
{
	name: "LeftParen",
	pos: position{line: 222, col: 1, offset: 5228},
	expr: &litMatcher{
	pos: position{line: 222, col: 13, offset: 5242},
	val: "(",
	ignoreCase: false,
},
},
{
	name: "RightParen",
	pos: position{line: 224, col: 1, offset: 5247},
	expr: &litMatcher{
	pos: position{line: 224, col: 14, offset: 5262},
	val: ")",
	ignoreCase: false,
},
},
{
	name: "EscapedChar",
	pos: position{line: 226, col: 1, offset: 5267},
	expr: &charClassMatcher{
	pos: position{line: 226, col: 15, offset: 5283},
	val: "[\\x00-\\x1f\"\\\\]",
	chars: []rune{'"','\\',},
	ranges: []rune{'\x00','\x1f',},
	ignoreCase: false,
	inverted: false,
},
},
{
	name: "EscapeSequence",
	pos: position{line: 228, col: 1, offset: 5299},
	expr: &choiceExpr{
	pos: position{line: 228, col: 18, offset: 5318},
	alternatives: []interface{}{
&ruleRefExpr{
	pos: position{line: 228, col: 18, offset: 5318},
	name: "SingleCharEscape",
},
&ruleRefExpr{
	pos: position{line: 228, col: 37, offset: 5337},
	name: "UnicodeEscape",
},
	},
},
},
{
	name: "SingleCharEscape",
	pos: position{line: 230, col: 1, offset: 5352},
	expr: &charClassMatcher{
	pos: position{line: 230, col: 20, offset: 5373},
	val: "[\"\\\\/bfnrt]",
	chars: []rune{'"','\\','/','b','f','n','r','t',},
	ignoreCase: false,
	inverted: false,
},
},
{
	name: "UnicodeEscape",
	pos: position{line: 232, col: 1, offset: 5386},
	expr: &seqExpr{
	pos: position{line: 232, col: 17, offset: 5404},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 232, col: 17, offset: 5404},
	val: "u",
	ignoreCase: false,
},
&ruleRefExpr{
	pos: position{line: 232, col: 21, offset: 5408},
	name: "HexDigit",
},
&ruleRefExpr{
	pos: position{line: 232, col: 30, offset: 5417},
	name: "HexDigit",
},
&ruleRefExpr{
	pos: position{line: 232, col: 39, offset: 5426},
	name: "HexDigit",
},
&ruleRefExpr{
	pos: position{line: 232, col: 48, offset: 5435},
	name: "HexDigit",
},
	},
},
},
{
	name: "DecimalDigit",
	pos: position{line: 234, col: 1, offset: 5445},
	expr: &charClassMatcher{
	pos: position{line: 234, col: 16, offset: 5462},
	val: "[0-9]",
	ranges: []rune{'0','9',},
	ignoreCase: false,
	inverted: false,
},
},
{
	name: "NonZeroDecimalDigit",
	pos: position{line: 236, col: 1, offset: 5469},
	expr: &charClassMatcher{
	pos: position{line: 236, col: 23, offset: 5493},
	val: "[1-9]",
	ranges: []rune{'1','9',},
	ignoreCase: false,
	inverted: false,
},
},
{
	name: "Number",
	pos: position{line: 238, col: 1, offset: 5500},
	expr: &actionExpr{
	pos: position{line: 238, col: 10, offset: 5511},
	run: (*parser).callonNumber1,
	expr: &seqExpr{
	pos: position{line: 238, col: 10, offset: 5511},
	exprs: []interface{}{
&zeroOrOneExpr{
	pos: position{line: 238, col: 10, offset: 5511},
	expr: &litMatcher{
	pos: position{line: 238, col: 10, offset: 5511},
	val: "-",
	ignoreCase: false,
},
},
&ruleRefExpr{
	pos: position{line: 238, col: 15, offset: 5516},
	name: "Integer",
},
&zeroOrOneExpr{
	pos: position{line: 238, col: 23, offset: 5524},
	expr: &seqExpr{
	pos: position{line: 238, col: 25, offset: 5526},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 238, col: 25, offset: 5526},
	val: ".",
	ignoreCase: false,
},
&oneOrMoreExpr{
	pos: position{line: 238, col: 29, offset: 5530},
	expr: &ruleRefExpr{
	pos: position{line: 238, col: 29, offset: 5530},
	name: "DecimalDigit",
},
},
	},
},
},
&zeroOrOneExpr{
	pos: position{line: 238, col: 46, offset: 5547},
	expr: &ruleRefExpr{
	pos: position{line: 238, col: 46, offset: 5547},
	name: "Exponent",
},
},
	},
},
},
},
{
	name: "Integer",
	pos: position{line: 242, col: 1, offset: 5612},
	expr: &choiceExpr{
	pos: position{line: 242, col: 11, offset: 5624},
	alternatives: []interface{}{
&litMatcher{
	pos: position{line: 242, col: 11, offset: 5624},
	val: "0",
	ignoreCase: false,
},
&seqExpr{
	pos: position{line: 242, col: 17, offset: 5630},
	exprs: []interface{}{
&ruleRefExpr{
	pos: position{line: 242, col: 17, offset: 5630},
	name: "NonZeroDecimalDigit",
},
&zeroOrMoreExpr{
	pos: position{line: 242, col: 37, offset: 5650},
	expr: &ruleRefExpr{
	pos: position{line: 242, col: 37, offset: 5650},
	name: "DecimalDigit",
},
},
	},
},
	},
},
},
{
	name: "Exponent",
	pos: position{line: 244, col: 1, offset: 5665},
	expr: &seqExpr{
	pos: position{line: 244, col: 12, offset: 5678},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 244, col: 12, offset: 5678},
	val: "e",
	ignoreCase: true,
},
&zeroOrOneExpr{
	pos: position{line: 244, col: 17, offset: 5683},
	expr: &charClassMatcher{
	pos: position{line: 244, col: 17, offset: 5683},
	val: "[+-]",
	chars: []rune{'+','-',},
	ignoreCase: false,
	inverted: false,
},
},
&oneOrMoreExpr{
	pos: position{line: 244, col: 23, offset: 5689},
	expr: &ruleRefExpr{
	pos: position{line: 244, col: 23, offset: 5689},
	name: "DecimalDigit",
},
},
	},
},
},
{
	name: "Bool",
	pos: position{line: 246, col: 1, offset: 5704},
	expr: &choiceExpr{
	pos: position{line: 246, col: 8, offset: 5713},
	alternatives: []interface{}{
&actionExpr{
	pos: position{line: 246, col: 8, offset: 5713},
	run: (*parser).callonBool2,
	expr: &litMatcher{
	pos: position{line: 246, col: 8, offset: 5713},
	val: "true",
	ignoreCase: false,
},
},
&actionExpr{
	pos: position{line: 246, col: 38, offset: 5743},
	run: (*parser).callonBool4,
	expr: &litMatcher{
	pos: position{line: 246, col: 38, offset: 5743},
	val: "false",
	ignoreCase: false,
},
},
	},
},
},
{
	name: "HexDigit",
	pos: position{line: 248, col: 1, offset: 5774},
	expr: &charClassMatcher{
	pos: position{line: 248, col: 12, offset: 5787},
	val: "[0-9a-f]i",
	ranges: []rune{'0','9','a','f',},
	ignoreCase: true,
	inverted: false,
},
},
{
	name: "Regexp",
	pos: position{line: 250, col: 1, offset: 5798},
	expr: &actionExpr{
	pos: position{line: 250, col: 10, offset: 5809},
	run: (*parser).callonRegexp1,
	expr: &seqExpr{
	pos: position{line: 250, col: 10, offset: 5809},
	exprs: []interface{}{
&litMatcher{
	pos: position{line: 250, col: 10, offset: 5809},
	val: "/",
	ignoreCase: false,
},
&oneOrMoreExpr{
	pos: position{line: 250, col: 14, offset: 5813},
	expr: &charClassMatcher{
	pos: position{line: 250, col: 14, offset: 5813},
	val: "[A-Za-z0-9^$-._\\\\(\\\\)\\\\?\\\\+{} ]",
	chars: []rune{'^','_','\\','(','\\',')','\\','?','\\','+','{','}',' ',},
	ranges: []rune{'A','Z','a','z','0','9','$','.',},
	ignoreCase: false,
	inverted: false,
},
},
&litMatcher{
	pos: position{line: 250, col: 47, offset: 5846},
	val: "/",
	ignoreCase: false,
},
	},
},
},
},
{
	name: "_",
	displayName: "\"whitespace\"",
	pos: position{line: 254, col: 1, offset: 5915},
	expr: &zeroOrMoreExpr{
	pos: position{line: 254, col: 18, offset: 5934},
	expr: &charClassMatcher{
	pos: position{line: 254, col: 18, offset: 5934},
	val: "[ \\t\\r\\n]",
	chars: []rune{' ','\t','\r','\n',},
	ignoreCase: false,
	inverted: false,
},
},
},
{
	name: "EOF",
	pos: position{line: 256, col: 1, offset: 5946},
	expr: &notExpr{
	pos: position{line: 256, col: 7, offset: 5954},
	expr: &anyMatcher{
	line: 256, col: 8, offset: 5955,
},
},
},
	},
}
func (c *current) onObject1(val interface{}) (interface{}, error) {
    return val, nil
}

func (p *parser) callonObject1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onObject1(stack["val"])
}

func (c *current) onValue1(val interface{}) (interface{}, error) {
    return val, nil
}

func (p *parser) callonValue1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onValue1(stack["val"])
}

func (c *current) onExpressionList1(vals interface{}) (interface{}, error) {
    valsSl := toIfaceSlice(vals)
    res := &ExpressionGroup{}
    res.Left = valsSl[0].([]*Expression)
    if len(valsSl) > 1 && valsSl[1] != nil {
        group := toIfaceSlice(valsSl[1])
        res.Conjunction = string(group[1].([]byte))
        res.Right = group[2].([]*Expression)
    }
    return res, nil
}

func (p *parser) callonExpressionList1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpressionList1(stack["vals"])
}

func (c *current) onExpressionGroup1(vals interface{}) (interface{}, error) {
    valsSl := toIfaceSlice(vals)
    res := make([]*Expression, 0)
    //fmt.Println("expression group", len(valsSl))
    for _, val := range valsSl {
        //fmt.Println("exp group", i, "val", val)
        res = append(res, toIfaceSlice(val)[3].(*Expression))
    }
    return res, nil
}

func (p *parser) callonExpressionGroup1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpressionGroup1(stack["vals"])
}

func (c *current) onNodeVal1(val interface{}) (interface{}, error) {
    return val, nil
}

func (p *parser) callonNodeVal1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNodeVal1(stack["val"])
}

func (c *current) onNode1(vals interface{}) (interface{}, error) {
    var res Node
    valsSl := toIfaceSlice(vals)
    res.Key = valsSl[1].(string)
    if re, ok := valsSl[5].(*regexp.Regexp); ok {
        res.re = re
        res.RegExp = true
    } else {
        res.Value = toString(valsSl[5])
    }
    return &res, nil
}

func (p *parser) callonNode1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNode1(stack["vals"])
}

func (c *current) onExpression1(vals interface{}) (interface{}, error) {
    valsSl := toIfaceSlice(vals)
    left := valsSl[0].(*Node)
    var right *Expression
    var current *Expression
    var conjunction string
    //fmt.Println("len", len(valsSl))
    if len(valsSl) > 1 {
        grouping := toIfaceSlice(valsSl[1])
        for _, group := range grouping {
            igroup := toIfaceSlice(group)
            //fmt.Println(i, "group", igroup)
            conjunction = string(igroup[1].([]byte))
            //fmt.Println(i, conjunction)
            iright := igroup[3].(*Node)
            //fmt.Println(i, iright)
            newexp := &Expression{iright, conjunction, nil}
            if right == nil {
                right = newexp
                current = newexp
            } else {
                current.Right = newexp
                current = newexp
            }
        }
    }
    return &Expression{left, conjunction, right}, nil
}

func (p *parser) callonExpression1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpression1(stack["vals"])
}

func (c *current) onLabel1() (interface{}, error) {
    return string(c.text), nil
}

func (p *parser) callonLabel1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onLabel1()
}

func (c *current) onString1() (interface{}, error) {
    c.text = bytes.Replace(c.text, []byte(`\/`), []byte(`/`), -1)
    return strconv.Unquote(string(c.text))
}

func (p *parser) callonString1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onString1()
}

func (c *current) onNumber1() (interface{}, error) {
    return strconv.ParseFloat(string(c.text), 64)
}

func (p *parser) callonNumber1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onNumber1()
}

func (c *current) onBool2() (interface{}, error) {
 return true, nil 
}

func (p *parser) callonBool2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onBool2()
}

func (c *current) onBool4() (interface{}, error) {
 return false, nil 
}

func (p *parser) callonBool4() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onBool4()
}

func (c *current) onRegexp1() (interface{}, error) {
    return regexp.Compile(string(c.text[1: len(c.text)-1]))
}

func (p *parser) callonRegexp1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRegexp1()
}


var (
	// errNoRule is returned when the grammar to parse has no rule.
	errNoRule          = errors.New("grammar has no rule")

	// errInvalidEncoding is returned when the source is not properly
	// utf8-encoded.
	errInvalidEncoding = errors.New("invalid encoding")

	// errNoMatch is returned if no match could be found.
	errNoMatch         = errors.New("no match found")
)

// Option is a function that can set an option on the parser. It returns
// the previous setting as an Option.
type Option func(*parser) Option

// Debug creates an Option to set the debug flag to b. When set to true,
// debugging information is printed to stdout while parsing.
//
// The default is false.
func Debug(b bool) Option {
	return func(p *parser) Option {
		old := p.debug
		p.debug = b
		return Debug(old)
	}
}

// Memoize creates an Option to set the memoize flag to b. When set to true,
// the parser will cache all results so each expression is evaluated only
// once. This guarantees linear parsing time even for pathological cases,
// at the expense of more memory and slower times for typical cases.
//
// The default is false.
func Memoize(b bool) Option {
	return func(p *parser) Option {
		old := p.memoize
		p.memoize = b
		return Memoize(old)
	}
}

// Recover creates an Option to set the recover flag to b. When set to
// true, this causes the parser to recover from panics and convert it
// to an error. Setting it to false can be useful while debugging to
// access the full stack trace.
//
// The default is true.
func Recover(b bool) Option {
	return func(p *parser) Option {
		old := p.recover
		p.recover = b
		return Recover(old)
	}
}

// ParseFile parses the file identified by filename.
func ParseFile(filename string, opts ...Option) (interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseReader(filename, f, opts...)
}

// ParseReader parses the data from r using filename as information in the
// error messages.
func ParseReader(filename string, r io.Reader, opts ...Option) (interface{}, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Parse(filename, b, opts...)
}

// Parse parses the data from b using filename as information in the
// error messages.
func Parse(filename string, b []byte, opts ...Option) (interface{}, error) {
	return newParser(filename, b, opts...).parse(g)
}

// position records a position in the text.
type position struct {
	line, col, offset int
}

func (p position) String() string {
	return fmt.Sprintf("%d:%d [%d]", p.line, p.col, p.offset)
}

// savepoint stores all state required to go back to this point in the
// parser.
type savepoint struct {
	position
	rn rune
	w  int
}

type current struct {
	pos  position // start position of the match
	text []byte   // raw text of the match
}

// the AST types...

type grammar struct {
	pos   position
	rules []*rule
}

type rule struct {
	pos         position
	name        string
	displayName string
	expr        interface{}
}

type choiceExpr struct {
	pos          position
	alternatives []interface{}
}

type actionExpr struct {
	pos    position
	expr   interface{}
	run    func(*parser) (interface{}, error)
}

type seqExpr struct {
	pos   position
	exprs []interface{}
}

type labeledExpr struct {
	pos   position
	label string
	expr  interface{}
}

type expr struct {
	pos  position
	expr interface{}
}

type andExpr expr
type notExpr expr
type zeroOrOneExpr expr
type zeroOrMoreExpr expr
type oneOrMoreExpr expr

type ruleRefExpr struct {
	pos  position
	name string
}

type andCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type notCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type litMatcher struct {
	pos        position
	val        string
	ignoreCase bool
}

type charClassMatcher struct {
	pos        position
	val        string
	chars      []rune
	ranges     []rune
	classes    []*unicode.RangeTable
	ignoreCase bool
	inverted   bool
}

type anyMatcher position

// errList cumulates the errors found by the parser.
type errList []error

func (e *errList) add(err error) {
	*e = append(*e, err)
}

func (e errList) err() error {
	if len(e) == 0 {
		return nil
	}
	e.dedupe()
	return e
}

func (e *errList) dedupe() {
	var cleaned []error
	set := make(map[string]bool)
	for _, err := range *e {
		if msg := err.Error(); !set[msg] {
			set[msg] = true
			cleaned = append(cleaned, err)
		}
	}
	*e = cleaned
}

func (e errList) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		var buf bytes.Buffer

		for i, err := range e {
			if i > 0 {
				buf.WriteRune('\n')
			}
			buf.WriteString(err.Error())
		}
		return buf.String()
	}
}

// parserError wraps an error with a prefix indicating the rule in which
// the error occurred. The original error is stored in the Inner field.
type parserError struct {
	Inner  error
	pos    position
	prefix string
}

// Error returns the error message.
func (p *parserError) Error() string {
	return p.prefix + ": " + p.Inner.Error()
}

// newParser creates a parser with the specified input source and options.
func newParser(filename string, b []byte, opts ...Option) *parser {
	p := &parser{
		filename: filename,
		errs: new(errList),
		data: b,
		pt: savepoint{position: position{line: 1}},
		recover: true,
	}
	p.setOptions(opts)
	return p
}

// setOptions applies the options to the parser.
func (p *parser) setOptions(opts []Option) {
	for _, opt := range opts {
		opt(p)
	}
}

type resultTuple struct {
	v interface{}
	b bool
	end savepoint
}

type parser struct {
	filename string
	pt       savepoint
	cur      current

	data []byte
	errs *errList

	recover bool
	debug bool
	depth  int

	memoize bool
	// memoization table for the packrat algorithm:
	// map[offset in source] map[expression or rule] {value, match}
	memo map[int]map[interface{}]resultTuple

	// rules table, maps the rule identifier to the rule node
	rules  map[string]*rule
	// variables stack, map of label to value
	vstack []map[string]interface{}
	// rule stack, allows identification of the current rule in errors
	rstack []*rule

	// stats
	exprCnt int
}

// push a variable set on the vstack.
func (p *parser) pushV() {
	if cap(p.vstack) == len(p.vstack) {
		// create new empty slot in the stack
		p.vstack = append(p.vstack, nil)
	} else {
		// slice to 1 more
		p.vstack = p.vstack[:len(p.vstack)+1]
	}

	// get the last args set
	m := p.vstack[len(p.vstack)-1]
	if m != nil && len(m) == 0 {
		// empty map, all good
		return
	}

	m = make(map[string]interface{})
	p.vstack[len(p.vstack)-1] = m
}

// pop a variable set from the vstack.
func (p *parser) popV() {
	// if the map is not empty, clear it
	m := p.vstack[len(p.vstack)-1]
	if len(m) > 0 {
		// GC that map
		p.vstack[len(p.vstack)-1] = nil
	}
	p.vstack = p.vstack[:len(p.vstack)-1]
}

func (p *parser) print(prefix, s string) string {
	if !p.debug {
		return s
	}

	fmt.Printf("%s %d:%d:%d: %s [%#U]\n",
		prefix, p.pt.line, p.pt.col, p.pt.offset, s, p.pt.rn)
	return s
}

func (p *parser) in(s string) string {
	p.depth++
	return p.print(strings.Repeat(" ", p.depth) + ">", s)
}

func (p *parser) out(s string) string {
	p.depth--
	return p.print(strings.Repeat(" ", p.depth) + "<", s)
}

func (p *parser) addErr(err error) {
	p.addErrAt(err, p.pt.position)
}

func (p *parser) addErrAt(err error, pos position) {
	var buf bytes.Buffer
	if p.filename != "" {
		buf.WriteString(p.filename)
	}
	if buf.Len() > 0 {
		buf.WriteString(":")
	}
	buf.WriteString(fmt.Sprintf("%d:%d (%d)", pos.line, pos.col, pos.offset))
	if len(p.rstack) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(": ")
		}
		rule := p.rstack[len(p.rstack)-1]
		if rule.displayName != "" {
			buf.WriteString("rule " + rule.displayName)
		} else {
			buf.WriteString("rule " + rule.name)
		}
	}
	pe := &parserError{Inner: err, pos: pos, prefix: buf.String()}
	p.errs.add(pe)
}

// read advances the parser to the next rune.
func (p *parser) read() {
	p.pt.offset += p.pt.w
	rn, n := utf8.DecodeRune(p.data[p.pt.offset:])
	p.pt.rn = rn
	p.pt.w = n
	p.pt.col++
	if rn == '\n' {
		p.pt.line++
		p.pt.col = 0
	}

	if rn == utf8.RuneError {
		if n == 1 {
			p.addErr(errInvalidEncoding)
		}
	}
}

// restore parser position to the savepoint pt.
func (p *parser) restore(pt savepoint) {
	if p.debug {
		defer p.out(p.in("restore"))
	}
	if pt.offset == p.pt.offset {
		return
	}
	p.pt = pt
}

// get the slice of bytes from the savepoint start to the current position.
func (p *parser) sliceFrom(start savepoint) []byte {
	return p.data[start.position.offset:p.pt.position.offset]
}

func (p *parser) getMemoized(node interface{}) (resultTuple, bool) {
	if len(p.memo) == 0 {
		return resultTuple{}, false
	}
	m := p.memo[p.pt.offset]
	if len(m) == 0 {
		return resultTuple{}, false
	}
	res, ok := m[node]
	return res, ok
}

func (p *parser) setMemoized(pt savepoint, node interface{}, tuple resultTuple) {
	if p.memo == nil {
		p.memo = make(map[int]map[interface{}]resultTuple)
	}
	m := p.memo[pt.offset]
	if m == nil {
		m = make(map[interface{}]resultTuple)
		p.memo[pt.offset] = m
	}
	m[node] = tuple
}

func (p *parser) buildRulesTable(g *grammar) {
	p.rules = make(map[string]*rule, len(g.rules))
	for _, r := range g.rules {
		p.rules[r.name] = r
	}
}

func (p *parser) parse(g *grammar) (val interface{}, err error) {
	if len(g.rules) == 0 {
		p.addErr(errNoRule)
		return nil, p.errs.err()
	}

	// TODO : not super critical but this could be generated
	p.buildRulesTable(g)

	if p.recover {
		// panic can be used in action code to stop parsing immediately
		// and return the panic as an error.
		defer func() {
			if e := recover(); e != nil {
				if p.debug {
					defer p.out(p.in("panic handler"))
				}
				val = nil
				switch e := e.(type) {
				case error:
					p.addErr(e)
				default:
					p.addErr(fmt.Errorf("%v", e))
				}
				err = p.errs.err()
			}
		}()
	}

	// start rule is rule [0]
	p.read() // advance to first rune
	val, ok := p.parseRule(g.rules[0])
	if !ok {
		if len(*p.errs) == 0 {
			// make sure this doesn't go out silently
			p.addErr(errNoMatch)
		}
		return nil, p.errs.err()
	}
	return val, p.errs.err()
}

func (p *parser) parseRule(rule *rule) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRule " + rule.name))
	}

	if p.memoize {
		res, ok := p.getMemoized(rule)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
	}

	start := p.pt
	p.rstack = append(p.rstack, rule)
	p.pushV()
	val, ok := p.parseExpr(rule.expr)
	p.popV()
	p.rstack = p.rstack[:len(p.rstack)-1]
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth) + "MATCH", string(p.sliceFrom(start)))
	}

	if p.memoize {
		p.setMemoized(start, rule, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseExpr(expr interface{}) (interface{}, bool) {
	var pt savepoint
	var ok bool

	if p.memoize {
		res, ok := p.getMemoized(expr)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
		pt = p.pt
	}

	p.exprCnt++
	var val interface{}
	switch expr := expr.(type) {
	case *actionExpr:
		val, ok = p.parseActionExpr(expr)
	case *andCodeExpr:
		val, ok = p.parseAndCodeExpr(expr)
	case *andExpr:
		val, ok = p.parseAndExpr(expr)
	case *anyMatcher:
		val, ok = p.parseAnyMatcher(expr)
	case *charClassMatcher:
		val, ok = p.parseCharClassMatcher(expr)
	case *choiceExpr:
		val, ok = p.parseChoiceExpr(expr)
	case *labeledExpr:
		val, ok = p.parseLabeledExpr(expr)
	case *litMatcher:
		val, ok = p.parseLitMatcher(expr)
	case *notCodeExpr:
		val, ok = p.parseNotCodeExpr(expr)
	case *notExpr:
		val, ok = p.parseNotExpr(expr)
	case *oneOrMoreExpr:
		val, ok = p.parseOneOrMoreExpr(expr)
	case *ruleRefExpr:
		val, ok = p.parseRuleRefExpr(expr)
	case *seqExpr:
		val, ok = p.parseSeqExpr(expr)
	case *zeroOrMoreExpr:
		val, ok = p.parseZeroOrMoreExpr(expr)
	case *zeroOrOneExpr:
		val, ok = p.parseZeroOrOneExpr(expr)
	default:
		panic(fmt.Sprintf("unknown expression type %T", expr))
	}
	if p.memoize {
		p.setMemoized(pt, expr, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseActionExpr(act *actionExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseActionExpr"))
	}

	start := p.pt
	val, ok := p.parseExpr(act.expr)
	if ok {
		p.cur.pos = start.position
		p.cur.text = p.sliceFrom(start)
		actVal, err := act.run(p)
		if err != nil {
			p.addErrAt(err, start.position)
		}
		val = actVal
	}
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth) + "MATCH", string(p.sliceFrom(start)))
	}
	return val, ok
}

func (p *parser) parseAndCodeExpr(and *andCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndCodeExpr"))
	}

	ok, err := and.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, ok
}

func (p *parser) parseAndExpr(and *andExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(and.expr)
	p.popV()
	p.restore(pt)
	return nil, ok
}

func (p *parser) parseAnyMatcher(any *anyMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAnyMatcher"))
	}

	if p.pt.rn != utf8.RuneError {
		start := p.pt
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseCharClassMatcher(chr *charClassMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseCharClassMatcher"))
	}

	cur := p.pt.rn
	// can't match EOF
	if cur == utf8.RuneError {
		return nil, false
	}
	start := p.pt
	if chr.ignoreCase {
		cur = unicode.ToLower(cur)
	}

	// try to match in the list of available chars
	for _, rn := range chr.chars {
		if rn == cur {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of ranges
	for i := 0; i < len(chr.ranges); i += 2 {
		if cur >= chr.ranges[i] && cur <= chr.ranges[i+1] {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of Unicode classes
	for _, cl := range chr.classes {
		if unicode.Is(cl, cur) {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	if chr.inverted {
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseChoiceExpr(ch *choiceExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseChoiceExpr"))
	}

	for _, alt := range ch.alternatives {
		p.pushV()
		val, ok := p.parseExpr(alt)
		p.popV()
		if ok {
			return val, ok
		}
	}
	return nil, false
}

func (p *parser) parseLabeledExpr(lab *labeledExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLabeledExpr"))
	}

	p.pushV()
	val, ok := p.parseExpr(lab.expr)
	p.popV()
	if ok && lab.label != "" {
		m := p.vstack[len(p.vstack)-1]
		m[lab.label] = val
	}
	return val, ok
}

func (p *parser) parseLitMatcher(lit *litMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLitMatcher"))
	}

	start := p.pt
	for _, want := range lit.val {
		cur := p.pt.rn
		if lit.ignoreCase {
			cur = unicode.ToLower(cur)
		}
		if cur != want {
			p.restore(start)
			return nil, false
		}
		p.read()
	}
	return p.sliceFrom(start), true
}

func (p *parser) parseNotCodeExpr(not *notCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotCodeExpr"))
	}

	ok, err := not.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, !ok
}

func (p *parser) parseNotExpr(not *notExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(not.expr)
	p.popV()
	p.restore(pt)
	return nil, !ok
}

func (p *parser) parseOneOrMoreExpr(expr *oneOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseOneOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			if len(vals) == 0 {
				// did not match once, no match
				return nil, false
			}
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseRuleRefExpr(ref *ruleRefExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRuleRefExpr " + ref.name))
	}

	if ref.name == "" {
		panic(fmt.Sprintf("%s: invalid rule: missing name", ref.pos))
	}

	rule := p.rules[ref.name]
	if rule == nil {
		p.addErr(fmt.Errorf("undefined rule: %s", ref.name))
		return nil, false
	}
	return p.parseRule(rule)
}

func (p *parser) parseSeqExpr(seq *seqExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseSeqExpr"))
	}

	var vals []interface{}

	pt := p.pt
	for _, expr := range seq.exprs {
		val, ok := p.parseExpr(expr)
		if !ok {
			p.restore(pt)
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func (p *parser) parseZeroOrMoreExpr(expr *zeroOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseZeroOrOneExpr(expr *zeroOrOneExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrOneExpr"))
	}

	p.pushV()
	val, _ := p.parseExpr(expr.expr)
	p.popV()
	// whether it matched or not, consider it a match
	return val, true
}

func rangeTable(class string) *unicode.RangeTable {
	if rt, ok := unicode.Categories[class]; ok {
		return rt
	}
	if rt, ok := unicode.Properties[class]; ok {
		return rt
	}
	if rt, ok := unicode.Scripts[class]; ok {
		return rt
	}

	// cannot happen
	panic(fmt.Sprintf("invalid Unicode class: %s", class))
}

