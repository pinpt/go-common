package datamodel

import "github.com/linkedin/goavro"

func ToGolangObjectNil(isavro bool, isoptional bool) (interface{}, bool) {
	if isavro && isoptional {
		return goavro.Union("null", nil), true
	}
	return nil, true
}

func ToGolangObject(o interface{}, isavro bool, isoptional bool, avrotype string) (interface{}, bool) {
	if o == nil {
		return ToGolangObjectNil(isavro, isoptional)
	}
	switch v := o.(type) {
	case nil:
		return ToGolangObjectNil(isavro, isoptional)
	case string, int, int8, int16, int32, int64, float32, float64, bool:
		if isavro && isoptional {
			return goavro.Union(avrotype, v), true
		}
		return v, true
	case *string:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *int:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *int8:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *int16:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *int32:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *int64:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *float32:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *float64:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case *bool:
		if isavro && isoptional {
			if v == nil {
				return ToGolangObjectNil(isavro, isoptional)
			}
			pv := *v
			return goavro.Union(avrotype, pv), true
		}
		return v, true
	case map[string]interface{}:
		return o, true
	case *map[string]interface{}:
		return v, true
	case map[string]string:
		return v, true
	case *map[string]string:
		return *v, true
	case []string, []int64, []float64, []bool:
		return o, true
	case *[]string:
		return (*(o.(*[]string))), true
	case *[]int64:
		return (*(o.(*[]int64))), true
	case *[]float64:
		return (*(o.(*[]float64))), true
	case *[]bool:
		return (*(o.(*[]bool))), true
	case []interface{}:
		a := o.([]interface{})
		arr := make([]interface{}, 0)
		for _, av := range a {
			o, _ := ToGolangObject(av, isavro, false, "")
			arr = append(arr, o)
		}
		return arr, true
	}
	return nil, false
}
