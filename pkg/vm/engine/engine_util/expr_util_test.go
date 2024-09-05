// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine_util

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGetNonIntPkValueByExpr(t *testing.T) {
	type asserts = struct {
		result bool
		data   any
		expr   *plan.Expr
		typ    types.T
	}

	testCases := []asserts{
		// a > "a"  false   only 'and', '=' function is supported
		{false, 0, MakeFunctionExprForTest(">", []*plan.Expr{
			MakeColExprForTest(0, types.T_int64),
			plan2.MakePlan2StringConstExprWithType("a"),
		}), types.T_int64},
		// a = 100  true
		{true, int64(100),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(100),
			}), types.T_int64},
		// b > 10 and a = "abc"  true
		{true, []byte("abc"),
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest(">", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
			}), types.T_char},
	}

	t.Run("test getPkValueByExpr", func(t *testing.T) {
		for i, testCase := range testCases {
			result, _, _, data := getPkValueByExpr(testCase.expr, "a", testCase.typ, true, nil)
			if result != testCase.result {
				t.Fatalf("test getPkValueByExpr at cases[%d], get result is different with expected", i)
			}
			if result {
				if a, ok := data.([]byte); ok {
					b := testCase.data.([]byte)
					if !bytes.Equal(a, b) {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				} else {
					if data != testCase.data {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				}
			}
		}
	})
}

func TestGetPKExpr(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool("", m)
	type myCase struct {
		desc     []string
		exprs    []*plan.Expr
		valExprs []*plan.Expr
	}
	tc := myCase{
		desc: []string{
			"a=10",
			"a=20 and a=10",
			"30=a and 20=a",
			"a in (1,2)",
			"b=40 and a=50",
			"a=60 or b=70",
			"b=80 and c=90",
			"a=60 or a=70",
			"a=60 or (a in (70,80))",
			"(a=10 or b=20) or a=30",
			"(a=10 or b=20) and a=30",
			"(b=10 and a=20) or a=30",
		},
		exprs: []*plan.Expr{
			// a=10
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			// a=20 and a=10
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			}),
			// 30=a and 20=a
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					plan2.MakePlan2Int64ConstExprWithType(30),
					MakeColExprForTest(0, types.T_int64),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					plan2.MakePlan2Int64ConstExprWithType(20),
					MakeColExprForTest(0, types.T_int64),
				}),
			}),
			// a in (1,2)
			MakeInExprForTest[int64](
				MakeColExprForTest(0, types.T_int64),
				[]int64{1, 2},
				types.T_int64,
				m,
			),
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(40),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(50),
				}),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(70),
				}),
			}),
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(80),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(2, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(90),
				}),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(70),
				}),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				MakeInExprForTest[int64](
					MakeColExprForTest(0, types.T_int64),
					[]int64{70, 80},
					types.T_int64,
					m,
				),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("or", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("or", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("and", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
		},
		valExprs: []*plan.Expr{
			plan2.MakePlan2Int64ConstExprWithType(10),
			plan2.MakePlan2Int64ConstExprWithType(20),
			plan2.MakePlan2Int64ConstExprWithType(30),
			plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(2)),
			plan2.MakePlan2Int64ConstExprWithType(50),
			nil,
			nil,
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(60),
							plan2.MakePlan2Int64ConstExprWithType(70),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(60),
							plan2.MakePlan2Int64VecExprWithType(m, int64(70), int64(80)),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
			nil,
			plan2.MakePlan2Int64ConstExprWithType(30),
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(20),
							plan2.MakePlan2Int64ConstExprWithType(30),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
		},
	}
	pkName := "a"
	for i, expr := range tc.exprs {
		rExpr := getPkExpr(expr, pkName, proc)
		// if rExpr != nil {
		// 	t.Logf("%s||||%s||||%s", plan2.FormatExpr(expr), plan2.FormatExpr(rExpr), tc.desc[i])
		// }
		require.Equalf(t, tc.valExprs[i], rExpr, tc.desc[i])
	}
	require.Zero(t, m.CurrNB())
}

func TestGetPkExprValue(t *testing.T) {
	m := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcessWithMPool("", m)
	type testCase struct {
		desc       []string
		exprs      []*plan.Expr
		expectVals [][]int64
		canEvals   []bool
		hasNull    []bool
	}
	equalToVecFn := func(expect []int64, actual any) bool {
		vec := vector.NewVec(types.T_any.ToType())
		_ = vec.UnmarshalBinary(actual.([]byte))
		actualVals := vector.MustFixedColWithTypeCheck[int64](vec)
		if len(expect) != len(actualVals) {
			return false
		}
		for i := range expect {
			if expect[i] != actualVals[i] {
				return false
			}
		}
		return true
	}
	equalToValFn := func(expect []int64, actual any) bool {
		if len(expect) != 1 {
			return false
		}
		actualVal := actual.(int64)
		return expect[0] == actualVal
	}

	nullExpr := plan2.MakePlan2Int64ConstExprWithType(0)
	nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true

	tc := testCase{
		desc: []string{
			"a=2 and a=1",
			"a in vec(1,2)",
			"a=2 or a=1 or a=3",
			"a in vec(1,10) or a=5 or (a=6 and a=7)",
			"a=null",
			"a=1 or a=null or a=2",
		},
		canEvals: []bool{
			true, true, true, true, false, true,
		},
		hasNull: []bool{
			false, false, false, false, true, false,
		},
		exprs: []*plan.Expr{
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(2),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
			}),
			MakeFunctionExprForTest("in", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(2)),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("or", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(2),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(1),
					}),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(3),
				}),
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("or", []*plan.Expr{
					MakeFunctionExprForTest("in", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(5),
					}),
				}),
				MakeFunctionExprForTest("and", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(6),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(7),
					}),
				}),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				nullExpr,
			}),
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("or", []*plan.Expr{
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(1),
					}),
					MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_int64),
						nullExpr,
					}),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(2),
				}),
			}),
		},
		expectVals: [][]int64{
			{2}, {1, 2}, {1, 2, 3}, {1, 5, 6, 10}, {}, {1, 2},
		},
	}
	for i, expr := range tc.exprs {
		canEval, isNull, isVec, val := getPkValueByExpr(expr, "a", types.T_int64, false, proc)
		require.Equalf(t, tc.hasNull[i], isNull, tc.desc[i])
		require.Equalf(t, tc.canEvals[i], canEval, tc.desc[i])
		if !canEval {
			continue
		}
		if isVec {
			require.Truef(t, equalToVecFn(tc.expectVals[i], val), tc.desc[i])
		} else {
			require.Truef(t, equalToValFn(tc.expectVals[i], val), tc.desc[i])
		}
	}
	expr := MakeFunctionExprForTest("in", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
	})
	canEval, _, _, _ := getPkValueByExpr(expr, "a", types.T_int64, true, proc)
	require.False(t, canEval)
	canEval, _, _, _ = getPkValueByExpr(expr, "a", types.T_int64, false, proc)
	require.True(t, canEval)

	expr = MakeFunctionExprForTest("in", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64VecExprWithType(m, int64(1)),
	})
	canEval, _, _, val := getPkValueByExpr(expr, "a", types.T_int64, true, proc)
	require.True(t, canEval)
	require.True(t, equalToValFn([]int64{1}, val))

	proc.Free()
	require.Zero(t, m.CurrNB())
}

func TestEvalExprListToVec(t *testing.T) {
	m := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcessWithMPool("", m)
	type testCase struct {
		desc     []string
		oids     []types.T
		exprs    []*plan.Expr_List
		canEvals []bool
		expects  []*vector.Vector
	}
	tc := testCase{
		desc: []string{
			"nil",
			"[i64(2), i64(1)]",
			"[i64(1), i64(2)]",
			"[i64(2), i64vec(1,10), [i64vec(4,8), i64(5)]]",
		},
		oids: []types.T{
			types.T_int64, types.T_int64, types.T_int64, types.T_int64,
		},
		exprs: []*plan.Expr_List{
			nil,
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(2),
						plan2.MakePlan2Int64ConstExprWithType(1),
					},
				},
			},
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(1),
						plan2.MakePlan2Int64ConstExprWithType(2),
					},
				},
			},
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(2),
						plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
						{
							Expr: &plan.Expr_List{
								List: &plan.ExprList{
									List: []*plan.Expr{
										plan2.MakePlan2Int64VecExprWithType(m, int64(4), int64(8)),
										plan2.MakePlan2Int64ConstExprWithType(5),
									},
								},
							},
						},
					},
				},
			},
		},
		canEvals: []bool{
			false, true, true, true,
		},
	}
	tc.expects = append(tc.expects, nil)
	vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	tc.expects = append(tc.expects, vec)
	vec = vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	tc.expects = append(tc.expects, vec)
	vec = vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	vector.AppendAny(vec, int64(4), false, m)
	vector.AppendAny(vec, int64(5), false, m)
	vector.AppendAny(vec, int64(8), false, m)
	vector.AppendAny(vec, int64(10), false, m)
	tc.expects = append(tc.expects, vec)

	for i, expr := range tc.exprs {
		// for _, e2 := range expr.List.List {
		// 	t.Log(plan2.FormatExpr(e2))
		// }
		canEval, vec, put := evalExprListToVec(tc.oids[i], expr, proc)
		require.Equalf(t, tc.canEvals[i], canEval, tc.desc[i])
		if canEval {
			require.NotNil(t, vec)
			require.Equal(t, tc.expects[i].String(), vec.String())
		} else {
			require.Equal(t, tc.expects[i], vec)
		}
		if put != nil {
			put()
		}
		if tc.expects[i] != nil {
			tc.expects[i].Free(m)
		}
	}
	proc.Free()
	require.Zero(t, m.CurrNB())
}