use std::vec;

use datafusion::{
    logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule, scalar::ScalarValue,
};
use datafusion::{logical_plan::Operator, optimizer::utils};

use datafusion::error::Result as DFResult;
use datafusion::logical_plan::Expr;
use egg::{rewrite as rw, *};

pub struct Tokomak {}

impl Tokomak {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
pub type EGraph = egg::EGraph<Tokomak, ()>;

pub fn rules() -> Vec<Rewrite<TokomakExpr, ()>> {
    // TODO: add useful properties
    return vec![
        rw!("commute-add"; "(+ ?x ?y)" => "(+ ?y ?x)"),
        rw!("commute-mul"; "(* ?x ?y)" => "(* ?y ?x)"),
        rw!("commute-and"; "(and ?x ?y)" => "(and ?y ?x)"),
        rw!("commute-or"; "(or ?x ?y)" => "(or ?y ?x)"),
        rw!("commute-eq"; "(= ?x ?y)" => "(= ?y ?x)"),
        rw!("commute-neq"; "(<> ?x ?y)" => "(<> ?y ?x)"),
        rw!("converse-gt"; "(> ?x ?y)"=> "(< ?x ?y)"),
        rw!("converse-gte"; "(>= ?x ?y)"=> "(<= ?x ?y)"),
        rw!("converse-lt"; "(< ?x ?y)"=> "(> ?x ?y)"),
        rw!("converse-lte"; "(<= ?x ?y)"=> "(>= ?x ?y)"),
        rw!("add-0"; "(+ ?x 0)" => "?x"),
        rw!("add-assoc"; "(+ (+ ?a ?b) ?c)" => "(+ ?a (+ ?b ?c))"),
        rw!("minus-0"; "(- ?x 0)" => "?x"),
        rw!("mul-0"; "(* ?x 0)" => "0"),
        rw!("mul-1"; "(* ?x 1)" => "?x"),
        rw!("div-1"; "(/ ?x 1)" => "?x"),
        rw!("dist-and-or"; "(or (and ?a ?b) (and ?a ?c))" => "(and ?a (or ?b ?c))"),
        rw!("dist-or-and"; "(and (or ?a ?b) (or ?a ?c))" => "(or ?a (and ?b ?c))"),
        rw!("not-not"; "(not (not ?x))" => "?x"),
        rw!("or-same"; "(or ?x ?x)" => "?x"),
        rw!("and-same"; "(and ?x ?x)" => "?x"),
        rw!("cancel-sub"; "(- ?a ?a)" => "0"),
        rw!("and-true"; "(and true ?x)"=> "?x"),
        rw!("0-minus"; "(- 0 ?x)"=> "(negative ?x)"),
        rw!("and-false"; "(and false ?x)"=> "false"),
        rw!("or-false"; "(or false ?x)"=> "?x"),
        rw!("or-true"; "(or true ?x)"=> "true"),
    ];
}

define_language! {
    /// Supported expressions in Tokomak
    pub enum TokomakExpr {
        "+" = Plus([Id; 2]),
        "-" = Minus([Id; 2]),
        "*" = Multiply([Id; 2]),
        "/" = Divide([Id; 2]),
        "%" = Modulus([Id; 2]),
        "not" = Not(Id),
        "or" = Or([Id; 2]),
        "and" = And([Id; 2]),
        "=" = Eq([Id; 2]),
        "<>" = NotEq([Id; 2]),
        "<" = Lt([Id; 2]),
        "<=" = LtEq([Id; 2]),
        ">" = Gt([Id; 2]),
        ">=" = GtEq([Id; 2]),

        "is_not_null" = IsNotNull(Id),
        "is_null" = IsNull(Id),
        "negative" = Negative(Id),
        "between" = Between([Id; 3]),
        "between_inverted" = BetweenInverted([Id; 3]),
        "like" = Like([Id; 2]),
        "not_like" = NotLike([Id; 2]),
        Bool(bool),
        Int64(i64),
        Utf8(String),
        LargeUtf8(String),
        Column(Symbol),
    }
}

pub fn to_tokomak_expr(rec_expr: &mut RecExpr<TokomakExpr>, expr: Expr) -> Option<Id> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            let left = to_tokomak_expr(rec_expr, *left)?;
            let right = to_tokomak_expr(rec_expr, *right)?;
            let binary_expr = match op {
                Operator::Eq => TokomakExpr::Eq,
                Operator::NotEq => TokomakExpr::NotEq,
                Operator::Lt => TokomakExpr::Lt,
                Operator::LtEq => TokomakExpr::LtEq,
                Operator::Gt => TokomakExpr::Gt,
                Operator::GtEq => TokomakExpr::GtEq,
                Operator::Plus => TokomakExpr::Plus,
                Operator::Minus => TokomakExpr::Minus,
                Operator::Multiply => TokomakExpr::Multiply,
                Operator::Divide => TokomakExpr::Divide,
                Operator::Modulus => TokomakExpr::Modulus,
                Operator::And => TokomakExpr::And,
                Operator::Or => TokomakExpr::Or,
                Operator::Like => TokomakExpr::Like,
                Operator::NotLike => TokomakExpr::NotLike,
            };
            Some(rec_expr.add(binary_expr([left, right])))
        }
        Expr::Column(c) => Some(rec_expr.add(TokomakExpr::Column(Symbol::from(c)))),
        Expr::Literal(ScalarValue::Int64(Some(x))) => Some(rec_expr.add(TokomakExpr::Int64(x))),
        Expr::Literal(ScalarValue::Utf8(Some(x))) => Some(rec_expr.add(TokomakExpr::Utf8(x))),
        Expr::Literal(ScalarValue::LargeUtf8(Some(x))) => {
            Some(rec_expr.add(TokomakExpr::LargeUtf8(x)))
        }
        Expr::Literal(ScalarValue::Boolean(Some(x))) => {
            Some(rec_expr.add(TokomakExpr::Bool(x)))
        }
        Expr::Not(expr) => {
            let left = to_tokomak_expr(rec_expr, *expr)?;
            Some(rec_expr.add(TokomakExpr::Not(left)))
        }
        Expr::IsNull(expr) => {
            let left = to_tokomak_expr(rec_expr, *expr)?;
            Some(rec_expr.add(TokomakExpr::IsNull(left)))
        }
        Expr::IsNotNull(expr) => {
            let left = to_tokomak_expr(rec_expr, *expr)?;
            Some(rec_expr.add(TokomakExpr::IsNotNull(left)))
        }

        // not yet supported
        _ => None,
    }
}

fn to_exprs(rec_expr: &RecExpr<TokomakExpr>, id: Id) -> Expr {
    let refs = rec_expr.as_ref();
    let index: usize = id.into();
    match refs[index] {
        TokomakExpr::Plus(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Plus,
                right: Box::new(r),
            }
        }
        TokomakExpr::Minus(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Minus,
                right: Box::new(r),
            }
        }
        TokomakExpr::Divide(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Divide,
                right: Box::new(r),
            }
        }
        TokomakExpr::Modulus(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Modulus,
                right: Box::new(r),
            }
        }
        TokomakExpr::Not(id) => {
            let l = to_exprs(&rec_expr, id);
            Expr::Not(Box::new(l))
        }
        TokomakExpr::IsNotNull(id) => {
            let l = to_exprs(&rec_expr, id);
            Expr::IsNotNull(Box::new(l))
        }
        TokomakExpr::IsNull(id) => {
            let l = to_exprs(&rec_expr, id);
            Expr::IsNull(Box::new(l))
        }
        TokomakExpr::Negative(id) => {
            let l = to_exprs(&rec_expr, id);
            Expr::Negative(Box::new(l))
        }

        TokomakExpr::Between([expr, low, high]) => {
            let left = to_exprs(&rec_expr, expr);
            let low_expr = to_exprs(&rec_expr, low);
            let high_expr = to_exprs(&rec_expr, high);

            Expr::Between{
                expr: Box::new(left),
                negated: false,
                low: Box::new(low_expr),
                high: Box::new(high_expr),
            }
        }
        TokomakExpr::BetweenInverted([expr, low, high]) => {
            let left = to_exprs(&rec_expr, expr);
            let low_expr = to_exprs(&rec_expr, low);
            let high_expr = to_exprs(&rec_expr, high);

            Expr::Between{
                expr: Box::new(left),
                negated: false,
                low: Box::new(low_expr),
                high: Box::new(high_expr),
            }
        }
        TokomakExpr::Multiply(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Multiply,
                right: Box::new(r),
            }
        }
        TokomakExpr::Or(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Or,
                right: Box::new(r),
            }
        }
        TokomakExpr::And(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::And,
                right: Box::new(r),
            }
        }
        TokomakExpr::Eq(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Eq,
                right: Box::new(r),
            }
        }
        TokomakExpr::NotEq(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::NotEq,
                right: Box::new(r),
            }
        }
        TokomakExpr::Lt(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Lt,
                right: Box::new(r),
            }
        }
        TokomakExpr::LtEq(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::LtEq,
                right: Box::new(r),
            }
        }
        TokomakExpr::Gt(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Gt,
                right: Box::new(r),
            }
        }
        TokomakExpr::GtEq(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::GtEq,
                right: Box::new(r),
            }
        }
        TokomakExpr::Like(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Like,
                right: Box::new(r),
            }
        }
        TokomakExpr::NotLike(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::NotLike,
                right: Box::new(r),
            }
        }

        TokomakExpr::Int64(i) => Expr::Literal(ScalarValue::Int64(Some(i))),
        TokomakExpr::Utf8(ref i) => Expr::Literal(ScalarValue::Utf8(Some(i.clone()))),
        TokomakExpr::LargeUtf8(ref i) => Expr::Literal(ScalarValue::LargeUtf8(Some(i.clone()))),
        TokomakExpr::Column(col) => Expr::Column(col.to_string()),
        TokomakExpr::Bool(b) => {
            Expr::Literal(ScalarValue::Boolean(Some(b)))
        }
    }
}

impl OptimizerRule for Tokomak {
    fn optimize(&self, plan: &LogicalPlan) -> DFResult<LogicalPlan> {
        let inputs = plan.inputs();
        let new_inputs: Vec<LogicalPlan> = inputs
            .iter()
            .map(|plan| self.optimize(plan))
            .collect::<DFResult<Vec<_>>>()?;
        // optimize all expressions individual (for now)
        let mut exprs = vec![];
        for expr in plan.expressions().iter() {
            let rec_expr = &mut RecExpr::default();
            let tok_expr = to_tokomak_expr(rec_expr, expr.clone());
            match tok_expr {
                None => exprs.push(expr.clone()),
                Some(_expr) => {
                    let runner = Runner::<TokomakExpr, (), ()>::default()
                        .with_expr(rec_expr)
                        .run(&rules());

                    let mut extractor = Extractor::new(&runner.egraph, AstSize);
                    let (_, best_expr) = extractor.find_best(runner.roots[0]);
                    let start = best_expr.as_ref().len() - 1;
                    exprs.push(to_exprs(&best_expr, start.into()).clone());
                }
            }
        }

        utils::from_plan(plan, &exprs, &new_inputs)
    }

    fn name(&self) -> &str {
        "tokomak"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::prelude::{CsvReadOptions, ExecutionConfig, ExecutionContext};
    use egg::Runner;

    #[test]
    fn test_add_0() {
        let expr = "(+ 0 (x))".parse().unwrap();
        let runner = Runner::<TokomakExpr, (), ()>::default()
            .with_expr(&expr)
            .run(&rules());

        let mut extractor = Extractor::new(&runner.egraph, AstSize);

        let (_best_cost, best_expr) = extractor.find_best(runner.roots[0]);

        assert_eq!(format!("{}", best_expr), "x")
    }

    #[test]
    fn test_dist_and_or() {
        let expr = "(or (or (and (= 1 2) foo) (and (= 1 2) bar)) (and (= 1 2) boo))"
            .parse()
            .unwrap();
        let runner = Runner::<TokomakExpr, (), ()>::default()
            .with_expr(&expr)
            .run(&rules());

        let mut extractor = Extractor::new(&runner.egraph, AstSize);

        let (_, best_expr) = extractor.find_best(runner.roots[0]);

        assert_eq!(
            format!("{}", best_expr),
            "(and (= 1 2) (or boo (or foo bar)))"
        )
    }

    #[tokio::test]
    async fn custom_optimizer() {
        // register custom tokomak optimizer, verify that optimization took place

        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().add_optimizer_rule(Arc::new(Tokomak::new())),
        );

        ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())
            .unwrap();

        // create a plan to run a SQL query
        let lp = ctx
            .sql("SELECT price*0-price from example")
            .unwrap()
            .to_logical_plan();

        assert_eq!(
            format!("{}", lp.display_indent()),
            "Projection: (- #price)\
            \n  TableScan: example projection=Some([0])"
        )
    }

    #[tokio::test]
    async fn custom_optimizer_filter() {
        // register custom tokomak optimizer, verify that optimization took place

        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().add_optimizer_rule(Arc::new(Tokomak::new())),
        );
        ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())
            .unwrap();

        // create a plan to run a SQL query
        let lp = ctx
            .sql("SELECT price from example WHERE (price=1 AND price=2) OR (price=1 AND price=3)")
            .unwrap()
            .to_logical_plan();

        assert_eq!(
            format!("{}", lp.display_indent()),
            "Filter: #price Eq Int64(1) And #price Eq Int64(2) Or #price Eq Int64(3)\
            \n  TableScan: example projection=Some([0])"
        )
    }
}
