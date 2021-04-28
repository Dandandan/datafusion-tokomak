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
        rw!("minus-0"; "(- ?x 0)" => "?x"),
        rw!("mul-0"; "(* ?x 0)" => "0"),
        rw!("mul-1"; "(* ?x 1)" => "?x"),
        rw!("div-1"; "(/ ?x 1)" => "?x"),
        rw!("dist-and-or"; "(or (and ?a ?b) (and ?a ?c))" => "(and ?a (or ?b ?c))"),
        rw!("dist-or-and"; "(and (or ?a ?b) (or ?a ?c))" => "(or ?a (and ?b ?c))"),
        rw!("not-not"; "(not (not ?x))" => "?x"),
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

        Int64(i64), // rest is encoded as symbol
        Column(Symbol),
        Symbol(Symbol),
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
        // not yet supported
        _ => None,
    }
}

fn to_exprs(rec_expr: &RecExpr<TokomakExpr>, id: Id) -> Expr {
    let refs = rec_expr.as_ref();
    let index: usize = id.into();
    match refs[index] {
        // TokomakExpr::Plus(_) => {

        // }
        // TokomakExpr::Minus(_) => {}
        // TokomakExpr::Divide(_) => {}
        // TokomakExpr::Modulus(_) => {}
        // TokomakExpr::Not(_) => {}
        // TokomakExpr::Or(_) => {}
        // TokomakExpr::And(_) => {}
        // TokomakExpr::Eq(_) => {}
        // TokomakExpr::NotEq(_) => {}
        // TokomakExpr::Lt(_) => {}
        // TokomakExpr::LtEq(_) => {}
        // TokomakExpr::Gt(_) => {}
        // TokomakExpr::GtEq(_) => {}
        // TokomakExpr::IsNotNull(_) => {}
        // TokomakExpr::IsNull(_) => {}
        // TokomakExpr::Negative(_) => {}
        // TokomakExpr::Between(_) => {}
        // TokomakExpr::BetweenInverted(_) => {}
        // TokomakExpr::Like(_) => {}
        // TokomakExpr::NotLike(_) => {}
        TokomakExpr::Multiply(ids) => {
            let l = to_exprs(&rec_expr, ids[0]);
            let r = to_exprs(&rec_expr, ids[1]);

            Expr::BinaryExpr {
                left: Box::new(l),
                op: Operator::Multiply,
                right: Box::new(r),
            }
        }
        TokomakExpr::Int64(i) => Expr::Literal(ScalarValue::Int64(Some(i))),
        TokomakExpr::Column(col) => Expr::Column(col.to_string()),
        _ => unimplemented!("unimplemented to_exprs"),
    }
}

impl OptimizerRule for Tokomak {
    fn optimize(&self, plan: &LogicalPlan) -> DFResult<LogicalPlan> {
        let inputs = plan.inputs();
        let new_inputs: Vec<LogicalPlan> = inputs
            .iter()
            .map(|plan| self.optimize(plan))
            .collect::<DFResult<Vec<_>>>()?;
        // optimize all expressions individuall (for now)
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
            .sql("SELECT price*0 from example")
            .unwrap()
            .to_logical_plan();

        assert_eq!(
            format!("{}", lp.display_indent()),
            "Projection: Int64(0)\
            \n  TableScan: example projection=Some([0])"
        )
    }
}
