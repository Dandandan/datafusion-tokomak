use std::vec;

use datafusion::optimizer::utils;
use datafusion::{logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule};

use datafusion::error::Result;
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
        rw!("add-0"; "(+ ?x 0)" => "?x"),
        rw!("minus-0"; "(- ?x 0)" => "?x"),
        rw!("mul-0"; "(* ?x 0)" => "0"),
        rw!("mul-1"; "(* ?x 1)" => "?x"),
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

        //Literal(ScalarValue) Should add Eq
        Symbol(Symbol),
    }
}

pub fn to_tokomak_expr(
    mut rec_expr: &mut RecExpr<TokomakExpr>,
    expr: Expr,
) -> Option<(Id, RecExpr<TokomakExpr>)> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            let (li, mut left) = to_tokomak_expr(rec_expr, *left)?;
            let (ri, right) = to_tokomak_expr(&mut left, *right)?;
            let binary_expr = match op {
                datafusion::logical_plan::Operator::Eq => TokomakExpr::Eq,
                datafusion::logical_plan::Operator::NotEq => TokomakExpr::NotEq,
                datafusion::logical_plan::Operator::Lt => TokomakExpr::Lt,
                datafusion::logical_plan::Operator::LtEq => TokomakExpr::LtEq,
                datafusion::logical_plan::Operator::Gt => TokomakExpr::Gt,
                datafusion::logical_plan::Operator::GtEq => TokomakExpr::GtEq,
                datafusion::logical_plan::Operator::Plus => TokomakExpr::Plus,
                datafusion::logical_plan::Operator::Minus => TokomakExpr::Minus,
                datafusion::logical_plan::Operator::Multiply => TokomakExpr::Multiply,
                datafusion::logical_plan::Operator::Divide => TokomakExpr::Divide,
                datafusion::logical_plan::Operator::Modulus => TokomakExpr::Modulus,
                datafusion::logical_plan::Operator::And => TokomakExpr::And,
                datafusion::logical_plan::Operator::Or => TokomakExpr::Or,
                datafusion::logical_plan::Operator::Like => TokomakExpr::Like,
                datafusion::logical_plan::Operator::NotLike => TokomakExpr::NotLike,
            };

            unimplemented!("TODO")
        }
        // not yet supported
        _ => None,
    }
}

impl OptimizerRule for Tokomak {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let inputs = plan.inputs();
        let new_inputs = inputs
            .iter()
            .map(|plan| self.optimize(plan))
            .collect::<Result<Vec<_>>>()?;
        // optimize all expressions
        let rec_expr = &mut RecExpr::default();
        let mut exprs = vec![];
        for expr in plan.expressions().iter() {
            let tok_expr = to_tokomak_expr(rec_expr, expr.clone());
            match tok_expr {
                None => exprs.push(expr.clone()),
                Some(expr) => unimplemented!("!!"),
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
    use super::*;
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
}
