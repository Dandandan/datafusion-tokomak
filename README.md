# datafusion-tokomak
Experimental pluggable optimizer for DataFusion using graph rewriting.

The goal is to allow experimenting with the [Egg](https://github.com/egraphs-good/egg) library,
and experiment with a maintainable and aggresive optimization framework.

The optimizer is WIP.


Usage example:

```rust
 let mut ctx = ExecutionContext::with_config(
    ExecutionConfig::new().add_optimizer_rule(Arc::new(Tokomak::new())),
);

ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())?;

// create a DataFusion dataframe
let df = ctx
    .sql("SELECT price * 0 from example")?;

```
