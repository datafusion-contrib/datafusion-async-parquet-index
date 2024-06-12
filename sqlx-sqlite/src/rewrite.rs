use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::{expressions as phys_expr, PhysicalExprRef};
use sea_query::{Alias, BinOper, CaseStatement, ColumnRef, IntoIden, SimpleExpr, Value};


/// Convert a DataFusion PhysicalExpr to a SeaQuery SimpleExpr
pub fn physical_expr_to_sea_query(expr: &PhysicalExprRef) -> SimpleExpr {
    if let Some(expr) = expr.as_any().downcast_ref::<phys_expr::BinaryExpr>(){
        let left = physical_expr_to_sea_query(expr.left());
        let right = physical_expr_to_sea_query(expr.right());
        match expr.op() {
            Operator::Eq => left.binary(BinOper::Equal, right),
            Operator::NotEq => left.binary(BinOper::NotEqual, right),
            Operator::Lt => left.binary(BinOper::SmallerThan, right),
            Operator::LtEq => left.binary(BinOper::SmallerThanOrEqual, right),
            Operator::Gt => left.binary(BinOper::GreaterThan, right),
            Operator::GtEq => left.binary(BinOper::GreaterThanOrEqual, right),
            Operator::Plus => left.binary(BinOper::Add, right),
            Operator::Minus => left.binary(BinOper::Sub, right),
            Operator::Multiply => left.binary(BinOper::Mul, right),
            Operator::Divide => left.binary(BinOper::Div, right),
            Operator::Modulo => left.binary(BinOper::Mod, right),
            Operator::And => left.binary(BinOper::And, right),
            Operator::Or => left.binary(BinOper::Or, right),
            Operator::LikeMatch => left.binary(BinOper::Like, right),
            Operator::NotLikeMatch => left.binary(BinOper::NotLike, right),
            Operator::BitwiseShiftLeft => left.binary(BinOper::LShift, right),
            Operator::BitwiseShiftRight => left.binary(BinOper::RShift, right),
            _ => SimpleExpr::Constant(Value::Bool(Some(true)))
        }
    } else if let Some(expr) = expr.as_any().downcast_ref::<phys_expr::Column>() {
        SimpleExpr::Column(ColumnRef::Column(Alias::new(expr.name().to_string()).into_iden()))
    } else if let Some(expr) = expr.as_any().downcast_ref::<phys_expr::Literal>() {
        match expr.value() {
            ScalarValue::Null => SimpleExpr::Keyword(sea_query::Keyword::Null),
            ScalarValue::Boolean(v) => SimpleExpr::Constant(Value::Bool(*v)),
            ScalarValue::Float32(v) => SimpleExpr::Constant(Value::Float(*v)),
            ScalarValue::Float64(v) => SimpleExpr::Constant(Value::Double(*v)),
            ScalarValue::Int8(v) => SimpleExpr::Constant(Value::TinyInt(*v)),
            ScalarValue::Int16(v) => SimpleExpr::Constant(Value::SmallInt(*v)),
            ScalarValue::Int32(v) => SimpleExpr::Constant(Value::Int(*v)),
            ScalarValue::Int64(v) => SimpleExpr::Constant(Value::BigInt(*v)),
            ScalarValue::UInt8(v) => SimpleExpr::Constant(Value::TinyUnsigned(*v)),
            ScalarValue::UInt16(v) => SimpleExpr::Constant(Value::SmallUnsigned(*v)),
            ScalarValue::UInt32(v) => SimpleExpr::Constant(Value::Unsigned(*v)),
            ScalarValue::UInt64(v) => SimpleExpr::Constant(Value::BigUnsigned(*v)),
            ScalarValue::Utf8(v) => match v {
                Some(v) => SimpleExpr::Constant(Value::String(Some(Box::new(v.to_string())))),
                None => SimpleExpr::Constant(Value::String(None)),
            },
            ScalarValue::LargeUtf8(v) => match v {
                Some(v) => SimpleExpr::Constant(Value::String(Some(Box::new(v.to_string())))),
                None => SimpleExpr::Constant(Value::String(None)),
            },
            ScalarValue::Binary(v) => match v {
                Some(v) => SimpleExpr::Constant(Value::Bytes(Some(Box::new(v.to_vec())))),
                None => SimpleExpr::Constant(Value::Bytes(None)),
            },
            ScalarValue::FixedSizeBinary(_, v) => match v {
                Some(v) => SimpleExpr::Constant(Value::Bytes(Some(Box::new(v.to_vec())))),
                None => SimpleExpr::Constant(Value::Bytes(None)),
            },
            ScalarValue::LargeBinary(v) => match v {
                Some(v) => SimpleExpr::Constant(Value::Bytes(Some(Box::new(v.to_vec())))),
                None => SimpleExpr::Constant(Value::Bytes(None)),
            },
            // Extend with other types, e.g. to support arrays, dates, etc.
            _ => SimpleExpr::Constant(Value::Bool(Some(true)))
        }
    } else if let Some(expr) = expr.as_any().downcast_ref::<phys_expr::CaseExpr>() {
        let mut case = CaseStatement::new();
        for (when, then) in expr.when_then_expr() {
            case = case.case(physical_expr_to_sea_query(when), physical_expr_to_sea_query(then));
        }
        if let Some(else_exp) = expr.else_expr() {
            case = case.finally(physical_expr_to_sea_query(else_exp));
        };
        SimpleExpr::Case(Box::new(case))
    } else {
        SimpleExpr::Constant(Value::Bool(Some(true)))
    }
}