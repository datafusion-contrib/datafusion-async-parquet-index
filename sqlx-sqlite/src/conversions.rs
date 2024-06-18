use datafusion::arrow::{array::{Array, ArrayRef, AsArray}, datatypes::DataType};
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion::arrow::datatypes;
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

/// Convert a DataFusion Array to a Vec of SeaQuery Values
pub fn array_to_values(array: ArrayRef) -> Option<Vec<Value>> {
    let values = match array.data_type() {
        DataType::Int8 => {
            let array = array.as_primitive::<datatypes::Int8Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::TinyInt(Some(v)),
                    None => Value::TinyInt(None)
                }
            }).collect()
        }
        DataType::Int16 => {
            let array = array.as_primitive::<datatypes::Int16Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::SmallInt(Some(v)),
                    None => Value::SmallInt(None)
                }
            }).collect()
        }
        DataType::Int32 => {
            let array = array.as_primitive::<datatypes::Int32Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Int(Some(v)),
                    None => Value::Int(None)
                }
            }).collect()
        }
        DataType::Int64 => {
            let array = array.as_primitive::<datatypes::Int64Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::BigInt(Some(v)),
                    None => Value::BigInt(None)
                }
            }).collect()
        }
        DataType::UInt8 => {
            let array = array.as_primitive::<datatypes::UInt8Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::TinyUnsigned(Some(v)),
                    None => Value::TinyUnsigned(None)
                }
            }).collect()
        }
        DataType::UInt16 => {
            let array = array.as_primitive::<datatypes::UInt16Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::SmallUnsigned(Some(v)),
                    None => Value::SmallUnsigned(None)
                }
            }).collect()
        }
        DataType::UInt32 => {
            let array = array.as_primitive::<datatypes::UInt32Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Unsigned(Some(v)),
                    None => Value::Unsigned(None)
                }
            }).collect()
        }
        DataType::UInt64 => {
            let array = array.as_primitive::<datatypes::UInt64Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::BigUnsigned(Some(v)),
                    None => Value::BigUnsigned(None)
                }
            }).collect()
        }
        DataType::Float32 => {
            let array = array.as_primitive::<datatypes::Float32Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Float(Some(v)),
                    None => Value::Float(None)
                }
            }).collect()
        }
        DataType::Float64 => {
            let array = array.as_primitive::<datatypes::Float64Type>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Double(Some(v)),
                    None => Value::Double(None)
                }
            }).collect()
        }
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::String(Some(Box::new(v.to_string()))),
                    None => Value::String(None)
                }
            }).collect()
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::String(Some(Box::new(v.to_string()))),
                    None => Value::String(None)
                }
            }).collect()
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Bytes(Some(Box::new(v.to_vec()))),
                    None => Value::Bytes(None)
                }
            }).collect()
        }
        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Bytes(Some(Box::new(v.to_vec()))),
                    None => Value::Bytes(None)
                }
            }).collect()
        }
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            array.iter().map(|v| {
                match v {
                    Some(v) => Value::Bytes(Some(Box::new(v.to_vec()))),
                    None => Value::Bytes(None)
                }
            }).collect()
        }
        // Extend with other types, e.g. to support arrays, dates, etc.
        _ => return None,
    };
    Some(values)
}