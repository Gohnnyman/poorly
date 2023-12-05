use super::*;
use crate::core::types::{DataType, TypedValue};

fn table() -> Table {
    Table {
        name: "test".into(),
        columns: vec![
            ("id".into(), DataType::Int),
            ("price".into(), DataType::Float),
        ],
        file: tempfile::tempfile().unwrap(),
        serial: 0,
    }
}

fn join(i: i32) -> Table {
    Table {
        name: format!("join{}", i),
        columns: vec![
            ("id".into(), DataType::Int),
            ("email".into(), DataType::Email),
        ],
        file: tempfile::tempfile().unwrap(),
        serial: 0,
    }
}

#[test]
fn select() -> Result<(), PoorlyError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec![], [].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn test_join() -> Result<(), PoorlyError> {
    let mut table1 = join(1);
    let mut table2 = join(2);
    let row1: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        (
            "email".into(),
            TypedValue::Email("test@gmail.com".to_string()),
        ),
    ]
    .into();

    let row2: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(2)),
        (
            "email".into(),
            TypedValue::Email("test2@gmail.com".to_string()),
        ),
    ]
    .into();

    let row3: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        (
            "email".into(),
            TypedValue::Email("table2@gmail.com".to_string()),
        ),
    ]
    .into();

    let row4: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(2)),
        (
            "email".into(),
            TypedValue::Email("table22@gmail.com".to_string()),
        ),
    ]
    .into();

    table1.insert(row1)?;
    table1.insert(row2)?;
    table2.insert(row3)?;
    table2.insert(row4)?;

    let mut conditions = HashMap::new();
    conditions.insert("join1.id".to_string(), TypedValue::Int(1));

    let mut join_on = HashMap::new();
    join_on.insert("join1.id".to_string(), "join2.id".to_string());

    let result = table1
        .join(&mut table2, vec![], conditions, join_on)?
        .remove(0);

    assert_eq!(result.get("join1.id"), Some(&TypedValue::Int(1)));
    assert_eq!(result.get("join2.id"), Some(&TypedValue::Int(1)));
    assert_eq!(
        result.get("join1.email"),
        Some(&TypedValue::Email("test@gmail.com".to_string()))
    );
    assert_eq!(
        result.get("join2.email"),
        Some(&TypedValue::Email("table2@gmail.com".to_string()))
    );

    assert_eq!(result.len(), 4);

    Ok(())
}

#[test]
fn project() -> Result<(), PoorlyError> {
    let mut table = table();
    let mut row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec!["price".into()], [].into())?;
    assert_eq!(rows.len(), 1);

    row.remove("id");
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn filter() -> Result<(), PoorlyError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;

    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(2)),
        ("price".into(), TypedValue::Float(18.18)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec![], [("id".into(), TypedValue::Int(2))].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn update() -> Result<(), PoorlyError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;
    table.update(
        [("price".into(), TypedValue::Float(123.45))].into(),
        [].into(),
    )?;

    let rows = table.select(vec![], [].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["price"], TypedValue::Float(123.45));

    Ok(())
}

#[test]
fn delete() -> Result<(), PoorlyError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;
    table.delete([].into())?;

    let rows = table.select(vec![], [].into())?;
    assert!(rows.is_empty());

    Ok(())
}
