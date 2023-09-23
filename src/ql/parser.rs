use nom::{self, bytes::complete::{self, tag}, character::complete::{space1, space0, line_ending}, branch, sequence::delimited};

// This is going to be a toy version of CQL. All inserts and deletes will go to a table called 'the_table'.

#[derive(Debug, PartialEq)]
pub enum Operation {
    Insert(String, String),
    Delete(String),
    Select(String),
}

// e.g. 'INSERT INTO the_table (key) VALUES ("foo")' / 'DELETE FROM the_table WHERE key = "foo"' / 'SELECT * FROM the_table WHERE key = "foo"'

impl Operation {
    pub fn from_str(input: &str) -> Result<(Operation), nom::Err<nom::error::Error<&str>>> {
        let (_, operation) = branch::alt((insert, delete, select))(input)?;
        Ok(operation)
    }
}

fn insert(input: &str) -> nom::IResult<&str, Operation> {
    let (input, _) = tag("INSERT")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("INTO")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("the_table")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("(")(input)?;
    let (input, key) = complete::is_not(")")(input)?;
    let (input, _) = tag(")")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("VALUES")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("(")(input)?;
    let (input, value) = complete::is_not(")")(input)?;
    let (input, _) = tag(")")(input)?;
    let (input, _) = tag(";")(input)?;
    // consume any whitespace left
    let (input, _) = space0(input)?;
    // consume any newlines
    let (input, _) = line_ending(input)?;
    match input.len() {
        0 => Ok((input, Operation::Insert(key.to_string(), value.to_string()))),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Eof))),
    }
}

fn delete(input: &str) -> nom::IResult<&str, Operation> {
    let (input, _) = tag("DELETE")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("the_table")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("WHERE")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("key")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("\"")(input)?;
    let (input, key) = complete::is_not("\"")(input)?;
    let (input, _) = tag("\"")(input)?;
    let (input, _) = tag(";")(input)?;
    // consume any whitespace left
    let (input, _) = space0(input)?;
    // consume any newlines
    let (input, _) = line_ending(input)?;
    
    match input.len() {
        0 => Ok((input, Operation::Delete(key.to_string()))),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Eof))),
    }
}

fn select(input: &str) -> nom::IResult<&str, Operation> {
    // SELECT some_key FROM the_table;
    let (input, _) = tag("SELECT")(input)?;
    let (input, _) = space1(input)?;
    let (input, key) = complete::is_not(" ")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = space1(input)?;
    let (input, _) = tag("the_table")(input)?;
    let (input, _) = tag(";")(input)?;
    // consume any whitespace left
    let (input, _) = space0(input)?;
    // consume any newlines
    let (input, _) = line_ending(input)?;
    println!("input: {}", input.len());
    match input.len() {
        0 => Ok((input, Operation::Select(key.to_string()))),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Eof))),
    }
}
