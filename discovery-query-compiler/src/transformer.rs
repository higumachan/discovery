use apollo_parser::{ast, Parser};

fn add_type_field(code: &str) -> String {
    let parser = Parser::new(code);

    let ast = parser.parse();

    for def in ast.document().definitions() {
        match def {
            ast::Definition::OperationDefinition(op_def) => {
                dbg!(&op_def);
                dbg!(op_def.directives());
                dbg!(op_def.variable_definitions());
                dbg!(op_def.name());
                dbg!(op_def.operation_type());
                dbg!(op_def.selection_set());
                dbg!(op_def.selection_set().unwrap().selections().next().unwrap());
                let s = op_def.selection_set().unwrap().selections().next().unwrap();
                match s {
                    ast::Selection::Field(f) => {
                        dbg!(f.name(), f.arguments(), f.selection_set());
                    }
                    _ => {
                        panic!("無理");
                    }
                }
            }
            _ => {
                dbg!("other");
            }
        }
    }

    "".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_type_field() {
        let code = r"query MeQuery {
    users(limit: 1) {
        id
        gavatar_hash
        name
        follows {
            id
            user_to {
                id
                name
                gavatar_hash
            }
        }
        followers {
            user_from {
                id
                name
                gavatar_hash
            }
        }
    }
}
";

        assert_eq!(
            add_type_field(code),
            r"query MeQuery {
    users(limit: 1) {
        __typename
        id
        gavatar_hash
        name
        follows {
            __typename
            id
            user_to {
                __typename
                id
                name
                gavatar_hash
            }
        }
        followers {
            __typename
            user_from {
                __typename
                id
                name
                gavatar_hash
            }
        }
    }
}
"
        );
    }
}
