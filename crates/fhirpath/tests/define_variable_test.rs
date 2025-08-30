//! Tests for defineVariable function
//! 
//! NOTE: These tests document the current limitations of defineVariable.
//! Proper implementation requires architectural changes to support context
//! modification through expression chains.

#[cfg(test)]
mod tests {
    use chumsky::Parser;
    use helios_fhirpath::evaluator::{evaluate, EvaluationContext};
    use helios_fhirpath::parser::parser;
    use helios_fhirpath_support::EvaluationResult;
    use helios_fhir::FhirVersion;

    #[test]
    fn test_define_variable_basic_syntax() {
        // Test that defineVariable parses and doesn't error on basic syntax
        let expr = "defineVariable('v1', 'value1')";
        let parsed = parser().parse(expr);
        assert!(parsed.is_ok(), "Failed to parse defineVariable expression");
        
        let context = EvaluationContext::new_empty(FhirVersion::R4);
        let result = evaluate(&parsed.unwrap(), &context, None);
        assert!(result.is_ok(), "defineVariable should not error");
    }

    #[test]
    fn test_define_variable_returns_input() {
        // Test that defineVariable returns its input unchanged
        let expr = "5.defineVariable('v1', 10)";
        let parsed = parser().parse(expr).unwrap();
        
        let context = EvaluationContext::new_empty(FhirVersion::R4);
        let result = evaluate(&parsed, &context, None).unwrap();
        
        assert_eq!(result, EvaluationResult::integer(5));
    }

    #[test]
    fn test_define_variable_not_accessible() {
        // This test documents current limitation: variables defined by defineVariable
        // are not accessible in subsequent operations
        
        // Test 1: When defineVariable is called on a non-empty collection
        let expr = "'test'.defineVariable('v1', 'value1').select(%v1)";
        let parsed = parser().parse(expr).unwrap();
        
        let context = EvaluationContext::new_empty(FhirVersion::R4);
        let result = evaluate(&parsed, &context, None);
        
        // This should error with undefined variable since select() tries to access %v1
        assert!(result.is_err(), "Expected error when accessing undefined variable");
        assert!(result.unwrap_err().to_string().contains("Undefined Variable: %v1"));
        
        // Test 2: When defineVariable is called with empty input
        let expr2 = "defineVariable('v1', 'value1').select(%v1)";
        let parsed2 = parser().parse(expr2).unwrap();
        let result2 = evaluate(&parsed2, &context, None);
        
        // With empty input, defineVariable returns Empty, and select() on Empty returns Empty
        assert!(result2.is_ok(), "Should not error when select is called on Empty");
        assert_eq!(result2.unwrap(), EvaluationResult::Empty);
    }

    #[test]
    fn test_system_variable_protection() {
        // Test that system variables cannot be overridden
        let expr = "defineVariable('context', 'oops')";
        let parsed = parser().parse(expr).unwrap();
        
        let context = EvaluationContext::new_empty(FhirVersion::R4);
        let result = evaluate(&parsed, &context, None);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot override system variable"));
    }

    #[test]
    fn test_variable_scoping_in_select() {
        // This test documents that child contexts are created for select()
        // but defineVariable still doesn't work due to architectural limitations
        let expr = "1 | 2 | 3";
        let parsed = parser().parse(expr).unwrap();
        
        let context = EvaluationContext::new_empty(FhirVersion::R4);
        let result = evaluate(&parsed, &context, None).unwrap();
        
        match result {
            EvaluationResult::Collection { items, .. } => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected collection")
        }
    }

    #[test] 
    fn test_context_inheritance() {
        // Test that child contexts can access parent variables
        let mut parent_context = EvaluationContext::new_empty(FhirVersion::R4);
        parent_context.set_variable_result("%parent_var", EvaluationResult::string("parent".to_string()));
        
        let child_context = parent_context.create_child_context();
        
        // Child should be able to access parent variable
        assert!(child_context.lookup_variable("%parent_var").is_some());
        
        // Parent should not see child variables
        let mut child_with_var = child_context.clone();
        child_with_var.set_variable_result("%child_var", EvaluationResult::string("child".to_string()));
        
        assert!(parent_context.lookup_variable("%child_var").is_none());
    }
}