use rust_decimal::Decimal;
use std::str::FromStr;

#[test]
fn test_boundary_zero_case() {
    let value = Decimal::from_str("-0.0034").unwrap();
    let precision = 1;
    
    println!("\nTesting lowBoundary(-0.0034, 1):");
    println!("Original value: {}", value);
    println!("Precision: {}", precision);
    
    // What does the value round to at precision 1?
    let rounded = value.round_dp(precision);
    println!("Rounded to precision {}: {}", precision, rounded);
    println!("Rounded == 0: {}", rounded == Decimal::ZERO);
    
    // Let's trace through the algorithm
    let value_str = value.to_string();
    let is_negative = value < Decimal::ZERO;
    println!("Is negative: {}", is_negative);
    
    let value_str_no_sign = value_str.trim_start_matches('-');
    let (integer_part, decimal_part) = if let Some(dot_pos) = value_str_no_sign.find('.') {
        (&value_str_no_sign[..dot_pos], &value_str_no_sign[dot_pos + 1..])
    } else {
        (value_str_no_sign.as_ref(), "")
    };
    
    println!("Integer part: '{}'", integer_part);
    println!("Decimal part: '{}'", decimal_part);
    
    let actual_decimals = decimal_part.len() as u32;
    println!("Actual decimals: {}", actual_decimals);
    
    // Since actual_decimals (4) > precision (1), we use the floor logic
    if actual_decimals >= precision {
        println!("\nUsing floor logic since actual_decimals >= precision");
        let scale = 10_i64.pow(precision);
        let scale_dec = Decimal::from(scale);
        println!("Scale: {}", scale_dec);
        
        let scaled = value * scale_dec;
        println!("Value * scale: {}", scaled);
        
        let floored = scaled.floor();
        println!("Floor of scaled: {}", floored);
        
        let result = floored / scale_dec;
        println!("Result (floored / scale): {}", result);
        
        println!("\nAnalysis:");
        println!("- Value -0.0034 rounds to 0.0 at precision 1");
        println!("- The low boundary should be the smallest value that rounds to 0.0");
        println!("- For precision 1, values from -0.05 to 0.05 round to 0.0");
        println!("- So the low boundary should be -0.05");
        println!("- But floor(-0.034) = -1, then -1/10 = -0.1");
        println!("- This gives us -0.1 instead of -0.05");
        
        // Test if we need special handling for values that round to 0
        if rounded == Decimal::ZERO && value != Decimal::ZERO {
            println!("\nSpecial case: value rounds to 0 but is not 0");
            // The low boundary for values that round to 0 should be -0.05 for precision 1
            let half_unit = Decimal::from(5) / Decimal::from(10_i64.pow(precision + 1));
            let expected_low_boundary = if value < Decimal::ZERO {
                -half_unit
            } else {
                -half_unit // For positive values that round to 0, low boundary is still -0.05
            };
            println!("Expected low boundary: {}", expected_low_boundary);
        }
    }
}

#[test]
fn test_boundary_cases_around_zero() {
    struct TestCase {
        value: &'static str,
        precision: u32,
        expected_low: &'static str,
        description: &'static str,
    }
    
    let test_cases = vec![
        TestCase {
            value: "-0.0034",
            precision: 1,
            expected_low: "-0.05",
            description: "negative value that rounds to 0",
        },
        TestCase {
            value: "0.0034",
            precision: 1,
            expected_low: "-0.05",
            description: "positive value that rounds to 0",
        },
        TestCase {
            value: "-0.05",
            precision: 1,
            expected_low: "-0.15",
            description: "negative boundary value",
        },
        TestCase {
            value: "0.05",
            precision: 1,
            expected_low: "-0.05",
            description: "positive boundary value",
        },
        TestCase {
            value: "0.0",
            precision: 1,
            expected_low: "-0.05",
            description: "exact zero",
        },
    ];
    
    for tc in test_cases {
        println!("\n=== Testing {} with precision {} ({}) ===", tc.value, tc.precision, tc.description);
        let value = Decimal::from_str(tc.value).unwrap();
        let rounded = value.round_dp(tc.precision);
        println!("Value {} rounds to {} at precision {}", value, rounded, tc.precision);
        
        // Calculate what the actual low boundary would be with current implementation
        let scale = 10_i64.pow(tc.precision);
        let scale_dec = Decimal::from(scale);
        let floored = (value * scale_dec).floor() / scale_dec;
        println!("Current implementation gives: {}", floored);
        println!("Expected: {}", tc.expected_low);
        
        if rounded == Decimal::ZERO {
            println!("This value rounds to 0!");
        }
    }
}