use octofhir_ucum::{analyse, is_comparable};
use octofhir_ucum::fhir::{FhirQuantity, convert_quantity};

fn main() {
    // Test 1: Check if g and mg are comparable
    println!("Test 1: Are 'g' and 'mg' comparable?");
    match is_comparable("g", "mg") {
        Ok(result) => println!("  Result: {}", result),
        Err(e) => println!("  Error: {}", e),
    }
    
    // Test 2: Convert using FhirQuantity
    println!("\nTest 2: Convert 4g to mg");
    let quantity = FhirQuantity::with_ucum_code(4.0, "g");
    match convert_quantity(&quantity, "mg") {
        Ok(result) => {
            println!("  Value: {}", result.value);
            println!("  Unit: {:?}", result.code);
        }
        Err(e) => println!("  Error: {}", e),
    }
    
    // Test 3: Days to weeks
    println!("\nTest 3: Are 'd' and 'wk' comparable?");
    match is_comparable("d", "wk") {
        Ok(result) => println!("  Result: {}", result),
        Err(e) => println!("  Error: {}", e),
    }
    
    println!("\nTest 4: Convert 7 days to weeks");
    let days = FhirQuantity::with_ucum_code(7.0, "d");
    match convert_quantity(&days, "wk") {
        Ok(result) => {
            println!("  Value: {}", result.value);
            println!("  Unit: {:?}", result.code);
        }
        Err(e) => println!("  Error: {}", e),
    }
}