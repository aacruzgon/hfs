# Precision Function - Decimal Limitation

The `precision()` function implementation has a known limitation with decimal values that have trailing zeros.

## Issue

The FHIRPath specification expects that `1.58700.precision()` should return `5` (counting all 5 digits including trailing zeros). However, the `rust_decimal::Decimal` type used internally may normalize or reformat decimal values, potentially losing information about trailing zeros in the original literal or adding additional precision.

## Current Behavior

The implementation counts all digits in the decimal's string representation. This may not match the original literal's precision if the decimal type reformats the value.

## Impact

This limitation only affects decimal literals with trailing zeros. The precision function works correctly for:
- Integer values
- Date values
- DateTime values
- Time values
- Decimal values without trailing zeros

## Future Improvement

A proper fix would require preserving the original string representation of decimal literals alongside their numeric values, which would be a significant architectural change to the parser and evaluation system.