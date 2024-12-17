# data-quality-tool

## Overview

The `data-quality-tool` is a Python package designed to perform data quality checks on CSV files. It includes functions to validate data types, check for duplicate keys, and identify rows with special characters. The tool can generate detailed error reports in both CSV and PDF formats.

## Installation

To install the `data-quality-tool` package, navigate to the root directory of your project and run the following command:

```sh
pip install -e .
```

This command installs the package in editable mode, allowing you to make changes to the code without reinstalling the package.

## Usage

### Importing the Package
To use the data-quality-tool package in your Python scripts or Jupyter Notebooks, import the necessary functions:

```python
import dataqualitycheck.dataqualitycheck as dq
```

## Example Usage in Jupyter Notebook

### Define the Expected Schema

Define the expected schema for your CSV file. The schema should include the expected data types, regex patterns, and other validation rules for each column:

```python
expected_schema = {
    "CustomerID": {"dtype": int, "nullable": False},
    "Name": {"dtype": str, "nullable": False},
    "Email": {"dtype": str, "regex": r"[^@]+@[^@]+\.[^@]+", "nullable": True},
    "PhoneNumber": {"dtype": str, "regex": r"^\+44 \d{4} \d{6}$", "nullable": True},
    "BirthDate": {"dtype": datetime, "format": '%Y-%m-%d', "nullable": True},
    "Country": {"dtype": str, "nullable": True},
    "PurchaseAmount": {"dtype": float, "nullable": True},
    "LoyaltyPoints": {"dtype": int, "nullable": True},
    "PremiumMember": {"dtype": bool, "nullable": True}
}
```
## Perform Data Quality Checks
Use the `read_file` function to perform data quality checks on your CSV file. The function returns a summary of the errors found:

```python
csv_file_path = "./files/testfile_with_errors.csv"
error_summary = dq.read_file(csv_file_path, expected_schema, 'CustomerID', pdf_report=True, csv_report=True)
```
## Function
`read_file`

The `read_file` function reads a CSV file and performs data quality checks based on the provided schema.

### Parameters:

* `csv_file_path` (str): The path to the CSV file.
* `expected_schema` (dict): The expected schema for the CSV file.
* `primary_key` (str): The primary key column to check for duplicates.
* `pdf_report` (bool): Whether to generate a PDF report of the errors.
* `csv_report` (bool): Whether to generate a CSV report of the errors.

### Returns:

* A dictionary containing the counts of datatype errors, duplicate key errors, and special character errors, as well as a detailed error summary.

* `validate_data_types`

    The `validate_data_types` function validates the data types of the columns in the DataFrame based on the expected schema.

* `check_for_duplicate_keys`
 
    The `check_for_duplicate_keys` function checks for duplicate primary keys in the DataFrame.

* `check_special_characters`

    The `check_special_characters` function checks for special characters and escape characters in the specified columns of the DataFrame.

* `write_error_details_to_csv`

    The `write_error_details_to_csv` function writes the error details to CSV files.

* `write_error_details_to_pdf`

    The `write_error_details_to_pdf` function writes the error details to a PDF file.
