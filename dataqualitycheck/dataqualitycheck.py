import os
import polars as pl
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from datetime import datetime
import ast
import re
import pandas as pd
from collections import defaultdict
import textwrap

class DataQualityCheckError(Exception):
    pass

def check_special_characters(df, expected_schema):
    try:
        for column, schema_info in expected_schema.items():
            if schema_info['dtype'] == str:
                special_char_pattern = r'[^a-zA-Z0-9\s@.\-_+]|[\n\t\r]'
                mask = df[column].str.contains(special_char_pattern)
    # Filter the DataFrame to get rows with special characters
                special_char_rows = df.filter(mask)
                return special_char_rows
    except Exception as e:
        raise DataQualityCheckError(f"Failed to find duplicate keys: {str(e)}")

def check_for_duplicate_keys_polars(key, df):
    # Return a polars dataframe of duplicate rows based on 'key' column
    try:
        duplicate_keys = df.filter(pl.col(key).is_duplicated())
        return duplicate_keys
    except Exception as e:
        raise DataQualityCheckError(f"Failed to find duplicate keys: {str(e)}")

def check_for_duplicate_rows_polars(df):
    # Return Polars dataframe of duplicate rows based on all columns

    duplicates_all = df.filter(df.is_duplicated())
    return duplicates_all

def validate_data_types(df, expected_schema):
    try:
        # Validate the datatypes of each column
        error_details = []
        error_summary = defaultdict(lambda: defaultdict(int))

        for column, schema_info in expected_schema.items():
            expected_dtype = schema_info['dtype']
            regex_pattern = schema_info.get('regex', None)
            format_pattern = schema_info.get('format', None)
            nullable = schema_info.get('nullable', True)
            actual_dtype = df.schema[column]
        
            if actual_dtype != expected_dtype:
                if expected_dtype == bool:
                # Handle boolean separately
                    error_indices = (
                        df.filter(pl.col(column).is_not_null())
                        .filter(~pl.col(column).cast(pl.Utf8).str.contains_any(["True", "False","1","0","true","false","TRUE","FALSE"]))
                        .select("index")
                        .to_series()
                        .to_list()
                    )
                elif expected_dtype == datetime:
                    error_indices = (
                        df.filter(pl.col(column).is_not_null())
                        .filter(pl.col(column).str.to_date(format_pattern,strict=False).is_null())
                        .select("index")
                        .to_series()
                        .to_list()
                    )
                else:
                    error_indices = (
                        df.filter(pl.col(column).is_not_null())
                        .filter(pl.col(column).cast(expected_dtype, strict=False).is_null())
                        .select("index")
                        .to_series()
                        .to_list()
                    )

                error_values = (
                    df.filter(pl.col("index").is_in(error_indices))
                    .select(column)
                    .to_series()
                    .to_list()
                )
                for index, value in zip(error_indices, error_values):
                    if isinstance(value, str):
                        try:
                            actual_value = ast.literal_eval(value)
                        except (ValueError, SyntaxError):
                            actual_value = value
                    else:
                        actual_value = value

                    error_details.append(
                        {
                            "Column": column,
                            "Error Type": "Datatype Mismatch",
                            "Expected": expected_dtype.__name__,
                            "Actual": type(actual_value).__name__,
                            "Error Value": value,
                            "Index": index
                        }
                    )

            if regex_pattern:
                regex_error_indices = (
                    df.filter(pl.col(column).is_not_null())
                    .filter(~pl.col(column).str.contains(regex_pattern))
                    .select("index")
                    .to_series()
                    .to_list()
                )
                error_indices.extend(regex_error_indices)

                error_values = (
                    df.filter(pl.col("index").is_in(regex_error_indices))
                    .select(column)
                    .to_series()
                    .to_list()
                )
                for index, value in zip(regex_error_indices, error_values):
                    error_details.append(
                        {
                            "Column": column,
                            "Error Type": "Regex Mismatch",
                            "Expected": f"Value matching pattern: {regex_pattern}",
                            "Actual": value,
                            "Error Value": value,
                            "Index": index
                        }
                    )
            if not nullable:
                null_error_indices = (
                    df.filter(pl.col(column).is_null())
                    .select("index")
                    .to_series()
                    .to_list()
                )
                error_indices.extend(null_error_indices)

                for index in null_error_indices:
                    error_details.append(
                        {
                            "Column": column,
                            "Error Type": "Null Value",
                            "Expected": "Non-null value",
                            "Actual": "null",
                            "Error Value": "null",
                            "Index": index
                        }
                    )
        return sorted(error_details, key=lambda x: x['Index'])
    except Exception as e:
        raise DataQualityCheckError(f"Failed to validate data types: {str(e)}")

def write_error_details_to_pdf(error_details,duplicate_keys,special_char_rows):
    pdf_file = "datatype_validation_errors.pdf"
    c = canvas.Canvas(pdf_file, pagesize=letter)
    width, height = letter

    c.drawString(30, height - 30, "Datatype Validation Errors")
    c.drawString(30, height - 50, "----------------------------------------")

    y = height - 70
    for error in error_details:
        c.drawString(30, y, f"Column: {error['Column']}")
        c.drawString(30, y - 15, f"Error Type: {error['Error Type']}")
        c.drawString(30, y - 30, f"Expected: {error['Expected']}")
        c.drawString(30, y - 45, f"Actual: {error['Actual']}")
        c.drawString(30, y - 60, f"Error Value: {error['Error Value']}")
        c.drawString(30, y - 75, f"Index: {error['Index']}")
        c.drawString(30, y - 90, "----------------------------------------")
        y -= 105
        if y < 50:
            c.showPage()
            y = height - 30
    c.drawString(30, y, "Duplicate Keys")
    c.drawString(30, y - 20, "----------------------------------------")
    y -= 40
    for row in duplicate_keys.iter_rows(named=True):
        row_str = str(row)
        wrapped_text=textwrap.wrap(row_str,width=100)
        for line in wrapped_text:
            c.drawString(30, y, line)
            y -= 20
            if y < 50:
                c.showPage()
                y = height - 30
    # Add Special Characters section
    c.drawString(30, y, "Rows with Special Characters")
    c.drawString(30, y - 20, "----------------------------------------")
    y -= 40
    for row in special_char_rows.iter_rows(named=True):
        row_str = str(row)
        wrapped_text=textwrap.wrap(row_str,width=100)
        for line in wrapped_text:
            c.drawString(30, y, line)
            y -= 20
            if y < 50:
                c.showPage()
                y = height - 30

    c.save()

def write_error_details_to_csv(error_details,duplicate_keys,special_char_rows):
# Convert error details to a DataFrame and write to a CSV file
    error_df = pd.DataFrame(error_details)
    error_df.to_csv("datatype_mismatch.csv", index=False)

    # Convert duplicate keys to a DataFrame and write to a CSV file
    duplicate_keys_df = duplicate_keys.to_pandas()
    duplicate_keys_df.to_csv("duplicate_keys.csv", index=False)

    # Convert special character rows to a DataFrame and write to a CSV file
    special_char_rows_df = special_char_rows.to_pandas()
    special_char_rows_df.to_csv("special_characters.csv", index=False)


def read_file(csv_file_path, expected_schema, primary_key, pdf_report=False, csv_report=False):
    try:  # Check if the file exists before proceeding
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(
            f"Provided path was not able to be resolved: {csv_file_path}"
        )

    # Read the CSV file using Polars LazyFrame
        df = pl.scan_csv(
            csv_file_path, infer_schema_length=None, ignore_errors=True
            ).with_row_index(
        ) 

    # Process the data in batches
        batch_size = 10000
        error_details = []
        duplicate_keys = pl.DataFrame()
        duplicate_rows = pl.DataFrame()
        special_char_rows = pl.DataFrame()

        for df_batch in df.collect().iter_slices(batch_size):
            error_details.extend(validate_data_types(df_batch, expected_schema))
            duplicate_keys = duplicate_keys.vstack(check_for_duplicate_keys_polars(primary_key, df_batch))
            duplicate_rows = duplicate_rows.vstack(check_for_duplicate_rows_polars(df_batch))
            special_char_rows = special_char_rows.vstack(check_special_characters(df_batch, expected_schema))
        error_counts = defaultdict(lambda: defaultdict(int))
        for error in error_details:
            column = error['Column']
            error_type = error['Error Type']
            error_counts[column][error_type] += 1
            #print(
            #f"Column: {error['Column']}, Error Type: {error['Error Type']}, Expected: {error['Expected']}, Actual: {error['Actual']}, Error Value: {error['Error Value']}, Index: {error['Index']}"
         #)
        print("Datatype Error Summary:")
        print("----------------------------------------")
        for column, error_types in error_counts.items():
            for error_type, count in error_types.items():
                print(f"Column: {column}, Error Type: {error_type}, Count: {count}")
        # Count the number of errors
        datatype_error_count = len(error_details)
        duplicate_key_error_count = len(duplicate_keys)
        special_char_error_count = len(special_char_rows)
        # Write the error details to a PDF file
        if pdf_report==True:
            write_error_details_to_pdf(error_details,duplicate_keys,special_char_rows)
        # Write the error details to a CSV file
        if csv_report==True:
            write_error_details_to_csv(error_details,duplicate_keys,special_char_rows)
        print("Error Summary:")
        print("----------------------------------------")
        print(f"Total datatype_errors: {datatype_error_count}")
        print(f"Total duplicate_key_errors: {duplicate_key_error_count}")
        print(f"Total special_char_errors: {special_char_error_count}")
        return error_details, duplicate_keys, special_char_rows
    except Exception as e:
        raise DataQualityCheckError(f"An error occurred during data quality check: {str(e)}")
    