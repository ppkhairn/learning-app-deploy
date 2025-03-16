# import packages
import logging.config
import pytest
from src.simple_aws_assets import AwsAssets
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from src.simple_aws_assets import file_folder  # Import file_folder from the main script
from moto import mock_aws
import boto3
import os
import logging
from decimal import Decimal
logging.basicConfig(level=logging.INFO)

@pytest.fixture
def aws_assets() -> AwsAssets:
    return AwsAssets(3, 4, bucket_name="learning-tests", table_name="learning-tests-db-test")

# This fixture if to test the df_2_csv function.
@pytest.fixture
def aws_2_csv() -> AwsAssets:
    """
    This fixture if to test the df_2_csv function.
    Make sures we have the dataframe df before we call the df_2_csv method.
    """
    obj = AwsAssets(6, 8, bucket_name="learning-tests")
    obj.create_df()

    return obj

# This fixture is for mocking the aws and to test the upload to s3 function
@pytest.fixture
def aws_mock(aws_2_csv):
    """
    """
    # Check if the file is present in the temp_files folder to test.
    test_file_path = file_folder / "test_aws_s3.csv"
    if not test_file_path.exists():
        aws_2_csv.df_2_csv(test_file_path)
    with mock_aws():
        
        s3_client = boto3.client("s3", region_name = "us-east-1")
        bucket_name = "learning-tests"
        s3_client.create_bucket(Bucket = bucket_name)
        print(f"Created S3 bucket: {bucket_name}")
        yield s3_client

# This fixture is for mocking the aws and to test the dynamoDB create table function
# if the table does not exist in mck aws. basically tests if the table is created if not exist.
@pytest.fixture
def aws_mock_db():
    """
    """
    with mock_aws():

        dynamodb = boto3.resource("dynamodb", region_name = "us-east-1")
        dynamodb_client = boto3.client("dynamodb", region_name = "us-east-1")
        yield dynamodb, dynamodb_client
    
# This fixture is for mocking the aws and to test the dynamoDB create table function
# if the table exist in mck aws. basically tests if the table is not created if exist.
@pytest.fixture
def aws_mock_db_yes():
    """
    """
    with mock_aws():

        dynamodb = boto3.resource("dynamodb", region_name = "us-east-1")
        dynamodb_client = boto3.client("dynamodb", region_name = "us-east-1")
        table_name = "learning-tests-db-test-no-tab"
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"}  # Partition Key
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"}  # S = String
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        )

        yield dynamodb, dynamodb_client

@pytest.fixture
def aws_assets_tab() -> AwsAssets:
    return AwsAssets(3, 4, bucket_name="learning-tests", table_name="learning-tests-db-test-no-tab")              

def test_add(aws_assets) -> None:

    assert aws_assets.add() == 7

def test_sub(aws_assets) -> None:

    assert aws_assets.sub() == -1

def test_mul(aws_assets) -> None:

    assert aws_assets.mul() == 12

def test_dist(aws_assets) -> None:

    assert aws_assets.dist() == 5

    aws_assets.update_numbers(6, 8)

    assert aws_assets.dist() == 10

def test_div(aws_assets) -> None:

    assert aws_assets.div() == 0.75

    aws_assets.update_numbers(2, 3)

    assert aws_assets.div() == pytest.approx(0.6666667, 1e-4)

def test_div_with_zero(aws_assets) -> None:

    aws_assets.update_numbers(4, 0)

    with pytest.raises(ZeroDivisionError):
        aws_assets.div()

def test_create_df(aws_assets) -> None:

    df_ = aws_assets.create_df()
    assert isinstance(df_, pd.DataFrame)
    
    len_ = len(df_.columns)
    len_true = sum(df_.columns == ['value_1', 'value_2', 'add', 'sub', 'div', 'mul', 'dist'])
    assert len_ == len_true

def test_df_2_csv(aws_2_csv) -> None:

    """
    Test if df_2_csv calls to_csv with the expected file path.
    This tests if to_csv was passed with the exact file name arg.
    """
    expected_path = file_folder / "temp_f.csv"  # Ensure this matches the actual function
    with patch.object(pd.DataFrame, "to_csv") as mock_to_csv:

        aws_2_csv.df_2_csv()
        mock_to_csv.assert_called_once_with(expected_path)

def test_upload_to_s3(aws_mock, aws_2_csv) -> None:
    """
    """
    # bucket_name = "learning-tests"
    # s3_key = 
    filename = "test_aws_s3.csv"
    file_path = file_folder / filename
    # s3_client = aws_mock
    # with mock_aws():
    aws_2_csv.upload_to_s3(filename)

    # Check if the file was uploaded to the S3 bucket
    s3_client = aws_mock
    response = s3_client.list_objects_v2(Bucket="learning-tests", Prefix="uploads/")
    list_buckets = s3_client.list_buckets()
    # Extract the file names from the response
    file_names = [obj["Key"] for obj in response.get("Contents", [])]

    assert f"uploads/{filename}" in file_names

    if file_path.exists():
        os.remove(file_path)


def test_check_or_create_function_table(aws_mock_db, aws_assets) -> None:
    """
    Test function to check and create a dynamoDB table in mock aws. 
    This function is to test the following scenario: no table "learning-tests-db-test" exist and 
    the function is successful in creating a table. The case where the table already exist is covered in 
    the next test function "test_check_or_create_function_yes_table"
    """
    # This section is to ensure there is no existing table in mock aws

    dynamodb_resource, dynamodb_client = aws_mock_db
    response = dynamodb_client.list_tables()
    table_in_db = response.get('TableNames', [])
    assert len(table_in_db) == 0

    # This section is to ensure that the table named "learning-tests-db-test" was created in  mock aws
    tab = aws_assets.check_or_create_table()
    assert tab == aws_assets.dynamodb.Table(name=aws_assets.table_name) # returns the table name that function tried to create

    # dynamodb_resource, dynamodb_client = aws_mock_db
    response = dynamodb_client.list_tables()
    table_in_db = response.get('TableNames', [])[0]
    assert table_in_db == aws_assets.table_name # Ensures that the table was created in an empty mock aws dynamoDB

def test_check_or_create_function_yes_table(aws_mock_db_yes, aws_assets_tab) -> None:
    """
    Test function to check and create a dynamoDB table in mock aws. 
    This function is to test the following scenario: Table "learning-tests-db-test" already exist and 
    the function is successful in bypassing the creating a table in the function and checks if the table already exists."
    """
    # This section checks if the table "learning-tests-db-test-no-tab" already exists in the dynamoDB database.
    # The table should be created in the fixture "aws_mock_db_yes" used as an arg in this function.
    dynamodb_resource, dynamodb_client = aws_mock_db_yes
    response = dynamodb_client.list_tables()
    table_in_db = response.get('TableNames', [])[0]
    assert table_in_db == aws_assets_tab.table_name

    # This section checks if the "check_or_create_table()" went through.
    # This automatically ensures that the function "check_or_create_table()" has bypassed the create table and just checked if table 
    # already exist. It also ensures that no table overwrite has happened as dynamoDB does not allow that and if the function
    # doesn't bypass the create_table, then boto3 raises "table already exist" error.
    # Therefore the assert line below, if successful ensures the above success.
    tab = aws_assets_tab.check_or_create_table()
    assert tab == aws_assets_tab.dynamodb.Table(name=aws_assets_tab.table_name)

def test_insert_data(aws_mock_db, aws_assets) -> None:
    """
    This function is to test insert_data() function.
    """
    dynamodb_resource, dynamodb_client = aws_mock_db

    # This section checks or create the table "learning-tests-db-test" to mock aws
    # asserts if the table was created with the same name.
    tab = aws_assets.check_or_create_table() 
    assert tab == aws_assets.dynamodb.Table(name=aws_assets.table_name)

    # This section adds the items to the table "learning-tests-db-test".
    itm = aws_assets.insert_data()
    response = dynamodb_resource.Table(f"{aws_assets.table_name}").get_item(Key={'id': f"{aws_assets.x}-{aws_assets.y}"})
    inserted_items = response.get("Item", {})
    expected_dict = {'id': '3-4', 'value_1': Decimal('3'), 
                     'value_2': Decimal('4'), 'add': Decimal('7'), 
                     'sub': Decimal('-1'), 'div': Decimal('0.75'), 
                     'mul': Decimal('12'), 'dist': Decimal('5.0')}
    assert inserted_items == expected_dict
