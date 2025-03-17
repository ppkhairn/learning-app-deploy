# import packages
# from src.add_nums import add, mul
import pandas as pd
from pathlib import Path
import boto3
import logging
import botocore.exceptions
from decimal import Decimal ## DynamoDB does not support float. Therefore converting data to Decimal before uploading to Dynamodb (line #152-#158)

# Manage paths
scr_path = Path(__file__)
base_folder = scr_path.parent
root_folder = base_folder.parent
file_folder = root_folder / "temp_files"

# create s3 instance
"""
*************************************************************
*************************************************************
Very important:

The s3_client defined below is at a global level. 
if we don't define s3_client in the upload_to_s3() function below again,
this might create a lot of issues while testing.
1) While testing, we want to mock the aws services and not use actual services.
2) Even though we create a mock_aws pytest.fixture and pass it through the test_upload_to_s3() function,
   the services might be initiated as mocked as given in the fixture but as soon as it enters the upload_to_s3() function,
   the function will start using real aws srevices.
3) The reason being: By ensuring that boto3.client("s3") is inside the method, the moto library can properly mock the client 
   when the upload_to_s3() method is called during testing. Without this, if the s3_client was defined outside 
   (or at the global level), the mocked s3_client wouldn't be used by the method, and the actual S3 service might be called instead.
4) So it is better to define the s3_client in the class or in the function.

#########################################
5) I am commenting out the s_client on line #45 here for the above reasons.

6) We can either add the s3_client definition to the individual function runnning tests like upload_to_s3() [since tests/test_upload_tos3()
   uses the function upload_to_s3()] or we can define the s3_client in the class __init__ like I have done below.

7) If you wanna define s3_client in the functions, see commented line #104.
8) If you want to define s3_client in the class, see line #56 below. (This is what this code is using.)
*************************************************************
*************************************************************
"""
# s3_client = boto3.client("s3")   

class AwsAssets():

    def __init__(self, x: float, y: float, bucket_name: str = "learning-tests", 
                 table_name: str = "learning-tests-db") -> None:

        self.x = x
        self.y = y
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.s3_client = boto3.client("s3", region_name='us-east-1')
        self.dynamodb = boto3.resource("dynamodb", region_name='us-east-1')      

    def add(self) -> float:

        return self.x + self.y
    
    def sub(self) -> float:

        return self.x - self.y
    
    def mul(self) -> float:

        return self.x * self.y
    
    def div(self) -> float:

        if self.y == 0:
            raise ZeroDivisionError("Cannot divide by Zero!")
        
        return self.x / self.y
    
    def dist(self) -> float:

        return ((self.x**2)+(self.y**2))**0.5
    
    def create_df(self) -> pd.DataFrame:

        df = {"value_1": [self.x],
                "value_2": [self.y],
                "add": [self.add()],
                "sub": [self.sub()],
                "div": [self.div()],
                "mul": [self.mul()],
                "dist": [self.dist()]}
        
        self.pdf = pd.DataFrame(df)
        
        return self.pdf
    
    def df_2_csv(self, filename: str = "temp_f.csv") -> None:

        self.pdf.to_csv(file_folder / filename)

        return None
    
    def upload_to_s3(self, file_name: str = "temp_f.csv") -> None:

        # s3_client = boto3.client("s3")
        local_file_path = file_folder / file_name
        s3_key = "uploads/" + file_name 
        try:
            a = self.s3_client.list_buckets()["Buckets"]
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            logging.info(f"File upload {s3_key} successful to bucket {self.bucket_name}")
        except botocore.exceptions.ClientError as e:
            # Catch other AWS errors (e.g., wrong bucket, insufficient permissions)
            error_code = e.response["Error"]["Code"]
            logging.error(f"❌ AWS ClientError: {error_code} - {e.response['Error']['Message']}")
        
        return None
    
    def check_or_create_table(self) -> None:
        """Check if a DynamoDB table exists, and create it if not."""
        try:
            table = self.dynamodb.Table(self.table_name)
            table.load()  # Try loading the table metadata to check existence
            print(f"Table '{self.table_name}' already exists.")
            return table
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f"Table '{self.table_name}' not found. Creating...")
                table = self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {"AttributeName": "id", "KeyType": "HASH"}  # Primary key
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "id", "AttributeType": "S"}
                    ],
                    ProvisionedThroughput={
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5
                    }
                )
                table.wait_until_exists()
                print(f"Table '{self.table_name}' created successfully.")
                return table
            else:
                raise

    def insert_data(self) -> None:
        """Insert data into the DynamoDB table."""
        table = self.dynamodb.Table(self.table_name)

        item = {"id": f"{self.x}-{self.y}",
                "value_1": Decimal(str(self.x)),
                "value_2": Decimal(str(self.y)),
                "add": Decimal(str(self.add())),
                "sub": Decimal(str(self.sub())),
                "div": Decimal(str(self.div())),
                "mul": Decimal(str(self.mul())),
                "dist": Decimal(str(self.dist()))}
        
        table.put_item(Item=item)
        print(f"Data inserted into '{self.table_name}'.")
        return None
    
    def update_numbers(self, x: float, y: float) -> None:

        self.x = x
        self.y = y
    