import json
import boto3
import re
import pandas as pd
from datetime import date

# Initialize clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Constants
sns_arn = 'arn:aws:sns:us-west-2:767398004946:s3-lambda-sns-cicd'
s3_target_bucket = "doordash-target-zn-dsil"

def lambda_handler(event, context):
    print("Event received:", event)

    try:
        # Extract bucket name and object key
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print("Bucket:", bucket_name)
        print("File Key:", s3_file_key)

        # Get the S3 object
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        file_content = response['Body'].read().decode('utf-8')
        print("S3 file content:", file_content)

        # Use regex to extract all JSON objects from the file content
        json_objects = re.findall(r'\{.*?\}', file_content)

        # Parse each JSON object and create a list
        json_data = []
        for json_object in json_objects:
            try:
                record = json.loads(json_object)
                if 'status' in record:  # Only include records with 'status'
                    json_data.append(record)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON object skipped: {json_object}, Error: {e}")

        if not json_data:
            raise ValueError("No valid records with 'status' key found in the file!")

        # Create DataFrame and filter data
        df = pd.DataFrame(json_data)
        print("Loaded DataFrame:", df)

        filtered_df = df[df['status'] == 'delivered']
        print("Filtered DataFrame:", filtered_df)

        # Save filtered data to a CSV file
        output_file_path = '/tmp/test.csv'
        filtered_df.to_csv(output_file_path, index=False)
        print("CSV file created:", output_file_path)

        # Upload processed file to S3
        date_var = str(date.today())
        s3_target_key = f'processed_data/{date_var}_processed_data.csv'
        s3_client.upload_file(output_file_path, s3_target_bucket, s3_target_key)
        print("Processed file uploaded to S3:", s3_target_key)

        # Send SNS notification
        message = f"Input S3 File s3://{bucket_name}/{s3_file_key} has been processed successfully!"
        sns_client.publish(
            Subject="SUCCESS - Daily Data Processing",
            TargetArn=sns_arn,
            Message=message
        )
        print("SNS notification sent:", message)

    except Exception as err:
        print("Error occurred:", err)
        # Send failure notification
        failure_message = f"Input S3 File s3://{bucket_name}/{s3_file_key} processing failed. Error: {err}"
        sns_client.publish(
            Subject="FAILED - Daily Data Processing",
            TargetArn=sns_arn,
            Message=failure_message
        )
