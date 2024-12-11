from datetime import date
import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-west-2:767398004946:s3-lambda-sns-cicd'
s3_target_arn="arn:aws:s3:::doordash-target-zn-dsil"
s3_landing_arn="arn:aws:s3:::doordash-landing-zn-dsil"

def lambda_handler(event, context):
    # TODO implement
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print(resp['Body'])
        json_data=json.loads(resp)
        s3_data = pd.read_json(resp['Body'].read().split('\r\n'))
        df= pd.DataFrame(columns=['id','status','amount','date'])
        for line in s3_data:
            py_dict = json.loads(line)
            if py_dict['status'] == 'delievered':
                df.loc=[py_dict['id']] = py_dict
        df.to_csv('/tmp/test.csv',sep=',')
        print ('test.csv file ceated')
        try:
            date_var=str(date.today())
            file_name='processed_data/{}_processed_data.csv'.format(date_var)
        except:
            file_name='processed_data/processed_data.csv'
        lambda_path= 'tmp/test.csv'
        s3=boto3.resource('s3')
        bucket = s3.Bucket(doordash-landing-zn-dsil)
        bucket.upload_file(lambda_path,file_name)
        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=os.getenv(sns_arn),
                                      Message="File {} has been formatted and filtered. Its been stored in {} as format {}".format(s3_file_key,bucket_name,file_name))
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')