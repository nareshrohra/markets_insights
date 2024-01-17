import boto3
from boto3.dynamodb.conditions import Key
import csv
from itertools import islice
import logging
import sys

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')

def upload_to_dynamodb(filename):
  # Configure logging
  logging.basicConfig(filename='dynamodb_update-{filename}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

  # Local CSV file path
  local_csv_path = 'C:/Users/SPEERU/Downloads/' + filename

  # DynamoDB table
  table = dynamodb.Table('00109-qa-due-diligence-test')

  #<continue code here>
  

if __name__ == "__main__":
    # Check if a filename is provided as a command-line argument
    if len(sys.argv) != 2:
        print("Usage: python script.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    upload_to_dynamodb(filename)
