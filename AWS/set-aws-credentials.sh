#!/bin/bash

if [ -f ".env" ]; then
  export $(cat .env | xargs)
fi

if [ -z "$aws_access_key_id" ]; then
  echo "aws_access_key_id not found in .env file"
  exit 1
fi

if [ -z "$aws_secret_access_key" ]; then
  echo "aws_secret_access_key not found in .env file"
  exit 1
fi

if [ -z "$aws_session_token" ]; then
  echo "aws_session_token not found in .env file"
  exit 1
fi

if [ -z "$region" ]; then
  echo "region not found in .env file"
  exit 1
fi

aws configure set aws_access_key_id "$aws_access_key_id"
aws configure set aws_secret_access_key "$aws_secret_access_key"
aws configure set aws_session_token "$aws_session_token"
aws configure set region "$region"

echo "AWS credentials set successfully"
