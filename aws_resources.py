import os
import json
import boto3
from snowflake.snowpark import Session
 
class aws_resource:
 
    aws_session = None
    def __init__(self,region="us-east-1") -> None:
        aws_resource.aws_session = boto3.session.Session(region_name=region)
       
    @classmethod
    def get_s3_client(cls,region="us-east-1"):
       
        if cls.aws_session == None or cls.aws_session.region_name != region:
            cls(region)
 
        return cls.aws_session.resource("s3")
   
    @classmethod
    def get_secretsmanager_client(cls,region="us-east-1"):
       
        if cls.aws_session == None or cls.aws_session.region_name != region:
            cls(region)
        return cls.aws_session.client(service_name="secretsmanager")
