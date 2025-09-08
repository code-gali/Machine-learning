import os
import yaml
from datetime import datetime
from aws_resources import get_boto3_client
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from dependencies import SnowFlakeConnector
import asyncio
from llm_client import LLMClient


def build_message(prompt, records):
    # Combine 5 records into one content string
    content = prompt + "\n" + "\n".join([str(record) for record in records])
    return {"role": "user", "content": content}

async def process_batch(batch, prompt):
    # Each batch is 5 records
    message = build_message(prompt, batch)
    llm = LLMClient()
    return await llm.generate_response([message])

async def main_async_llm(result_df, prompt):
    iterator = result_df.to_local_iterator()
    batch_size = 15
    records = []
    while True:
        try:
            for _ in range(batch_size - len(records)):
                records.append(next(iterator))
        except StopIteration:
            pass
        if not records:
            break
        # Split into 3 tasks, 5 records each
        tasks = []
        for i in range(3):
            batch = records[i*5:(i+1)*5]
            if batch:
                tasks.append(process_batch(batch, prompt))
        if tasks:
            responses = await asyncio.gather(*tasks)
            # Handle responses as needed
        records = []


def load_config():
    config_path = os.environ.get('CONFIG_DEV_PATH', 'config_dev.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['snowflake'], config['s3']

def handle_batch_control(s3_cfg):
    s3 = get_boto3_client()
    S3_BUCKET = s3_cfg['bucket']
    S3_KEY = s3_cfg['control_file_key']
    S3_INPROGRESS_KEY = s3_cfg['inprogress_file_key']
    # Check for inprogress file
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=S3_INPROGRESS_KEY)
        print('Duplicate run: inprogress file exists. Exiting job.')
        exit(0)
    except s3.exceptions.ClientError:
        pass
    # Copy control file to inprogress file
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={'Bucket': S3_BUCKET, 'Key': S3_KEY},
        Key=S3_INPROGRESS_KEY
    )
    # Download control file and get last end date
    import pandas as pd
    import io
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    ctrl_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    last_end_date = ctrl_df['paid end date'].iloc[0]
    return s3, S3_BUCKET, S3_KEY, S3_INPROGRESS_KEY, ctrl_df, last_end_date

def update_control_file(s3, S3_BUCKET, S3_KEY, S3_INPROGRESS_KEY, ctrl_df, paid_start_date, paid_end_date):
    from datetime import datetime
    import io
    ctrl_df['last run date'] = datetime.now().strftime('%Y-%m-%d')
    ctrl_df['paid start date'] = paid_start_date
    ctrl_df['paid end date'] = paid_end_date
    csv_buf = io.StringIO()
    ctrl_df.to_csv(csv_buf, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=csv_buf.getvalue())
    s3.delete_object(Bucket=S3_BUCKET, Key=S3_INPROGRESS_KEY)

def get_paid_dates(last_end_date):
    from datetime import datetime
    paid_start_date = os.environ.get('PAID_START_DATE', last_end_date)
    paid_end_date = os.environ.get('PAID_END_DATE', datetime.now().strftime('%Y-%m-%d'))
    return paid_start_date, paid_end_date

def get_snowflake_session(sf_cfg):
    sf_conn = SnowFlakeConnector.get_conn(
        sf_cfg['conn_name'],
        sf_cfg['conn_param']
    )
    return Session.builder.configs({"connection": sf_conn}).create()

#Extract Claim data 
def get_result_df(snowpark_session, sf_cfg, paid_start_date, paid_end_date):
    claim = snowpark_session.table(sf_cfg['claim_table'])
    attach = snowpark_session.table(sf_cfg['attach_table'])
    note = snowpark_session.table(sf_cfg['note_table'])
    gb_clm_line = snowpark_session.table(sf_cfg['gb_clm_line_table'])
    return (
        claim
        .join(attach, claim["ATXR_SOURCE_ID"] == attach["ATXR_SOURCE_ID"])
        .join(note, attach["ATXR_DEST_ID"] == note["ATXR_DEST_ID"])
        .join(gb_clm_line, gb_clm_line["GB_CLM_NBR"] == claim["CLCL_ID"])
        .filter((gb_clm_line["CLM_PAID_DT"] >= paid_start_date) & (gb_clm_line["CLM_PAID_DT"] <= paid_end_date))
        .select(
            claim["CLCL_ID"],
            gb_clm_line["CLM_PAID_DT"],
            claim["ATXR_SOURCE_ID"],
            attach["ATXR_DEST_ID"],
            attach["ATXR_DESC"],
            note["ATND_SEQ_NO"],
            note["ATND_TEXT"]
        )
    )

#COB Data validation 

def main():
    sf_cfg, s3_cfg = load_config()
    s3, S3_BUCKET, S3_KEY, S3_INPROGRESS_KEY, ctrl_df, last_end_date = handle_batch_control(s3_cfg)
    paid_start_date, paid_end_date = get_paid_dates(last_end_date)
    snowpark_session = get_snowflake_session(sf_cfg)
    result_df = get_result_df(snowpark_session, sf_cfg, paid_start_date, paid_end_date)

    # Example usage for LLM
    prompt = "Your prompt here"
    asyncio.run(main_async_llm(result_df, prompt))

    # Write to target table from config
    result_df.write.save_as_table(sf_cfg['target_table'], mode="overwrite")
    snowpark_session.close()

    # Update control file and remove inprogress file
    update_control_file(s3, S3_BUCKET, S3_KEY, S3_INPROGRESS_KEY, ctrl_df, paid_start_date, paid_end_date)

if __name__ == "__main__":
    main()
