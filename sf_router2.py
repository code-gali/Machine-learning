from fastapi import (
 BackgroundTasks,
 File,
 FastAPI,
 UploadFile,
 HTTPException,
 Form,  
 status,
)
from pydantic import BaseModel, Json
from datetime import datetime
from fastapi import (
    APIRouter,
    Depends,
    Query,
    Body,
    Header
)
from fastapi.responses import StreamingResponse,JSONResponse
from pydantic import (
    BaseModel,
    Field,
    ValidationError
)
from typing import (List,Optional)
from typing_extensions import (
    Annotated,
    Literal
)
from logging import Logger
from models import (
     CompleteQryModel,
     AiCompleteQryModel,
     GenAiCortexAudit,
     Txt2SqlModel,
     AgentModel,
     SqlExecModel,
     SearchModel,
     AnalystModel,
     LoadVectorModel,
     UploadFileModel
)
from prompts import (
    get_conv_response,
)
from config import GenAiEnvSettings
from dependencies import (
    get_config,
    get_logger,
    get_load_timestamp,
    ValidApiKey,
    SnowFlakeConnector,
    log_response,
    update_log_response,
    get_cortex_search_details,
    get_cortex_analyst_details,
    get_load_vector_data
)
#from ReduceReuseRecycleGENAI.api import get_api_key
#from ReduceReuseRecycleGENAI.snowflake import snowflake_conn
import httpx
import json
import uuid
import re
from functools import partial
from upload_your_data import read_file_extract
route = APIRouter(
    prefix="/api/cortex"
)
from   snowflake.connector.errors import DatabaseError
from datetime import datetime, date
 
 
@route.post("/complete")
async def llm_gateway(
        query: Annotated[CompleteQryModel,Body(embed=True)],
        config: Annotated[GenAiEnvSettings,Depends(get_config)],
        logger: Annotated[Logger,Depends(get_logger)],
        background_tasks: BackgroundTasks,
        get_load_datetime: Annotated[datetime,Depends(get_load_timestamp)]
 
):
    prompt = query.prompt.messages[-1].content
    messages_json = query.prompt.messages
   
    #The API key validation and generation has been pushed to backend; the api_validator will return True if API key is valid for the application.
    api_validator = ValidApiKey()
    try :
        if api_validator(query.api_key,query.aplctn_cd,query.app_id):
            try:
                sf_conn = SnowFlakeConnector.get_conn(
                    query.aplctn_cd,
                    query.app_lvl_prefix,
                    query.session_id,
                )
 
                sf_cursor = sf_conn.cursor()
                print(sf_cursor.execute("SELECT CURRENT_USER()"))
                print(sf_cursor.execute("SELECT CURRENT_ROLE()"))
                print(sf_cursor.execute("SELECT CURRENT_AVAILABLE_ROLES()"))
                print(sf_cursor.execute("SELECT CURRENT_ACCOUNT()"))
                print(sf_cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();"))
 
            except DatabaseError as e:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to resources"
                )
            clnt = httpx.AsyncClient(verify=False)
 
            provisioned_dict = {"provisioned_throughput_id": config.provisioned_id} if query.model in config.provisioned_models else {}
            request_body = {
                "model": query.model,
                "messages": [
                    {
                        "role": "user",
                        "content": query.sys_msg + get_conv_response(messages_json, query.limit_convs) + prompt
                    }
                ],
                **provisioned_dict
            }
 
            headers = {
                "Authorization": f'Snowflake Token="{sf_conn.rest.token}"',
                "Content-Type": "application/json",
                "Accept": "application/json",
                "method":"cortex",
                "api_key":query.api_key
            }
 
            url = getattr(config.COMPLETE, "{}_host".format(config.env))
            response_text = []
            query_id = [None]
            fdbck_id = [str(uuid.uuid4())]
           
            async def data_streamer():
                """
                Stream data from the service and yield responses with proper exception handling.
                """
                try:
                    async with clnt.stream('POST', url, headers=headers, json=request_body) as response:
                        if response.is_client_error:
                        error_message = await response.aread()
                        status_code = response.status_code
                        decoded_error = error_message.decode("utf-8")

                        if status_code == 429:
                            raise HTTPException(
                                status_code=429,
                                detail="Too many requests. Please implement retry with backoff. Original message: " + decoded_error
                            )
                        elif status_code == 402:
                            raise HTTPException(
                                status_code=402,
                                detail="Budget exceeded. Check your Cortex token quota. " + decoded_error
                            )
                        elif status_code == 400:
                            raise HTTPException(
                                status_code=400,
                                detail="Bad request sent to Snowflake Cortex. " + decoded_error
                            )
                        else:
                            raise HTTPException(
                                status_code=status_code,
                                detail=decoded_error
                            )
                        if response.is_server_error:
                            error_message = await response.aread()
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=error_message.decode("utf-8")
                            )
 
                        # Stream the response content
                        async for result_chunk in response.aiter_bytes():
                            for elem in result_chunk.split(b'\n\n'):
                                if b'content' in elem:  # Check for data presence
                                    try:
                                        chunk_dict = json.loads(elem.replace(b'data: ', b''))
                                        print(chunk_dict)
                                        full_response = chunk_dict['choices'][0]['delta']['text']
                                        full_response = full_response
                                        response_text.append(full_response)
                                        yield full_response#result_chunk
                                        query_id[0] =  chunk_dict['id']
                                    except json.JSONDecodeError as e:
                                        logger.error(f"Error decoding JSON: {e}")
                                        yield json.dumps({"error": "Error decoding JSON", "detail": str(e)})
                                        continue
                        yield "end_of_stream"
                        responses = {
                    "prompt":prompt,
                    "query_id": query_id[0],
                    "fdbck_id": fdbck_id[0] }
                    full_final_response = "".join(response_text)
                    yield json.dumps(responses)
 
                except httpx.RequestError as e:
                    logger.error(f"Request error: {e}")
                    yield json.dumps({"detail": str(e)})
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    yield json.dumps({"detail": str(e)})
           
                #Model recreated the for the Audit record
                audit_rec = GenAiCortexAudit(
                    edl_load_dtm = get_load_datetime,
                    edl_run_id = "0000",
                    edl_scrty_lvl_cd = "NA",
                    edl_lob_cd = "NA",
                    srvc_type = "complete",
                    aplctn_cd = config.pltfrm_aplctn_cd,
                    user_id = "Complete_User",#query.user_id,
                    mdl_id = query.model,
                    cnvrstn_chat_lmt_txt = query.limit_convs,
                    sesn_id = query.session_id,
                    prmpt_txt = prompt.replace("'","\\'"),
                    tkn_cnt = "0",
                    feedbk_actn_txt = "",
                    feedbk_cmnt_txt = "",
                    feedbk_updt_dtm = get_load_datetime,
                )
                #background_tasks.add_task(log_response,audit_rec,query_id,str(full_final_response),fdbck_id,query.session_id)
            return StreamingResponse(data_streamer(),media_type='text/event-stream')
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="unauthenticated user"
            )
   
    except HTTPException as e:
        logger.error(f"Request error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(e)
            )
 
@route.post("/v2/complete")
async def llm_gateway(
        api_key: Annotated[str | None, Header()],
        query: Annotated[AiCompleteQryModel, Body(embed=True)],
        config: Annotated[GenAiEnvSettings, Depends(get_config)],
        logger: Annotated[Logger, Depends(get_logger)],
        background_tasks: BackgroundTasks,
        get_load_datetime: Annotated[datetime, Depends(get_load_timestamp)]          
):
    prompt = query.prompt.messages[-1].content
    messages_json = query.prompt.messages
    session_id = "4533"
   
    # The API key validation and generation has been pushed to backend; the api_validator will return True if API key is valid for the application.
    api_validator = ValidApiKey()
    try:
        if api_validator(api_key, query.application.aplctn_cd, query.application.app_id):
            try:
                sf_conn = SnowFlakeConnector.get_conn(
                    query.application.aplctn_cd,
                    query.application.app_lvl_prefix,
                    session_id
                )
            except DatabaseError as e:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to resources"
                )
           
            clnt = httpx.AsyncClient(verify=False)
            pre_response_format = {"response_format": query.response_format.model_dump()} if query.response_format.schema else {}
           
            # Add provisioned throughput if model is in provisioned models list
            provisioned_dict = {"provisioned_throughput_id": config.provisioned_id} if query.model.model in config.provisioned_models else {}
           
            request_body = {
                "model": query.model.model,
                "messages": [
                    {
                        "role": "user",
                        "content": get_conv_response(messages_json) + prompt
                    }
                ],
                **query.model.options,
                **pre_response_format,
                **provisioned_dict
            }
            print("req", request_body)
 
            headers = {
                "Authorization": f'Snowflake Token="{sf_conn.rest.token}"',
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
 
            url = getattr(config.COMPLETE, "{}_host".format(config.env))
            response_text = []
            query_id = [None]
            fdbck_id = [str(uuid.uuid4())]
 
            async def data_streamer():
                """
                Stream data from the service and yield responses with proper exception handling.
                """
                vModel = query.model.model  # Model reference
                Created = datetime.utcnow().isoformat()  # Creation timestamp
               
                # Initialize response aggregation
                response_agg = {
                    "content": "",
                    "usage": {},
                    "tool_use": {}
                }
               
                try:
                    async with clnt.stream('POST', url, headers=headers, json=request_body) as response:
                        if response.is_client_error:
                        error_message = await response.aread()
                        status_code = response.status_code
                        decoded_error = error_message.decode("utf-8")

                        if status_code == 429:
                            raise HTTPException(
                                status_code=429,
                                detail="Too many requests. Please implement retry with backoff. Original message: " + decoded_error
                            )
                        elif status_code == 402:
                            raise HTTPException(
                                status_code=402,
                                detail="Budget exceeded. Check your Cortex token quota. " + decoded_error
                            )
                        elif status_code == 400:
                            raise HTTPException(
                                status_code=400,
                                detail="Bad request sent to Snowflake Cortex. " + decoded_error
                            )
                        else:
                            raise HTTPException(
                                status_code=status_code,
                                detail=decoded_error
                            )
                        if response.is_server_error:
                            error_message = await response.aread()
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=error_message.decode("utf-8")
                            )
 
                        # Stream the response content
                        async for result_chunk in response.aiter_bytes():
                            for elem in result_chunk.split(b'\n\n'):
                                if b'content' in elem:  # Check for data presence
                                    try:
                                        json_line = json.loads(elem.replace(b'data: ', b''))
                                       
                                        # Update usage information
                                        response_agg["usage"].update(json_line.get("usage", {}))
                                       
                                        # Extract query ID if available
                                        query_id[0] = json_line.get('id', query_id[0])
                                       
                                        # Process delta chunk
                                        delta_chunk = json_line["choices"][0].get("delta", {})
                                       
                                        # Handle text content
                                        if delta_chunk.get("type") == "text":
                                            response_agg["content"] += delta_chunk.get("content", "")
                                       
                                        # Store for backward compatibility
                                        if delta_chunk.get("content"):
                                            response_text.append(delta_chunk.get("content", ""))
                                           
                                    except json.JSONDecodeError as e:
                                        logger.error(f"Error decoding JSON: {e}")
                                        yield json.dumps({"error": "Error decoding JSON", "detail": str(e)})
                                        continue
 
                except httpx.RequestError as e:
                    logger.error(f"Request error: {e}")
                    yield json.dumps({"detail": str(e)})
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    yield json.dumps({"detail": str(e)})
 
                # Final response
                full_final_response = response_agg["content"]
               
                # Yield the aggregated response
                yield json.dumps({
                    "model": vModel,
                    "created": Created,
                    **response_agg
                })
               
                # Extract token count for audit
                token_count = response_agg["usage"].get("total_tokens", 0)
               
                # Update audit record
                audit_rec = GenAiCortexAudit(
                    edl_load_dtm=get_load_datetime,
                    edl_run_id="0000",
                    edl_scrty_lvl_cd="NA",
                    edl_lob_cd="NA",
                    srvc_type="complete",
                    aplctn_cd=config.pltfrm_aplctn_cd,
                    user_id="Complete_User",
                    mdl_id=vModel,
                    cnvrstn_chat_lmt_txt="0",
                    sesn_id=session_id,
                    prmpt_txt=prompt.replace("'", "\\'"),
                    tkn_cnt=str(token_count),
                    feedbk_actn_txt="",
                    feedbk_cmnt_txt="",
                    feedbk_updt_dtm=get_load_datetime,
                )
               
                background_tasks.add_task(
                    log_response,
                    audit_rec,
                    query_id,
                    str(full_final_response),
                    fdbck_id,
                    session_id
                )
 
            return StreamingResponse(data_streamer(), media_type='text/event-stream')
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="unauthenticated user"
            )
    except HTTPException as e:
        logger.error(f"Request error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
 
 
@route.post("/search_details/")
async def get_search_details(
        search_input: Annotated[List,Depends(SearchModel)]):
    try:
        return get_cortex_search_details(search_input)
    except Exception as e:
        raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not authorized for search resources"
            )
 
@route.post("/analyst_details/")
async def get_analyst_details(
        analyst_input: Annotated[List,Depends(AnalystModel)]):
    try:
        return get_cortex_analyst_details(analyst_input)
    except HTTPException as he:
        raise he
 
 
@route.post("/txt2sql")
async def llm_gateway(
        query: Annotated[Txt2SqlModel,Body(embed=True)],
        config: Annotated[GenAiEnvSettings,Depends(get_config)],
        logger: Annotated[Logger,Depends(get_logger)],
        background_tasks: BackgroundTasks,
        get_load_datetime: Annotated[datetime,Depends(get_load_timestamp)]
):
    prompt = query.prompt.messages[-1].content
    #semantic_model = [f"{query.database_nm}.{query.schema_nm}." + item for item in query.semantic_model]
   
    #The API key validation and generation has been pushed to backend; the api_validator will return True if API key is valid for the application.
    api_validator = ValidApiKey()
    try:
        if api_validator(query.api_key,query.aplctn_cd,query.app_id):
           
            try:
                sf_conn = SnowFlakeConnector.get_conn(
                    query.aplctn_cd,
                    query.app_lvl_prefix,
                    query.session_id
                )
            except DatabaseError as e:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to resources"
                )
           
            cs = sf_conn.cursor()
 
            stage_show = f"SHOW STAGES in SCHEMA {query.database_nm}.{query.schema_nm};"
            # Get list of all stage names
            df_stg_lst = cs.execute(stage_show).fetchall()
            stage_names  = [sublist[1] for sublist in df_stg_lst]
            print(stage_names)
            semantic_models = []
 
            for stage_name in stage_names:
                list_query = f"LIST @{query.database_nm}.{query.schema_nm}.{stage_name};"
                try:
                    df_stg_files_lst = cs.execute(list_query).fetchall()
                except Exception as e:
                    print(f"Error listing files in stage {stage_name}: {e}")
                    continue
                # Extract .yaml files
                for sublist in df_stg_files_lst:
                    file_path = sublist[0]
                    if file_path.endswith('.yaml') or file_path.endswith('.yaml.gz'):
                        semantic_models.append(file_path)
                print(semantic_models)
 
            #query1 = {"semantic_model": ["test.yaml", "contract_star_rating_v2.yaml"]}
            query1 = {"semantic_model": query.semantic_model}
 
            # Construct full stage paths for the query models
            semantic_model_paths = []
            for model_name in query1["semantic_model"]:
                # Find the matching path from semantic_models
                matching_paths = [path for path in semantic_models if path.endswith("/" + model_name) or path.endswith("/" + model_name + '.gz')]          
                if matching_paths:
                    # Use the first matching path and construct the full stage path
                    stage_path = f"@{query.database_nm}.{query.schema_nm}." + matching_paths[0]
                    semantic_model_paths.append(stage_path)
                else:
                    print(f"Warning: No matching path found for {model_name}")
                    raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No matching path found for {model_name}"
                    )
            #semantic_model_old = ["@DOC_AI_DB.HEDIS_SCHEMA.HEDIS_STAGE_FULL/" + item for item in query.semantic_model]
            #print(semantic_model_old)
            semantic_model = semantic_model_paths  
            print("mdl", semantic_model)
            clnt = httpx.AsyncClient(verify=False,timeout=300.0)        
            request_body = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                "semantic_model_file": semantic_model[0].split('.gz')[0] #f"@{query.database_nm}.{query.schema_nm}.{query.stage_nm}/{query.semantic_model[0]}",
            }
            headers = {
                "Authorization": f'Snowflake Token="{sf_conn.rest.token}"',
                "Content-Type": "application/json",
                "Accept": "application/json",
                "method":"cortex",
                "api_key":query.api_key
            }
            url = getattr(config.TXT2SQL, "{}_host".format(config.env))
            get_sql = ""
            query_id = [None]
            fdbck_id = [str(uuid.uuid4())]
            async def txt2sql_data_streamer():
                try:
                    """
                    Stream data from the TXT2SQL service and yield responses.
                    """
                    async with clnt.stream('POST', url, headers=headers, json=request_body) as response:
                        if response.is_client_error:
                        error_message = await response.aread()
                        status_code = response.status_code
                        decoded_error = error_message.decode("utf-8")

                        if status_code == 429:
                            raise HTTPException(
                                status_code=429,
                                detail="Too many requests. Please implement retry with backoff. Original message: " + decoded_error
                            )
                        elif status_code == 402:
                            raise HTTPException(
                                status_code=402,
                                detail="Budget exceeded. Check your Cortex token quota. " + decoded_error
                            )
                        elif status_code == 400:
                            raise HTTPException(
                                status_code=400,
                                detail="Bad request sent to Snowflake Cortex. " + decoded_error
                            )
                        else:
                            raise HTTPException(
                                status_code=status_code,
                                detail=decoded_error
                            )
                        if response.is_server_error:
                            error_message = await response.aread()
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=error_message.decode("utf-8")
                            )
                        async for result_chunk in response.aiter_bytes():
                            #print(result_chunk)
                            for elem in result_chunk.split(b'\n\n'):
                                if b'content' in elem:  # Check for data presence
                                    try:
                                        chunk_dict = json.loads(elem.replace(b'data: ', b''))
                                        query_id[0]=chunk_dict['request_id']
                                        items = chunk_dict.get("message", {}).get("content", [])
                                        for item in items:
                                            item_type = item.get("type")
                                            if item_type == "text":
                                                yield item.get("text", "")
                                                yield "end_of_interpretation"
                                            elif item_type == "sql":
                                                get_sql = item.get("statement", "")
                                                #print("sql is ", get_sql)
                                                yield item.get("statement", "")
                                            elif item_type == "suggestions":
                                                yield json.dumps({"suggestions": item.get("suggestions", [])})
                                    except json.JSONDecodeError as e:
                                        logger.error(f"Error decoding JSON: {e}")
                                        continue
                        yield "end_of_stream"
                        responses = {
                        "prompt":prompt,
                        "query_id": query_id[0],
                        "fdbck_id": fdbck_id[0],
                        "type": "sql" }
                        full_final_response = "".join(get_sql)
                        yield json.dumps(responses)
                        #yield json.dumps({"type": "sql"})
                        #yield json.dumps({"prompt":prompt,"type": "sql"})
 
                except httpx.RequestError as e:
                    logger.error(f"Request error: {e}")
                    yield json.dumps({"detail": str(e)})
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    yield json.dumps({"detail": str(e)})
 
                #Model recreated the for the Audit record
                audit_rec = GenAiCortexAudit(
                    edl_load_dtm = get_load_datetime,
                    edl_run_id = "0000",
                    edl_scrty_lvl_cd = "NA",
                    edl_lob_cd = "NA",
                    srvc_type = "Analyst",
                    aplctn_cd = config.pltfrm_aplctn_cd,
                    user_id = "Analyst_User",#query.user_id,
                    mdl_id = query.model,
                    cnvrstn_chat_lmt_txt = "0",#query.cnvrstn_chat_lmt_txt,
                    sesn_id = query.session_id,
                    prmpt_txt = prompt.replace("'","\\'"),
                    tkn_cnt = "0",
                    feedbk_actn_txt = "",
                    feedbk_cmnt_txt = "",
                    feedbk_updt_dtm = get_load_datetime,
                )
                #background_tasks.add_task(log_response,audit_rec,query_id,str(full_final_response),fdbck_id,query.session_id)
 
            # Return a streaming response
            return StreamingResponse(txt2sql_data_streamer(), media_type='text/event-stream')
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="unauthenticated user"
            )
    except HTTPException as e:
        logger.error(f"Request error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(e)
            )
 
 
@route.post("/agent")
async def llm_gateway(
        query: Annotated[AgentModel,Body(embed=True)],
        config: Annotated[GenAiEnvSettings,Depends(get_config)],
        logger: Annotated[Logger,Depends(get_logger)],
        background_tasks: BackgroundTasks,
        get_load_datetime: Annotated[datetime,Depends(get_load_timestamp)]
 
):
    prompt = query.prompt.messages[-1].content
    search_service = [f"{query.database_nm}.{query.schema_nm}." + item for item in query.search_service]
 
    api_validator = ValidApiKey()
    try:
        if api_validator(query.api_key,query.aplctn_cd,query.app_id):  
            try:
                sf_conn = SnowFlakeConnector.get_conn(
                    query.aplctn_cd,
                    query.app_lvl_prefix,
                    query.session_id
                )
            except DatabaseError as e:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User not authorized to resources"
                )
            cs = sf_conn.cursor()
 
            stage_show = f"SHOW STAGES in SCHEMA {query.database_nm}.{query.schema_nm};"
            # Get list of all stage names
            df_stg_lst = cs.execute(stage_show).fetchall()
            stage_names  = [sublist[1] for sublist in df_stg_lst]
            print("stg_name", stage_names)
            semantic_models = []
 
            for stage_name in stage_names:
                list_query = f"LIST @{query.database_nm}.{query.schema_nm}.{stage_name};"
                try:
                    df_stg_files_lst = cs.execute(list_query).fetchall()
                    print("df_stg",df_stg_files_lst)
                except Exception as e:
                    print(f"Error listing files in stage {stage_name}: {e}")
                    continue
                # Extract .yaml files
                for sublist in df_stg_files_lst:
                    file_path = sublist[0]
                    if file_path.endswith('.yaml'):
                        semantic_models.append(file_path)
                print("Semantic Model files are ", semantic_models)
            #     semantic_model = f"[@{query.database_nm}.{query.schema_nm}.{stage_name}/{query.semantic_model[0]}]"
 
            # # #query1 = {"semantic_model": ["test.yaml", "contract_star_rating_v2.yaml"]}
            # # query1 = {"semantic_model": query.semantic_model}
 
            # # # Construct full stage paths for the query models
            # # semantic_model_paths = []
            # # for model_name in query1["semantic_model"]:
            # #     # Find the matching path from semantic_models
            # #     matching_paths = [path for path in semantic_models if path.endswith("/" + model_name)]          
            # #     if matching_paths:
            # #         # Use the first matching path and construct the full stage path
            # #         stage_path = f"@{query.database_nm}.{query.schema_nm}." + matching_paths[0]
            # #         semantic_model_paths.append(stage_path)
            # #     else:
            # #         print(f"Warning: No matching path found for {model_name}")
            # # # semantic_model_old = ["@DOC_AI_DB.HEDIS_SCHEMA.HEDIS_STAGE_FULL/" + item for item in query.semantic_model]
            # # # print(semantic_model_old)
            # # semantic_model = semantic_model_paths
            # # #semantic_model = [f"@{query.database_nm}.{query.schema_nm}.EDAGNAI_SF_CORTEX_NOGBD_NOPHI/" + semantic_models[0]]
            semantic_model = ["@D01_EDA_GENAI.SUPPORTCBT.EDAGNAI_SF_CORTEX_NOGBD_NOPHI/llm_audt.yaml"]
            print(semantic_model)
            clnt = httpx.AsyncClient(verify=False,timeout=150.0)
            request_body = {
                "model": "llama3.1-70b" , #query.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                #"provisioned_throughput_id":"113aeca4-5fec-48e3-9bf7-171503018eb4",
                "tools": [],
                "tool_resources": {}
            }
 
            for idx, semantic_model_files in enumerate(semantic_model, start=1):
                tool_name = f"analyst{idx}"
                # Append to tools
                request_body["tools"].append({
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": tool_name
                    }
                })
                # Append to tool_resources
                request_body["tool_resources"][tool_name] = {
                    "semantic_model_file": semantic_model_files  # Each tool references its own list of .yaml files
                }
 
            # Dynamically append cortex_search tools and tool_resources
            for idx, service in enumerate(search_service, start=1):
                tool_name = f"search{idx}"
                # Append to tools
                request_body["tools"].append({
                    "tool_spec": {
                        "type": "cortex_search",
                        "name": tool_name
                    }
                })
                # Append to tool_resources
                request_body["tool_resources"][tool_name] = {
                    "name": service,  # Each tool references one service
                    "max_results": query.search_limit
                }
            headers = {
                "Authorization": f'Snowflake Token="{sf_conn.rest.token}"',
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            print(request_body)
            url = getattr(config.AGENT, "{}_host".format(config.env))  
            print("url",url)  
            query_id = [None]
            fdbck_id = [str(uuid.uuid4())]
            full_response_text = []
            full_sql_response = []        
            import re
            async def agent_data_streamer():
                citations = []
                async with clnt.stream('POST', url, headers=headers, json=request_body) as response:
                    if response.is_client_error:
                        error_message = await response.aread()
                        status_code = response.status_code
                        decoded_error = error_message.decode("utf-8")

                        if status_code == 429:
                            raise HTTPException(
                                status_code=429,
                                detail="Too many requests. Please implement retry with backoff. Original message: " + decoded_error
                            )
                        elif status_code == 402:
                            raise HTTPException(
                                status_code=402,
                                detail="Budget exceeded. Check your Cortex token quota. " + decoded_error
                            )
                        elif status_code == 400:
                            raise HTTPException(
                                status_code=400,
                                detail="Bad request sent to Snowflake Cortex. " + decoded_error
                            )
                        else:
                            raise HTTPException(
                                status_code=status_code,
                                detail=decoded_error
                            )
                    if response.is_server_error:
                        error_message = await response.aread()
                        raise HTTPException(
                            status_code=response.status_code,
                            detail=error_message.decode("utf-8")
                        )
                    async for result_chunk in response.aiter_bytes():
                        #print(result_chunk)
                        for elem in result_chunk.split(b'\n\n'):
                            print("ele",elem)
                            if b'data:' not in elem:
                                continue
                            try:
                                match = re.search(rb'data: ({.*})', elem)
                                if not match:
                                    continue
                                #print(match.group(1))
                                chunk_dict = json.loads(match.group(1))
                                #print(chunk_dict)
                                query_id[0]=chunk_dict.get("id", {})
                                delta = chunk_dict.get('delta', {})
                                content_list = delta.get('content', [])
                                for content_item in content_list:
                                    content_type = content_item.get("type")
                                    if content_type == "text":
                                        text_part = content_item.get("text", "")
                                        full_response_text.append(text_part)
                                        response = text_part.replace("【†", "[").replace("†】", "]").replace("】", "]").replace("【", "[")
                                        yield response
                                    elif content_type == "tool_results":
                                        tool_results = content_item.get("tool_results", {})
                                        content_list_inner = tool_results.get("content", [])
                                        for result in content_list_inner:
                                            if result.get("type") == "json":
                                                json_obj = result.get("json", {})
                                                citations = json_obj.get('searchResults', [])
                                                #print("sea",citations)
                                                extracted_text = json_obj.get("text", "")
                                                if extracted_text:
                                                    full_response_text.append(extracted_text)
                                                    yield extracted_text
                                                    yield "end_of_interpretation \n  "
                                                extracted_sql = json_obj.get("sql", "")
                                                if extracted_sql:
                                                    full_sql_response.append(extracted_sql)
                                                    yield extracted_sql
                                    elif content_type == "tool_use":
                                        continue  # Optional: capture or log tool_use
                            except json.JSONDecodeError:
                                continue
                    yield "end_of_stream"
                    if citations:
                        yield json.dumps({"citations": citations})
                    if full_response_text and full_sql_response:
                        final_response = extracted_sql
                        yield json.dumps({"prompt":prompt,"query_id": query_id[0],
                            "fdbck_id": fdbck_id[0],"type": "sql"})
                        #yield json.dumps({"type": "sql"})
                    elif full_response_text:  # Check if there's any text to yield
                        final_response = "".join(full_response_text)
                        yield json.dumps({"prompt":prompt,"query_id": query_id[0],
                            "fdbck_id": fdbck_id[0],"type": "text"})
       
                #Model recreated the for the Audit record
                audit_rec = GenAiCortexAudit(
                    edl_load_dtm = get_load_datetime,
                    edl_run_id = "0000",
                    edl_scrty_lvl_cd = "NA",
                    edl_lob_cd = "NA",
                    srvc_type = "Agent",
                    aplctn_cd = config.pltfrm_aplctn_cd,
                    user_id = "Agent_user",#query.user_id,
                    mdl_id = query.model,
                    cnvrstn_chat_lmt_txt = "0",#query.cnvrstn_chat_lmt_txt,
                    sesn_id = query.session_id,
                    prmpt_txt = prompt.replace("'","\\'"),
                    tkn_cnt = "0",
                    feedbk_actn_txt = "",
                    feedbk_cmnt_txt = "",
                    feedbk_updt_dtm = get_load_datetime,
                )
                background_tasks.add_task(log_response,audit_rec,query_id,str(final_response),fdbck_id,query.session_id)
 
            return StreamingResponse(agent_data_streamer(), media_type='text/event-stream')
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="unauthenticated user"
            )
    except HTTPException as e:
        logger.error(f"Request error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(e)
            )
