from datetime import datetime
from fastapi import (
    APIRouter,
    Depends,
    Query,
)
from fastapi.responses import StreamingResponse
from pydantic import (
    BaseModel,
    Field,
    ValidationError
)
from typing import List
from typing_extensions import (
    Annotated,
    Literal
)
from logging import Logger
from models import (
     CompleteQryModel
)

from config import GenAiEnvSettings
from dependencies import (
    get_config,
    get_logger,
)

from ReduceReuseRecycleGENAI.api import get_api_key
from ReduceReuseRecycleGENAI.snowflake import snowflake_conn
import httpx,json


route = APIRouter(
    prefix="/api/cortex"
)

__sf_conn_dict = {}
def snowflake_connect(aplctn_cd, session_id,wh_size,sf_prefix,logger,env,region_nm):
    sf_conn_dict_key = (aplctn_cd, session_id)
    if sf_conn_dict_key in __sf_conn_dict.keys() and not __sf_conn_dict[sf_conn_dict_key].is_closed():
        print(f'The connection already exists for Application =\'{aplctn_cd}\' and Request ID =\'{session_id}\' : {__sf_conn_dict[sf_conn_dict_key]}')
        return __sf_conn_dict[sf_conn_dict_key]
    else:
        conn = snowflake_conn(logger, aplctn_cd=aplctn_cd, env=env, region_name=region_nm, warehouse_size_suffix=wh_size,prefix = sf_prefix)
        print(f"Snowflake Connection established successfully")
        __sf_conn_dict[sf_conn_dict_key] = conn
        return conn


def fetch_last_n_jsons(history, user_message, assistant_response, limit_convs):
    """Add a user-assistant message pair to the conversation history with a pair limit"""
    
    # If limit_convs is 0, return an empty history (no conversation stored)
    if limit_convs == 0:
        return []

    # Create message pair
    message_pair = {
        'user': user_message,
        'assistant': assistant_response
    }

    # Add the pair to history
    history.append(message_pair)

    # If the number of pairs exceeds the limit, keep only the most recent pairs
    if len(history) > limit_convs:
        history = history[-limit_convs:]

    return history

def get_conv_response(messages_json, limit_convs=None):
    
    last_msg=messages_json[-1]
   # Initialize empty conversation history
    conversation_history = []

    # Iterate over the messages in pairs
    for i in range(0, len(messages_json) - 1, 2):
        user_message = messages_json[i]['content']
        assistant_response = messages_json[i + 1]['content']
        conversation_history = fetch_last_n_jsons(
            conversation_history,
            user_message,
            assistant_response,
            limit_convs
        )
        #print_conversation(conversation_history)

    INITAL_PROMPT = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

    You are a helpful AI chat assistant with all capabilities.
    Below is the context supplied.{last_msg}
    <|eot_id|>
    """
    new_prompt_template=INITAL_PROMPT + " The chat history is as below \n" + str(conversation_history)
    print("new_prompt_template",new_prompt_template)
    return new_prompt_template

@route.get("/complete")
async def llm_gateway(
        query: Annotated[CompleteQryModel,Query(title="All the input query perameters for the model")], 
        config: Annotated[GenAiEnvSettings,Depends(get_config)],
        logger: Annotated[Logger,Depends(get_logger)]
):
    
    print(query)
    print(config)
    print(logger)
    #format the message json; need to revisit the logic in the next iteration 
    messages_json = query.prompt
    prompt = query.prompt 
    if isinstance(messages_json, list) and len(messages_json) > 0:
        for elm in range(len(messages_json) -1, -1,-1):
            if messages_json[elm]['role'] == 'user':
                prompt = messages_json[elm]['content']
                break 

    if isinstance(messages_json, str):
        prompt_last_msg = [{"role": "user", "content": messages_json}]
        messages_json = prompt_last_msg  

    #Get API key; this code should go to dependencies 
    api_key = get_api_key(
        logger,
        config.env,
        config.region_name,
        query.aplctn_cd,
        query.app_id
    )

    if query.api_key == api_key:
        
        #Teams Connection for cortex; this code should go to dependencies 
        sf_conn = snowflake_connect(
            query.aplctn_cd, 
            query.session_id,
            config.app_lvl_warehouse_size_suffix,
            query.app_lvl_prefix,
            logger,
            config.env,
            config.region_name,
        )

        #call to snowflake 
        async with httpx.AsyncClient(verify=False) as clnt: 

            request_body={
                "model": query.model,
                "messages": [
                    {
                        "role": "user",
                        "content":  query.sys_msg +  \
                                    get_conv_response( 
                                       messages_json,
                                       query.limit_convs
                                    ) + \
                                    prompt
                    }
                ]
            }
            
            headers = {
            "Authorization": f'Snowflake Token="{sf_conn.rest.token}"',
            "Content-Type": "application/json",
            "Accept": "application/json"
            }

            resp = await clnt.post(
                url=config.ENV["{}_host".format(config.env)],
                headers=headers,
                data=request_body,
                stream=True,
            )
            response_data= resp.raise_for_status()
            final_result = []
            async for result_cunk  in response_data:
                            if len(result_cunk) > 0:
                                chunk_dict = json.loads(result_cunk)
                                final_result.append(chunk_dict['choices'][0]['delata']['content'])
                                query_id =  chunk_dict['data']['id']
            
            return {
                 "query_id" : query_id,
                 "final_result" : final_result
            }

        #Connection to log audit; this code should go to dependencies 
        pltfrm_sf_conn = snowflake_connect(
            config.pltfrm_aplctn_cd, 
            query.session_id,
            config.pltfrm_lvl_wh_size,
            config.pltfrm_lvl_prefix,
            logger,
            config.env,
            config.region_nm,
        ) 


        
    


    return query

    
