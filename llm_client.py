import json 
import logging
import requests
import shlex
from pydantic import BaseModel, Field
from typing import Any, Dict, List 
from dependencies import SnowFlakeConnector

HOST = "xxxxxxxx"
url = f"xxxxxxx"

DEFAULT_MAX_TOKENS = 16000
DEFAULT_MODEL = "claude-4-sonnet"

logger = logging.getLogger(__name__)


def resolve_refs(schema: dict) -> dict:
    """
    Recursively resolve $ref in a Pydantic JSON schema using $defs.
    Returns a new dict with all references replaced by their definitions.
    """
    defs = schema.get("$defs", {})
    def _resolve(obj):
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_path = obj["$ref"]
                # Only support local refs like "#/$defs/SomeType"
                if ref_path.startswith("#/$defs/"):
                    def_key = ref_path.split("/")[-1]
                    return _resolve(defs[def_key])
                else:
                    raise ValueError(f"Unsupported $ref path: {ref_path}")
            else:
                return {k: _resolve(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_resolve(item) for item in obj]
        else:
            return obj
    # Remove $defs from the top-level after resolving
    resolved = _resolve(schema)
    if "$defs" in resolved:
        del resolved["$defs"]
    return resolved


class LLMClient:
    def __init__(self, config: dict | None = None, session: Any = None):
        if config is None:
            config = {
                "temperature": 0,
                "max_tokens": DEFAULT_MAX_TOKENS,
                "model": DEFAULT_MODEL
            }

        self.config = config
        if "max_tokens" not in config:
            self.config["max_tokens"] = DEFAULT_MAX_TOKENS
        if "model" not in config:
            self.config["model"] = DEFAULT_MODEL
        self.session = session

    async def _generate_response(
        self,
        messages: list[dict[str,Any]],
        response_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        
        if not self.session.is_valid():
            self.session = SnowFlakeConnector.get_conn('aedl','',)

        response_format = {}
        if response_model:
            resolved_schema = resolve_refs(response_model.model_json_schema())
            # Recursively clean properties: remove 'title', replace 'anyOf' with 'type', remove 'default' if present
            def clean_property(obj):
                if isinstance(obj, dict):
                    obj.pop("title", None)
                    if "anyOf" in obj:
                        # Find the first dict in anyOf with a 'type' key
                        for entry in obj["anyOf"]:
                            if isinstance(entry, dict) and "type" in entry:
                                obj["type"] = entry["type"]
                                break
                        obj.pop("anyOf", None)
                        obj.pop("default", None)
                    for v in obj.values():
                        clean_property(v)
                elif isinstance(obj, list):
                    for item in obj:
                        clean_property(item)
            properties = resolved_schema.get("properties", {})
            clean_property(properties)
            response_format = {
                "response_format": {
                    "type": "json",
                    "schema": {
                        "properties": properties,
                        "required": resolved_schema.get("required", []),
                        "type": resolved_schema.get("type", "object")
                    }
                }
            }
        
        #response_format = {}
        options = {
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens if self.config.max_tokens is not None else 16000,
            **response_format
        }

        
        #Check if we require tools
        request_body = {
            "model": self.config.model,
            "messages": messages,
            **options,
        }
        
        
        headers = {
                "Authorization": f'Snowflake Token="{self.session.rest.token}"',
                "Content-Type": "application/json",
                "Accept": "application/json"
        }
        
        try:
            response = requests.post(
               url=self.config.base_url 
              ,headers=headers
              ,json=request_body
            )
            #Format the response
            print("--------<Response received as a result of LLM call>-------") 
            print(response.text)
            response.raise_for_status()
            response_chunks = []
            if response.status_code == 200:
                for line in response.iter_lines():
                    if line:
                        line_content = line.decode('utf-8').replace('data: ', '')
                        json_line = json.loads(line_content)
                        delta_chunk =json_line["choices"][0].get("delta", {})
                        response_chunks.append(delta_chunk.get("content", ""))
                result = "".join(response_chunks)
                print(response.status_code)
                print(result)
                print("--------<Response received as a result of LLM call>-------")
                return json.loads(result.strip("```").strip("json").strip())
        except Exception as e:
            logger.error(f'Error in generating LLM response: {e}')
            logger.error(f'Response body: {response.response.status_code}')
            logger.error(f"Response body: {response.request.body}")
            print("-----<curl command>--------")
            curl_cmd = [
                "curl",
                "-X", "POST",
                shlex.quote(self.config.base_url),
                "-H", shlex.quote(f'Authorization: Snowflake Token=\"{self.session.rest.token}\"'),
                "-H", "'Content-Type: application/json'",
                "-H", "'Accept: application/json'",
                "-d", shlex.quote(json.dumps(request_body))
            ]
            print(" ".join(curl_cmd))
            print("-----<curl command>--------")
            raise
