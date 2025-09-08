from ReduceReuseRecycleGENAI.snowflake import snowflake_conn
import logging

logger = logging.getLogger(__name__)

class SnowFlakeConnector:
    conn = None
    conn_key=None
    sf_conn_dict={}
    def __init__(self,aplctn_cd,sf_prefix):
        #self.config = get_config()
        #self.logger = get_logger()
        self.aplctn_cd = aplctn_cd
        self.sf_prefix = sf_prefix

        SnowFlakeConnector.conn =  snowflake_conn(
           logger,
           aplctn_cd="xxx",#aplctn_cd,
           env="preprod",#self.config.env,
           region_name="xxx",#self.config.region_name,
           warehouse_size_suffix="_M",#self.config.pltfrm_lvl_warehouse_size_suffix,
           prefix = ""
        )

    @classmethod
    def get_conn(cls,aplctn_cd,sf_prefix):
        key = (aplctn_cd,sf_prefix)
        if key in cls.sf_conn_dict and cls.sf_conn_dict[key].is_valid():
            print(f"The connection already exists for Application ='{aplctn_cd}' and Request ID = : {cls.sf_conn_dict[key]}")
            return cls.sf_conn_dict[key]
        else:
            cls(aplctn_cd,sf_prefix)
            cls.sf_conn_dict[key]=cls.conn
            print(f"Snowflake Connection established successfully for application")
            print("sf_conn is", cls.conn)
            return cls.conn
