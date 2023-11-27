from bp_split_up_sql import sql_split_app 
from bp_connect_api_s_prompts import openai_s_prompts_app
from bp_connect_api_c_prompts import openai_c_prompts_app

import azure.functions as func
app = func.FunctionApp()

app.register_functions(sql_split_app)
app.register_functions(openai_s_prompts_app) 
app.register_functions(openai_c_prompts_app) 