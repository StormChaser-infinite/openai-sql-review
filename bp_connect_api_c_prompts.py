import azure.functions as func
from openai import AzureOpenAI
from azure.storage.blob import ContainerClient 
import pymongo
import datetime as dt
import logging
import json
import sqlparse


input_container = 'sql-selected'
cosmos_db_name = "sql-model-review"
collection_name = "kitt-collection"
openai_c_prompts_app = func.Blueprint()

@openai_c_prompts_app.blob_trigger(arg_name="myblob", path="sql-selected/{name}",connection="kittsqlmodelconnectionstring") 
def openai_c_prompts(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    file_name = myblob.name[myblob.name.find("/")+1:]
    sql_string = read_blob(file_name)
    response = connect_openai(file_name, sql_string)

    save_reponses(response)


def trim_sql_query(sqistring: str):
    """Clean up all the comments in the input SQL query"""
    sql_cleaned = sqlparse.format(sqistring, strip_comments=True).strip()
    return sql_cleaned


def read_blob(filename: str):
    "Read the SQL Scripts from the blob container"
    try: 
        blobservice = ContainerClient(account_url = "https://prodkittsqlmodel.blob.core.windows.net", 
                                        credential= "dH2zsQDaDtA4fBgPQMCK1TaVM/F6HCQ56kG+6fHqddulxR8FAe6V/JPh8wKRjiwNmm1/8F1WPUVn+AStAxElrg==",
                                        container_name = input_container)
        contents = blobservice.get_blob_client(filename).download_blob().readall()
        contents = trim_sql_query(contents)
        print(f"Successfully read the file - {filename} from blob container!")
        return contents
    except Exception as e:
        print(f"Failed to connect to the blob container, the error is {e}.")


def count_tokens(copmletios):
    """Return the number of tokens used in the prompt"""
    number_tokens = copmletios.usage.prompt_tokens
    if number_tokens < 4096:    ## the maximum number of tokens that OpenAI can in prompts
        print(f"{number_tokens} prompt tokens counted by the OpenAI API.")
    else:
        print(f"Warnning! The query is too long and exceeds the token limit. ")


def connect_openai(filename: str, sqlsstring: str):
    """connect to Azure openai and run promots to retrive the response"""
    file_reviewed = filename[0:filename.find('.')]
    response = {"queryId": file_reviewed, 
                "review_date": dt.date.today().strftime('%Y-%m-%d'),
                "responses": {}}
    questions_count = 0
    resp = []
    try:
        client = AzureOpenAI(api_version="2023-05-15",
                            azure_endpoint="https://kmrt.openai.azure.com/",
                            api_key= "a95055f384dd4051a696499d00a064a3")
        prompts = []
        data = json.load(open("Questions_C.json"))
        questions = []
        for key, value in data.items():
            questions.append(value) 

        questions_count = 0
        for i in range(0, len(questions)) :
            if questions_count == 0:
                prompts.append({"role": "user","content": questions[i] + sqlsstring})
                completion = client.chat.completions.create( model="cd-kmrt-sbx-gpt4",  
                                                            messages=[{"role": "user","content": questions[i] + sqlsstring},], 
                                                            temperature= 0.3) 
                prompts.append({"role": "assistant","content": completion.choices[0].message.content})
            else:
                prompts.append({"role": "user","content": questions[i]})
                completion = client.chat.completions.create( model="cd-kmrt-sbx-gpt4",  
                                                            messages=prompts, 
                                                            temperature= 0.3) 
                prompts.append({"role": "assistant","content": completion.choices[0].message.content})

            count_tokens(completion)
            resp.append({"questionId": questions_count+1,
                         "number of tokens": completion.usage.prompt_tokens,
                          questions[i]: completion.choices[0].message.content})
            
            questions_count = questions_count + 1

        response.update({"responses": resp})
        
        print(f"Successfully connected to OpenAI API and sent the customised promots. {questions_count} responses are recieved!")

        return response
    except Exception as e:
        print(f"Connecting to OpenAI API failed because {e}")


def save_reponses(messages: dict):
    """Save the responses to the Cusmos DB"""
    try:
        CONNECTION_STRING = "mongodb://kitt-sql-review:hfM2p34q75uIxLRJtQfYNbZk8Y4RmAJMSlb5SRtXp6NfMhVQfzSevb7FcQ95MJdGqqX59bQQoHp3ACDbvHrHgw==@kitt-sql-review.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@kitt-sql-review@"
        mglient = pymongo.MongoClient(CONNECTION_STRING)
        collection = mglient[cosmos_db_name][collection_name]
        ## check the collection exists or not
        if collection_name not in mglient[cosmos_db_name].list_collection_names():
        # Creates a unsharded collection that uses the DBs shared throughput
            mglient[cosmos_db_name].command({"customAction": "CreateCollection", "collection": collection_name})
            print("Created collection '{}'.\n".format(collection_name))
        else:
            print("Using collection: '{}'.\n".format(collection_name))
        #create a document
        result = collection.update_one({"name": messages["queryId"]}, 
                                       {"$set": messages}, upsert=True)
        
        print("Upserted {} document with _id {}\n".format(messages["queryId"] ,result.upserted_id))

    except Exception as e:
        print(f"Exporting the responses to Cusmos DB failed because {e}")