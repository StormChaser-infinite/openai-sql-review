from git import Repo 
import os
from azure.storage.blob import BlobServiceClient

def create_subfolders(subfoldername: str, filename: str):
    """Create a subfolder with users' initial or email address when uploading the sql text file"""
    try:
        current_dir = os.getcwd()
        repo = Repo(path=current_dir, search_parent_directories=True)
        
        # read the content in the uploaded text file
        with open(filename, "r") as file:
            file_content = file.read()
            file.close()

        # create a subfolder with users' initials or email address
        subfolder_path = os.path.join(repo.working_dir,"sql_inputs" ,subfoldername)
        if os.path.exists(subfolder_path):
            pass
        else:
            os.mkdir(subfolder_path)

        # if the file already exits, override it with the new one
        file_path = os.path.join(subfolder_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w") as file:
            file.write(file_content)
            file.close()

        # commit the changes to the repo
        repo.index.add([file_path])
        repo.index.commit(f"Added '{subfoldername}' subfolder")
        origin = repo.remote(name='origin')
        origin.push()
        print(f"Commit succeeded with the file uploaded: {subfoldername}, {filename} ")
        write_blob(filename, subfoldername)

    except Exception as e:
        print(f"Create subfolder {subfoldername} and upload the file {filename} failed because {e}")

def write_blob( subfolder: str, filename: str):
    "Write the SQL Scripts from repo to storage account"
    try: 
        with open(filename, "r") as file:
            file_content = file.read()
            file.close()
        blobservice = BlobServiceClient.from_connection_string(conn_str=  
                                                               "DefaultEndpointsProtocol=https;AccountName=kittsqlmodel;AccountKey=6n/WfghjW+2xAN2h1iCiYxELJPADDC9h5Fr+iLbg+/1kBSoD8eVSeyeStKEyALWyNRE4XEBT8OJY+ASt5EspHQ==;EndpointSuffix=core.windows.net" )
        file_name = subfolder+ "/"+ filename
        container = blobservice.get_container_client("sql-inputs")
        blob_client = container.get_blob_client(file_name)
    
        blob_client.upload_blob(file_content,  overwrite=True)

        print(f"Successfully write the file - {filename} into {subfolder} blob container!")
        
    except Exception as e:
        print(f"Fails to connect to the blob container, the error is {e}.")

def main():
    subfolder_name = "lilisun"
    sql_input_name = "Test.txt"
    create_subfolders(subfolder_name, sql_input_name)
    write_blob(subfolder_name, sql_input_name)
main()

