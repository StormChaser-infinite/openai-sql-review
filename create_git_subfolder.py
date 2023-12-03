from git import Repo 
import os

def create_sub_folders(subfoldername: str, filename: str):
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
        repo.git.add(subfolder_path)
        repo.git.commit("-m", f"Added '{subfoldername}' subfolder")
        print("Commit succeeded with the file uploaded: {subfoldername}, {filename} ")

    except Exception as e:
        print(f"Create subfolder {subfoldername} and upload the file {filename} failed because {e}")

def main():
    subfolder_name = "LISU"
    sql_input_name = "Test.txt"
    create_sub_folders(subfolder_name, sql_input_name)

main()

