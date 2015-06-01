from os.path import splitext, basename

def get_collection_name(db_file):
    file_name = db_file.name
    return splitext(basename(file_name))[0]
