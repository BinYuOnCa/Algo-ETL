import json
import os
from pathlib import Path



path = str(Path(os.getcwd()))

def read_from_config(file_name=path + "/config.json"):
    with open(file_name) as json_data_file:
        data = json.load(json_data_file)
    return data

# Write to config
def write_to_config(data, file_name=path + "/config.json"):
    with open(file_name, "w") as outfile:
        json.dump(data, outfile)

# print(write_to_config(data))
def main():

    print("This is to get config data")
    # config = read_from_config()

if __name__ == "__main__":
    main()

def mssql():
    config = read_from_config()
    return config["mssql"]

def postgresql():
    config = read_from_config()
    return config["postgresql"]

def finnhub():
    config = read_from_config()
    return config["finnhub"]

def settings():
    config = read_from_config()
    return config["settings"]

def email():
    config = read_from_config()
    return config["email"]

def sms():
    config = read_from_config()
    return config["sms"]

