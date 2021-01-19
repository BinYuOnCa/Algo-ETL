import yaml

GLOBAL_CONFIG_FILENAME = 'config.yml'

def get_global_config():
    with open(GLOBAL_CONFIG_FILENAME) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


global_config = get_global_config()


if __name__ == "__main__":
    print(f'config:{global_config}')
