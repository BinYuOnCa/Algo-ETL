import configparser
import getopt
import sys


__config = configparser.ConfigParser()
__config.read_file(open('application.conf'))


def get(config_name: str):
    return __config[config_name]


def getArgs(argv):
    help_info = "loaddata.py -r <1 or D> -a <rest or api>"
    try:
        opts, _ = getopt.getopt(argv, "hr:a:", ["resolution="])
    except getopt.GetoptError:
        print(help_info)
        sys.exit(2)
    except Exception:
        sys.exit(2)

    if(len(opts) == 0):
        print(help_info)
        sys.exit(2)

    args = {}
    for opt, arg in opts:
        if opt == '-h':
            print(help_info)
            sys.exit()
        elif opt in ("-r", "--resolution"):
            if(arg != "1" and arg != "D"):
                print(help_info)
            else:
                args['resolution'] = arg
        elif opt in ("-a", "--api"):
            if(arg != "api" and arg != "rest"):
                print(help_info)
            else:
                args['api'] = arg
    return args


if(__name__ == "__main__"):
    print(get('DATABASE'))
