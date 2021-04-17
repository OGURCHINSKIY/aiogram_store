import os
import sys
from io import BytesIO, StringIO
import requests
from zipfile import ZipFile

COMMANDS = ("install", "uninstall")
STORE_JSON = "https://raw.githubusercontent.com/OGURCHINSKIY/aiogram_store/main/store.json"
PACKAGES_LINK = "https://api.github.com/repos/OGURCHINSKIY/aiogram_store/contents/packages/{package}?ref=main"


def main():
    args = sys.argv[1:]
    if not args:
        print("help")
        sys.exit(0)
    if len(args) != 2:
        print("wrong params count")
        sys.exit(1)
    command, name = args
    if command not in COMMANDS:
        print("what???")
        sys.exit(1)
    if command == "install":
        package = requests.get(PACKAGES_LINK.format(package=name))
        if package.status_code == 404:
            print(f"package {name} not find")
            sys.exit(0)
        print(package.json())
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        packages_path = os.path.join(cur_dir, "packages")
        print(f"package {name} installed")

