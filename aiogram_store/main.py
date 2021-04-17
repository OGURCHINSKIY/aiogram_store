import os
import sys
from io import BytesIO

import requests
from zipfile import ZipFile

COMMANDS = ("install", "uninstall")
STORE_JSON = "https://raw.githubusercontent.com/OGURCHINSKIY/aiogram_store/main/store.json"
PACKAGES_LINK = "https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/OGURCHINSKIY/aiogram_store/tree/main/aiogram_store/packages/{package}"


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
        packages = requests.get(STORE_JSON).json()
        if name not in packages:
            print(f"package {name} not find")
            sys.exit(0)
        packages_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "packages")
        print(PACKAGES_LINK.format(package=name))
        package = requests.get(PACKAGES_LINK.format(package=name))
        zipfile = ZipFile(BytesIO(package.content))
        zipfile.extractall(path=packages_path)
        print(f"package {name} installed")

