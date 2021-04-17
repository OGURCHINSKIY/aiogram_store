import os
import shutil
import sys
import requests

COMMANDS = ("install", "uninstall")
PACKAGES_LINK = "https://api.github.com/repos/OGURCHINSKIY/aiogram_store/contents/packages/{package}?ref=main"
STORE_DIR = os.path.dirname(os.path.realpath(__file__))
HELP = f"""aiogram store
    usage:
        install <package name> - install package
        uninstall <package name> - uninstall package
"""


def create_path(filename: str):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError:
            return


def download_dir(url: str):
    with requests.get(url) as api:
        for data in api.json():
            if data.get("type") == "file":
                create_path(os.path.join(STORE_DIR, data.get("path")))
                with open(os.path.join(STORE_DIR, data.get("path")), "wb") as f:
                    with requests.get(data.get("download_url")) as r:
                        f.write(r.content)
            elif data.get("type") == "dir":
                download_dir(data.get("url"))


def main():
    args = sys.argv[1:]
    if not args:
        print(HELP)
        sys.exit(0)
    if len(args) != 2:
        print(HELP)
        sys.exit(0)
    command, name = args
    if command not in COMMANDS:
        print(HELP)
        sys.exit(0)
    package_exist = os.path.exists(os.path.join(STORE_DIR, "packages", name))
    if command == "install":
        if package_exist:
            print("package already installed")
            sys.exit(0)
        package_url = PACKAGES_LINK.format(package=name)
        package = requests.get(package_url)
        if package.status_code == 404:
            print("package", name, "not found")
            sys.exit(0)
        print("loading package")
        download_dir(package_url)
        print("package", name, "installed")
    elif command == "uninstall":
        package = os.path.join(STORE_DIR, "packages", name)
        if package_exist:
            print("delete", name)
            shutil.rmtree(package)
            print(name, "deleted")
        else:
            print("package", name, "not found")

