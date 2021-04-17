import sys

COMMANDS = ("install", "uninstall")


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
    print(f"{command} - {name}")
