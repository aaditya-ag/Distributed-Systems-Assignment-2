# This file prints messages in different colors

def printError(message):
    # Message in Red Color
    print("\033[91m{}\033[00m" .format(message))

def printSuccess(message):
    # Message in Green Color
    print("\033[92m{}\033[00m" .format(message))

def printInfo(message):
    # Message in Yellow Color
    print("\033[93m{}\033[00m" .format(message))
