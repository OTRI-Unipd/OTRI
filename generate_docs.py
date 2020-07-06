import os

# Should be called automatically by the git hook
# html format
# --force deletes previous docs
# -o is the output dir
os.system("pdoc otri --html --force -o doc")