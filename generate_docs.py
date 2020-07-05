import os

# html format
# --force deletes previous docs
# -o is the output dir
os.system("pdoc otri --html --force -o doc")