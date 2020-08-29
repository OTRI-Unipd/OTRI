'''
Should be called automatically by the git hook
# html format
# --force deletes previous docs
# -o is the output dir
'''

import os

os.system("pdoc otri --html --force -o doc --config show_inherited_members=True --template-dir=.github/doctemplates")
