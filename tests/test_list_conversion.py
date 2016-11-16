import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import scoserv.utils as utils

dictionary = {'A':'0', 'B':'1'}

print dictionary

refs = utils.to_list(dictionary, label_key='k', label_value='v')

for kvp in refs:
    print kvp

print utils.from_list(refs, label_key='k', label_value='v')
