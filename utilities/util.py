####################################################################
#
# This file is part of exfor-parser.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Disclaimer: The code is still under developments and not ready
#             to use. It has been made public to share the progress
#             among collaborators.
# Contact:    nds.contact-point@iaea.org
#
####################################################################

import re
import re
import os
import shutil
import time
import json
import math
from datetime import datetime, timedelta
from .elem import elemtoz_nz


def closest(nums, val):
    return nums[min(range(len(nums)), key=lambda i: abs(nums[i] - val))]


def closest(nums, val):
     return nums[min(range(len(nums)), key = lambda i: abs(nums[i]-val))]

def slices(s, *args):
    position = 0
    for length in args:
        yield s[position : position + length]
        position += length


def get_number_from_string(x):
    return re.sub(r"\D+", "", x)


def get_str_from_string(x):
    return re.sub(r"\d+", "", str(x))


def split_by_number(x) -> list:
    ## retrun list for e.g.
    # ['Br', '077', '']
    # ['Br', '077', 'g']
    # ['Br', '077', 'm']
    return re.split(r"(\d+)", x)


def get_number_from_string(x):
    return re.sub(r"\D+", "", x)


def get_str_from_string(x):
    return re.sub(r"\d+", "", str(x))


def split_by_number(x) -> list:
    ## retrun list for e.g.
    # ['Br', '077', '']
    # ['Br', '077', 'g']
    # ['Br', '077', 'm']
    return re.split(r"(\d+)", x)


def flatten(xs):
    from collections.abc import Iterable

    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


def flatten_list(list):
    return [item for sublist in list for item in sublist]


def check_list(init_list):
    # print(any(isinstance(i, list) for i in init_list))
    def _is_list_instance(init_list):
        sub_list = flatten_list(init_list)
        _is_list_instance(sub_list)

        return isinstance(init_list, list)


def dict_merge(dicts_list):
    d = {**dicts_list[0]}
    for entry in dicts_list[1:]:
        # print("entry:", entry)
        for k, v in entry.items():
            d[k] = (
                [d[k], v]
                if k in d and type(d[k]) != list
                else [*d[k] + v] if k in d else v
            )
    return d


def combine_dict(d1, d2):
    return {
        k: list(d[k] for d in (d1, d2) if k in d)
        for k in set(d1.keys()) | set(d2.keys())
    }


def get_key_from_value(d, val):
    keys = [k for k, v in d.items() if v == val]
    if keys:
        return keys[0]
    return None


def toJSON(self):
    return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def del_file(fname):
    os.remove(fname)


def del_outputs(name, outpath):
    path = os.path.join(outpath, name)

    if os.path.exists(path):
        shutil.rmtree(path)

    os.mkdir(path)


def print_time():
    now = datetime.now()
    return now.strftime("%d/%m/%Y %H:%M:%S")


def print_process_time(start_time=None):
    if start_time:
        str = "--- %s seconds ---" % timedelta(seconds=time.time() - start_time)
        return str

    else:
        return time.time()


def cos_to_angle_degrees(cos_value):
    if not -1.0 <= cos_value <= 1.0:
        raise ValueError("cos_value must be between -1 and 1.")
    
    radians = math.acos(cos_value) 
    degrees = math.degrees(radians)  
    return degrees



def x4style_nuclide_expression(elem, mass):
    if get_str_from_string(mass) != "":
        return f"{elemtoz_nz(elem)}-{elem.upper()}-{get_number_from_string(mass)}-{get_str_from_string(mass).replace('-', '').upper()}"

    return f"{elemtoz_nz(elem)}-{elem.upper()}-{str(int(mass))}"


def libstyle_nuclide_expression(elem, mass):
    if get_str_from_string(mass) != "":
        return f"{elem.capitalize()}{get_number_from_string(mass).zfill(3)}{get_str_from_string(mass).replace('-', '').lower()}"

    return f"{elem.capitalize()}{get_number_from_string(mass).zfill(3)}"


def round_half_up(n, decimals=0):
    multiplier = 10**decimals
    return math.floor(n * multiplier + 0.5) / multiplier


def correct_pub_year(yearstr):

    if len(yearstr) == 2:
        return "19" + yearstr

    elif len(yearstr) == 4:
        if yearstr.startswith("20") or yearstr.startswith("19"):
            ## 1960 or 2020
            return yearstr

        elif not yearstr.startswith("19"):
            ### 8811
            return "19" + yearstr[0:2]

    elif len(yearstr) == 6:
        if yearstr.startswith("20"):
            ### 200109
            return yearstr[0:4]

        elif yearstr.startswith("19"):
            ###  196809
            return yearstr[0:4]

        elif not yearstr.startswith("19") and not yearstr.startswith("20"):
            ###  680901, most of the case it could be 19s
            return "19" + yearstr[0:2]

    elif len(yearstr) == 8:
        ### 20001120
        return yearstr[0:4]

    else:
        ## Just in case
        return yearstr
