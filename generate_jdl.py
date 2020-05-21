#!/usr/bin/env python3
from jinja2 import Template
import click
import os

DEFAULT_CE="ALICE::FZK::ARC"
DEFAULT_TEMPLATE="template.jdl.jinja"
DEFAULT_JDL_OUTPUT="JDL"

def get_template(template):
    with open("template.jdl.jinja") as f:
        return Template(f.read())

def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

def get_grid_output_dir(base, target_ce, _id):
    suffix = normalize_ce_name(target_ce) + "_" + str(_id)
    return os.path.join(base, suffix)

def patch_jdl(jdl_template, target_ce, _id, grid_home):
    output_dir = grid_home + "/site-sonar/outputs"
    params = {
        "grid_home": grid_home,
        "target_ce": target_ce,
        "output_dir": get_grid_output_dir(output_dir, target_ce, _id),
    }
    return jdl_template.render(params)

def write_jdl(patched_jdl, target_ce, _id):
    jdl_filename = normalize_ce_name(target_ce) + "_" + str(_id) + ".jdl"
    full_path = os.path.join(DEFAULT_JDL_OUTPUT, jdl_filename)
    full_path = os.path.abspath(full_path)

    with open(full_path, "w") as f:
        f.write(patched_jdl)


def generate_template(template, write, id, target_ce, grid_home):
    _id = id
    template = template or DEFAULT_TEMPLATE
    target_ce = target_ce or DEFAULT_CE
    template = get_template(template)
    patched = patch_jdl(template, target_ce, _id, grid_home)

    if write:
        print("Writing: {} #{}".format(target_ce, _id))
        write_jdl(patched, target_ce, _id)
    else:
        print(patched)

if __name__ == "__main__":
    main()
