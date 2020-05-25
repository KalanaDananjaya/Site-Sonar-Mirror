import os

def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

def get_grid_output_dir(base, target_ce, _id):
    suffix = base + '/outputs/' + normalize_ce_name(target_ce) + "_" + str(_id)
    return os.path.join(base, suffix)
