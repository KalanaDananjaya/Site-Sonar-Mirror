import os
import shutil
import json
import logging

from db_connection import get_siteid_by_normalized_name, get_nodeid_by_node_name, add_parsed_output_by_names, delete_parsed_outputs,initialize_processing_state, update_processing_state_by_sitename

def process_section(section_array):
    section_title = section_array[0].replace('-----','').strip()
    section_array.pop(0)
    section = {}
    section.update({'title':section_title})
    data = {}
    if len(section_array) > 1:
        section.update({'mulitple_lines': True})
    elif len(section_array) == 1:
        section.update({'mulitple_lines': False})
    for line in section_array:
        # Ignore the comments
        line = line.split('#',1)[0]
        # Identify the key value pair separated by ':'
        key_val_pair = line.split(':',1)
        if len(key_val_pair) == 1:
            key = key_val_pair[0].rstrip()
            # Check for empty command results
            if key:
                value = 'no_result'
            # Ignore empty lines
            else:
                continue
        elif len(key_val_pair) == 2:
            key = key_val_pair[0].strip()
            value = key_val_pair[1].strip()
        else:
            key = None
            value = None 
        data.update({key:value})
    section.update({'data':data})
    return section

def parse_mid_section(mid_section):
    line_num = 0
    sections = []
    # Read the whole file and process sections
    while line_num < len(mid_section):
        if mid_section[line_num].startswith('-----'):
            line_start = line_num
            line_num += 1
            end = 0
            while end == 0 and line_num < len(mid_section):
                # If a new section start, mark the section end
                if mid_section[line_num].startswith('-----'):
                    line_end = line_num
                    end = 1
                    break
                # If the section didn't end but the file ended, mark the section end
                elif line_num == len(mid_section)-1:
                    line_end = len(mid_section)
                    end = 1
                    break
                else:
                    line_num += 1
            # Process section
            section = process_section(mid_section[line_start:line_end])
            sections.append(section)
        else:
            line_num += 1
    return sections

def parse_init_section(init_section):
    # Execution Grid Site Name
    site_name = init_section[0].split('is at')[1].strip()
    # Execution Machine Hostname
    hostname = init_section[1].split(':',1)[1].strip()
    return site_name, hostname

def clear_output_dir(output_dir):
    if os.path.exists(output_dir): 
        shutil.rmtree(output_dir, ignore_errors=True)

def parse_output_directory(output_dir):
    try:
        for dir in os.listdir(output_dir):
            filepath = output_dir +'/' + dir 
            # Remove the job number from the directory name
            split_dir_name = dir.rsplit('_',1)
            normalized_site_name = split_dir_name[0]
            job_number = split_dir_name[1]
            try:
                if 'stdout' not in os.listdir(filepath):
                    raise IOError('Stdout File not avaialabe')
                else:
                    with open(filepath + '/stdout','r') as f:
                        output = f.readlines()
                        # First 2 lines should be processed separately as they are different from the template
                        init_section = output[:2]
                        mid_section = output[2:]
                        file_data = {}
                        site_name, hostname = parse_init_section(init_section)
                        mid_sections = parse_mid_section(mid_section)
                        file_data.update({'site_name': site_name})
                        file_data.update({'hostname': hostname})
                        file_data.update({'sections': mid_sections})
                        parsed_result = json.dumps(file_data)
                        result = add_parsed_output_by_names(normalized_site_name,file_data['hostname'],parsed_result)
                        if result:
                            logging.info('%s directory parsed succesfully', dir)
                            update_processing_state_by_sitename(normalized_site_name,'PARSED')
                        else:
                            logging.error('Error parsing %s directory',dir)
            except NotADirectoryError:
                logging.debug('%s is not a directory.Skipping...',dir)
    except Exception as error:
        logging.exception(error)


