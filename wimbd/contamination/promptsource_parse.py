import argparse
import csv
import re
from glob import glob

from wimbd.contamination.templates import INCLUDED_USERS, TemplateCollection
from wimbd.contamination.utils import get_dataset


def main():

    parse = argparse.ArgumentParser("")
    parse.add_argument("--path", type=str)
    parse.add_argument("--out_file", type=str)
    
    args = parse.parse_args()

    datasets = []
    for path in glob(args.path + '/**/templates.yaml', recursive=True):
        datasets.append(path)
        

    with open(args.out_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        for dataset in datasets:
            path = dataset.split('/')
            dataset_name = path[8]
            subset_name = path[9] if len(path) == 11 else ''
            
            template_collection = TemplateCollection()
            dataset_templates = template_collection.get_dataset(dataset_name, subset_name)
            
            # Selecting the first template
            template = list(dataset_templates.templates.values())[0]
            jinja = template.jinja

            # Matching the inputs from the data to the jinja template
            matches = re.findall(r"{{[a-zA-Z0-9_-]*}}", jinja.replace(' ', ''))
            matches = [x.replace("{{", "").replace("}}", "") for x in matches]


            print(dataset_name, subset_name, matches, sep='\t')
            writer.writerow([dataset_name, subset_name, matches])
        # exit(0)
        # dataset = get_dataset('anli', None)
        # keys = dataset.keys()

        # for key in keys:
        #     if 'test' in key:
        #         print(key)
        #         for match in matches:
        #             print(match, dataset[key][match][0])



if __name__ == "__main__":
    """
    Usage example:
    
    ``````

    """
    main()
