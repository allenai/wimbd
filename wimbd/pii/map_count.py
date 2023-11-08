import argparse
from wimbd.utils.utils import read_json_gz_file
import re

'''
To debug
'''

pattern_dict=None


 
def contains_url(text):
    '''
    Function to check if text contains URL
    '''

    # findall() has been used
    # with valid conditions for urls in string

    # regrex for url

    regex =  re.compile("(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})")
    url = re.findall(regex, text)
    return len(url) > 0



def postprocess_email(text_input, match, pii_start, pii_end):
    '''
    Function to post process email addresses
    Rules:
    (1) The email address besides the domain, cannot be only "("
    (2) There must be a "." in the domain
    '''
    addressee=match.split("@")[0]
    domain=match.split("@")[1]


    if addressee.strip()=="(" or "." not in domain:
        return False
    return True

def postprocess_phone_numbers(text_input, match, pii_start, pii_end):
    '''
    Function to post process email addresses
    Rules:
    (1) ISBN, DOI, or "#" cannot appear in a context window of 50 characters from the match
    (2) Cannot contain URL
    '''
    context_window = text_input[max(0, pii_start - 50): min(len(text_input), pii_end + 50)].lower()
    if "isbn" in context_window or "doi" in context_window or "#" in context_window or contains_url(context_window):
        return False
    return True


def postprocess_ip_addresses(text_input, match, pii_start, pii_end):
    '''
    Function to post process email addresses
    Rules:
    (1) ISBN, DOI, or "#" cannot appear in a context window of 50 characters from the match
    '''
    context_window = text_input[max(0, pii_start - 50): min(len(text_input), pii_end + 50)].lower()
    if "isbn" in context_window or "doi" in context_window or "#" in context_window:
        return False
    return True
    

def postprocess_pass(text_input, match, pii_type):
    match = str("".join(match))
    pii_start = text_input.find(match)
    pii_end = pii_start + len(match)

    if pii_type=="email":
        return postprocess_email(text_input, match, pii_start, pii_end)
    elif pii_type=="phone_numbers":
        return postprocess_phone_numbers(text_input, match, pii_start, pii_end)
    elif pii_type=="IP_addresses":
        return postprocess_ip_addresses(text_input, match, pii_start, pii_end)


def extract_pii_regex(text_input: str,
                      context_window_one_side: int = 100):
    pii = []


    for pii_type in pattern_dict:
        pattern = pattern_dict[pii_type]
        # search for the pattern in the string
        matches = pattern.findall(text_input.lower())
        # loop through the matches and print corresponding values from the dictionary
        for match in matches:
            
            match = str("".join(match))
            pii_start = text_input.find(match)
            pii_end = pii_start + len(match)

            if postprocess_pass(text_input, match, pii_type):
                pii.append(pii_type)

    return pii





def main():
    parse = argparse.ArgumentParser("")

    parse.add_argument("--in_file", type=str, help="file to analyze")
    parse.add_argument("--bs", type=int, default=100, help="batch size for inputs")
    parse.add_argument("--classifier", type=str, default="regex", help="regex")

    args = parse.parse_args()

    data = read_json_gz_file(args.in_file)
    bs = args.bs
    inputs = []

    # Regular expressions for different types of PII
    global pattern_dict

    pattern_dict = {"email": re.compile("[.\s@,?!;:)(]*([^\s@]+@[^\s@,?!;:)(]+?)[.\s@,?!;:)(]?[\s\n\r]"),
                    "phone_numbers": re.compile("\s+\(?(\d{3})\)?[-\. ]*(\d{3})[-. ]?(\d{4})"),
                    "IP_addresses": re.compile(
                        "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"),
                    }

    THRESHOLD=500000
    row_count=0
    for row in data:

        row_count+=1

        
        if not row['text'] or row['text'].strip() == '': continue

        input=row["text"]
        doc_length=len(input)
        if doc_length > THRESHOLD or "............................................." in input or input.count("...........")>50 or "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" in input\
              or "# # # # # # #" in input or "???????" in input or input.count("/ / / / /")>50 or ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,," in input or ":):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):):)" in input or input.count("%")>1000\
        or ";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;" in input:

            pass

        else:
            preds =  extract_pii_regex(input)

            # When we hit the correct batch size
            for p in preds:
                print(p)




if __name__ == "__main__":
    main()
