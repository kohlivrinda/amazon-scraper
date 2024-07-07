import re
import json
from pathlib import Path

def load_json_data(filepath: Path):
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data


def correct_json_file(input_file, output_file):
    with open(input_file, 'r') as file:
        content = file.read().strip()
    
    # Use a regular expression to insert a comma before '{' when it is directly followed by '"product_name"', accounting for any type and amount of whitespace between '}' and '{'
    corrected_content = re.sub(r'}\s*(?=\s*{\s*"\s*product_name")', '},', content)
    
    # Wrap the whole content in square brackets to form a valid JSON array
    corrected_content = f'[{corrected_content}]'
    
    with open(output_file, 'w') as file:
        file.write(corrected_content)
        
def main():
    ORIGINAL_PATH = './outputs/test_outputs.json'
    NEW_PATH = './outputs/test_clean_data.json'
    correct_json_file(ORIGINAL_PATH, NEW_PATH )
    a = load_json_data(NEW_PATH)
    if a:
        print('cleaning successful')
    
if __name__ == "__main__":
    main()
    
    
    